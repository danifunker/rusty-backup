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

## Slice 13: the Restore tab

`src/gui/restore_tab.rs` (3,181 lines, 85 engine references) held five restore modes in the view:
the backup-folder load (metadata, Clonezilla partclone headers) on the UI thread, the size and
alignment rules, the trailing-free-space plan and its preview, the layout-fit projection, the
single-partition target scan, a `gui::pending_image` decode, Write Image's decoded-size probe on
the UI thread, Build Disk's target and plan, the remote target's connect worker, the confirmation
window, and four `spawn_guarded` restore workers plus the provision runner. The TUI's Restore
screen read the backup with its own loader and ran `run_restore` on its own thread.

### Steps

1. (ca83aa31) Model and controller. `model::restore_plan` holds the rules: `load_backup` (native
   `metadata.json`, a Clonezilla image, or a `.cbk` unpacked to a temp folder) into a
   `LoadedBackup` whose `SizeRow`s carry each partition's safe minimum (defragmented, then in
   place, then imaged), `SizeRow::choice` (the `SizeMode` -> `RestoreSizeChoice` mapping),
   `partition_sizes` (extend-last forces Fill), `ExpandPlan` (trailing free space: image targets
   only, the MiB cap), `resolve_alignment`, `layout_fit` (the engine's `calculate_restore_layout`
   projected before Start), `plan_segments`, `summary`, `original_alignment_label`, and the
   single-partition / new-disk configs with `new_disk_start_lba`. The layouts themselves
   (alignment preservation, the disk-label schemes' `restore_patch`, the single-file CHD path)
   stay in `crate::restore`. `controller::restore::RestoreController` (`mod.rs`, `source.rs`,
   `target.rs`, `remote.rs`, `run.rs`) owns:
   - Sources: `OpenBackup` (a job; a folder handed over during a restore loads when it ends),
     `PickBackupFolder`, `OpenSourceImage` / `PickSourceImage` (a wrapped image is decoded in a
     job), `OpenDropped` (a folder restores in full, a file is a partition image), `ClearBackup`,
     `SetSourcePartition`. `HostRequest::RememberRecent` asks the shell to remember a folder.
   - Targets per `TargetSlot` (FullDisk, SinglePartition, NewDisk, BuildDisk): `SelectDevice {
     slot, row, device }`, `ClearDevice`, `SetTargetIsDevice`, `PickTargetImage` (a save picker),
     and `reconcile_devices` (a refreshed list moves each target with its row; the
     single-partition scan is dropped when its disk changed). The full-disk form: alignment,
     per-partition sizes, the trailing free space, `target_size`, `fixed_total`, `layout_fit`;
     the remote target (`SetRemote`, `RemoteConnect` as a job, `RemotePickDevice`,
     `RemoteUseImage`). The single-partition `ScanTarget` job and `target_partitions` (with
     whether the source fits). The new-disk `NewDiskChoice`.
   - Write Image File: `OpenWriteImage` measures the decoded size in a job; `WriteImageTarget`
     is `HostRequest::WriteImage { path, size }` for the shell's disk writer.
   - Build Disk: `SetBuildSize`, `BuildDefine` (opens the plan, a `DiskBuilder` the shell's editor
     edits through `builder()`), `BuildApply` (an image target builds at once; a device is an
     `Effect::AskText` whose validator wants the device's name), `BuildCancel`. The build is a
     job over `provision_runner`, its log lines and bytes mirrored.
   - Start: `validate()` per mode, `Start` -> `Effect::Confirm(purpose::RESTORE)` naming what is
     overwritten, `Confirmed` runs the mode's restore as a job (`run_mirrored` over
     `run_restore`, `restore_to_remote` or `run_single_partition_restore`), `Cancel`,
     `progress()` (`RestoreBar`), `last_result`. `wait_idle` / `wait_finished` for a headless
     shell or a test.
2. (7582a373) The egui tab renders and maps. It keeps only the recent-list mirror, the shared
   progress bar flag and the Physical Disk Export window Write Image hands its image to; the
   partition editor edits the controller's `DiskBuilder`. `gui::pending_image` is gone (its last
   user was this tab).
3. (this commit) `src/gui/restore_tab.rs` joins `ENFORCED`; the TUI's Restore screen loads a
   backup folder, `.cbk` or Clonezilla image through a headless `RestoreController`, hands it the
   target, sizes and alignment, presses Start, answers the confirmation (after its own device
   prompt) and mirrors the run (`the_restore_screen_restores_a_backup_through_the_controller`);
   its image source is measured through the controller too. A ui scenario
   (`tests/ui_scenarios/restore_open_validate_run.toml`, vm `restore`) refuses an empty folder,
   loads a backup, walks Start's refusals, confirms, restores to an image byte for byte and closes.

Shells' engine references 393 -> 291 (`restore_tab.rs` 85 -> 0, `pending_image.rs` 2 -> 0,
`tui_app.rs` 81 -> 66).

`rb-cli restore` keeps its own `RestoreConfig`: its `--size minimum` is the engine's floor
(`RestoreSizeChoice::Minimum`) where the GUI sizes to the recorded minimum, Original is an empty
size list, the target size comes from `--target-size`, the device or the metadata, and the
alignment can come from the config file; `model::restore_plan` would change the bytes it writes.

### Behaviour changes

- GUI: the restore confirmation is the shared confirmation dialog (same wording, the partition
  list included, no coloured line); Build Disk's typed device name is a text prompt whose OK
  enables only on the exact name (it was a field in the confirmation window).
- GUI: loading a backup folder (metadata, Clonezilla partclone headers) and Write Image's
  decoded-size probe run in jobs with a progress line (they ran on the UI thread).
- GUI: picking a partition image forgets a loaded Clonezilla image and its size rows too (they
  stayed); loading a different backup resets the source-partition pick; a backup folder handed
  over from Inspect replaces a picked partition image.
- GUI: a failed partition-image decode is logged in the log panel (it went to stderr only).
- GUI: Build Disk's progress is the shared job bar (phase, rate, Cancel); once it ends only the
  result line stays (the bar stayed at 100%).
- GUI: Cancel shows only while a restore runs (another tab's job showed a Cancel that did nothing).
- TUI: Clonezilla images restore (they were refused); load errors are the GUI's ("No backup found
  (metadata.json or Clonezilla image)", "Invalid metadata.json: ..."); the load shows "Loading the
  backup..." on the source step instead of the task box.
- TUI: Minimum sizes each partition as the GUI does (its recorded minimum, defragmented first)
  and by its own index (it sent the engine's Minimum floor for indices 0..n, which skipped
  logical partitions); a device target is sized to the device, as in the GUI and the CLI (it was
  the backup's source size).
- TUI: a `.cbk`'s default target sits beside the `.cbk` (it was beside the temp folder); after
  Esc cancels a run, the final result replaces "Cancel requested...".
- CLI: none.

### Left

- The TUI Restore screen has no single-partition, new-disk or Build Disk mode, remote target,
  trailing free space or per-partition sizes; each is an existing `RestoreAction`.
- Write Image File's target, confirmation and write are still the Physical Disk Export window
  (`physical_disk_export.rs`, 19 references) and the TUI's own `physical_write_runner` call; one
  write controller would serve both and Inspect's export.
- The partition editor (`partition_editor_modal.rs`, 25) reaches the model for `DiskBuilder`'s
  rows; it is a controller of its own.
- `rb-cli restore` could take `model::restore_plan` once its Minimum and size defaults agree with
  the GUI's (the differences above).
- Pickers are still blocking rfd calls on the UI thread (`EffectHost`).

## Slice 14: the disk writer (Physical Disk Export, Write Image File)

`src/gui/physical_disk_export.rs` (737 lines, 19 engine references) was the Physical Disk Export
window: its own form state, a VBR read for the hidden-sectors hint on the UI thread, a
`Job::spawn` for the target's partitions, the size / alignment / GPT rules and a
`physical_write_runner::start_physical_write` thread polled each frame. Inspect's export and the
Restore tab's Write Image File both opened it. The TUI's Restore screen poured an image source
with its own copy of the partition read, region cycling and request building, on its own
`start_physical_write` thread.

Step 0 (b30a9597, 7119b5ba): unit tests wrote temp paths into the user's
`~/.config/rusty-backup/config.json` recent lists (the TUI's Inspect opens, and anything else
reaching `UpdateConfig::save`). `UpdateConfig::user_config_dir()` now returns
`<temp>/rusty-backup-test-config` under `cfg(test)`, which covers every recent list, the Commander
recent store, the Custom CHD choice and `keybindings.toml`; slice 13's `cfg!(test)` special case
in the TUI Restore screen is gone.

### Steps

1. (8eabc8b4) Model and controller. `model::disk_write_plan` holds the rules: `WriteOpen` (what a
   screen hands over: path, decoded size if known, fs hint, whether it has a table, whether it is
   a superfloppy), `prepare` (measures the decoded size, reads the VBR hint, seeds the
   `WrapForm` from `superfloppy_wrap::suggest_wrap_params`), `read_target_partitions`,
   `partition_fit_warning`, `TYPE_BYTE_PRESETS` and `build_request` (a partition write by byte
   offset, a whole-disk write capped at the device or grown for an image file, the synthesized
   table's size / alignment / GPT type checks). `controller::disk_write::DiskWriteController`
   (`mod.rs`, `target.rs`, `run.rs`) owns:
   - `Open(WriteOpen)` (a job; a new source drops the target), `Close`, `is_open`, `opening()`.
   - The target: `SelectDevice { row, device }` (the partitions are read in a job; the same disk
     is not re-read), `SetTargetImage` (the TUI's image-file target), `reconcile_devices` (the
     target follows its row, as the window's index did), `SetTargetPartition`,
     `CycleTargetPartition` (whole disk, each partition, whole disk), `target_partitions()`
     (`TargetPartition` with position and label), `partition_warning()`.
   - The form: `SetSynthesize`, `SetTableKind`, `SetAlignment`, `SetCustomAlignment`,
     `SetTypeByte`, `SetSize`; `can_synthesize()`; edits are refused while a write runs.
   - `Write`: `build_request`'s refusal lands in `error()`; a device asks
     `Effect::AskText(purpose::DEVICE_NAME)` whose validator wants the device's exact name (checked
     again on `Answered`), an image file `Effect::Confirm(purpose::OVERWRITE_FILE)`. The write is a
     job over `physical_write_runner::run_worker` through `run_mirrored` (bytes, log lines and
     cancel mirrored, wake lock held); `progress()`, `result()` (bytes written or the error),
     `has_run()`, `is_writing()`, `Cancel`. `wait_idle` / `wait_finished` for a headless shell or
     a test.
2. (036d6883) The egui window renders and maps; it keeps only its `EffectHost`. Inspect hands it
   `InspectController::physical_source()` (now a `WriteOpen`; the size is measured in the writer's
   job, not on the UI thread), the Restore tab `WriteOpen::image(path, size)` from
   `HostRequest::WriteImage`. Both tabs poll it in the background too.
3. (this commit) `src/gui/physical_disk_export.rs` joins `ENFORCED`. The TUI's Restore screen pours
   an image source through a headless `DiskWriteController` (opened when the image loads, the
   device handed over when picked, Space / arrows send `CycleTargetPartition`, Start answers the
   writer's question after the screen's own `y` prompt, the bar and result follow the job;
   `the_restore_screen_writes_an_image_through_the_disk_writer`). `read_target_partitions` and
   `next_write_region` are gone from `tui_app.rs`. A ui scenario vm `disk_write`
   (`tests/ui_scenarios/disk_write_partition_and_superfloppy.toml`,
   `disk_write_image_file.toml`) writes into one partition of a disk-image file standing in for a
   device behind the typed name (a wrong name writes nothing), wraps a superfloppy in a fresh MBR,
   and writes to an image file after a yes.

Shells' engine references 291 -> 263 (`physical_disk_export.rs` 19 -> 0, `tui_app.rs` 66 -> 57).

`rb-cli write` keeps its own request: its `--partition` takes the `IMG@` selector forms, and its
system-disk and mount preflight (`cli::device_safety`) runs before the write.

### Behaviour changes

- GUI: the device name is typed into the shared text prompt that "Write to disk..." opens (OK
  enables only on the exact name); it was a field under the form that enabled the button.
- GUI: opening the window measures the source (decoded size, VBR hint) in a job ("Reading the
  source..."); Inspect measured the decoded size on the UI thread when Physical Disk Export was
  clicked.
- GUI: the write's progress is the shared job widget (phase, percentage, rate and time left,
  Cancel); the source, wrap and completion lines and "Wrote N to the target" / "Disk write failed:
  ..." go to the log panel (they were dropped).
- GUI: without a synthesized table the synthesized partition's size checks no longer run, so an
  image exactly the device's size (or not a multiple of 512 bytes) is no longer refused with
  "Partition end ... exceeds target disk size"; a source larger than the target is still refused
  (by the runner's fit check).
- GUI: a refreshed device list whose row no longer exists clears the target (the combo showed
  "Select a device..." while the old index stayed).
- TUI: a partition write addresses the partition by its byte offset, as the GUI and `rb-cli write`
  do; on an X68000 SASI disk (256-byte sectors) the old `start_lba * 512` landed 256 bytes early.
  Every other table writes the same bytes.
- TUI: a whole-device write is capped at the device's size up front (it was the larger of the
  device and the image, so an oversized image failed at the end of the device); the device's size
  comes from the device list instead of a second enumeration at Start.
- TUI: a source too large for the chosen partition, or a partition that vanished, is refused before
  anything is written with the controller's message ("Source is X but partition N holds only Y").
- TUI: loading a new image source keeps the device target on the form (the writer is re-pointed at
  it and its partitions re-read).
- CLI: none.

### Left

- The TUI has no synthesize form (its image source always counts as having a table) and no Inspect
  export, so it never opens the writer for a superfloppy.
- `rb-cli write` could build its request through `disk_write_plan::build_request` once its
  partition selector maps to the plan's `PartitionInfo`.
- The dialogs the Inspect and Restore tabs host that still reach the engine:
  `partition_editor_modal.rs` (25), `resize_popup.rs` (22), `remote_browser.rs` (12),
  `floppy_convert_dialog.rs` (8), `progress.rs` (7), `expand_hfs_dialog.rs` (4),
  `alto_resize_dialog.rs` (2).
- Pickers are still blocking rfd calls on the UI thread (`EffectHost`).

## Slice 14b: the partition window

`src/gui/partition_editor_modal.rs` (998 lines, 25 engine references) drew and edited two model
objects by `&mut`: Inspect's `PartitionEditor` (Edit Partition Table) and Restore's `DiskBuilder`
(Build Disk). The size-mode stamping, the Apply rule (validate, then apply only with no errors and
some edits), the layout bars' placement, the type catalog lookups and a Build Disk row's source
pick (an `rfd` call and a decoded-size read on the UI thread) were all in the view.

### Steps

1. (099ed965) `controller::partition_edit` (`mod.rs`, `table.rs`, `build.rs`). The editing state
   stays in the model objects the hosts own, because their apply jobs consume them; the module is
   the rules over them, shared by both hosts:
   - `TableEditAction` (`SetType`, `SetSizeMode` (Original / Minimum stamp the size field),
     `SetSizeText`, `SetBootable`, `Delete`, `Undo`, `FillGap`, the Add fields, `Add`, `Validate`)
     through `apply_table_edit`; `TableEditView` (rows, `BootStyle`, gaps, the Add fields, the next
     slot, the Current / After `LayoutBar`s as `BarSegment`s, errors, status, `can_apply`).
     `InspectAction::Table` forwards to it and `InspectController::table_editor()` serves the view;
     `ApplyTableEdits` validates first and refuses a backup folder.
   - `BuildAction` (`SetKind`, `SetAlign`, `SetHeads`, `SetSectorsPerTrack`, the row fields,
     `PickRowSource`, `ClearRowSource`, `MoveRow`, `RemoveRow` (never the last row), `AddRow`,
     `Validate`) through `apply_build`; `BuildView` (kinds, alignment hint, geometry, names, rows
     with their planned start, slots, the planned `LayoutBar` or why it cannot be placed,
     `can_create`). `RestoreAction::Build` forwards to it; `PickRowSource` is an `Effect::PickFile`
     (`purpose::BUILD_SOURCE`), and the picked image's decoded size is measured in a job
     (`build_source_probing()`); `BuildApply` validates first.
   - `type_choices` / `describe_type` (the catalog), `TableKind` and `SizeMode` re-exported.
   The egui window draws the views and returns each frame's edits (`show_table`, `show_build`); its
   render test feeds plain views. `tests/ui_scenarios/restore_build_disk.toml` plans a disk (a
   layout that cannot be placed, a fixed one, a second row) and builds it onto an image file.
2. (this commit) `src/gui/partition_editor_modal.rs` joins `ENFORCED`.

Shells' engine references 263 -> 238 (`partition_editor_modal.rs` 25 -> 0).

### Behaviour changes

- GUI: a Build Disk row's source image is measured in a job after the pick ("Measuring the source
  image..." beside Add Partition; Choose... and Create Disk wait for it); it was read on the UI
  thread. The picker is the tab's `EffectHost` picker (same filters and title).
- TUI, CLI: none.

### Left

- The TUI has no table editor or Build Disk; both would drive the same actions.
- Inspect's other dialogs: `resize_popup.rs` (22), `remote_browser.rs` (12),
  `floppy_convert_dialog.rs` (8), `progress.rs` (7), `expand_hfs_dialog.rs` (4),
  `alto_resize_dialog.rs` (2).

Each slice: `cargo test` for what it touched, `tests/ui_scenarios.rs`,
`scripts/check-boundaries.sh`, the GUI build (`cargo check` / `clippy --bin rusty-backup`),
and the vintage proxy build from CONTRIBUTING.md § Rust 1.73 floor.

## Slice 15: the Inspect tab's other dialogs

The dialogs the Inspect tab (and its browse view) host that still reached the engine:
`resize_popup.rs` (22), `remote_browser.rs` (12), `floppy_convert_dialog.rs` (8), `progress.rs` (7),
`expand_hfs_dialog.rs` (4), `alto_resize_dialog.rs` (2), `squashfs_budget_dialog.rs` (2). One
dialog per step, smallest first; each joins `ENFORCED` in its own commit.

### Steps

1. `src/gui/progress.rs` (7 -> 0): the shared job widget takes `controller::app::Snapshot`,
   and `controller::app` re-exports `RateTracker` and `format_size`. No behaviour change.
2. `src/gui/alto_resize_dialog.rs` (2 -> 0): `controller::alto_resize::AltoResizeController`
   (`Open(path)`, `SetTarget`, `SaveAs` -> `Effect::PickFile(purpose::OUTPUT)`, `Picked`, `Close`;
   `summary_line`, `fit_warning`, `result`, `can_save`, `is_writing`, `wait_idle`) over
   `model::alto_resize` (`AltoTarget`, `summarize`, `resize_to`, the same clone and PDI write the
   dialog ran). The shared `controller::job_outcome` reads a finished job.
   `tests/ui_scenarios/alto_resize.toml` re-lays a blank Diablo 31 pack onto a Diablo 44.
   Behaviour: the summary is read and the pack written in jobs (both ran on the UI thread); the
   window opens at once with "Reading the Alto pack..." and closes with the same log line if the
   file is not a pack; the Save As picker is the shared `EffectHost` one (titled, starting in the
   home folder on Linux).
3. `src/gui/squashfs_budget_dialog.rs` (2 -> 0): the size-budget form moved into the browse
   view's modes. `controller::commander::BudgetForm` (`screen/budget.rs`: the seeded fields,
   `selected`, `ceiling`, `over_capacity`) is what the measurement job now leaves in the modes
   (`size_budget()` replaces `take_size_plan()`); `ModeAction::Budget(BudgetAction)` edits it,
   `Start` installs the budget and probes for editing (refused while over capacity), `Cancel`
   drops it. The egui dialog is a function that draws the form and returns the frame's edits.
   `edit_mode_on_squashfs_asks_for_a_size_budget_first` drives it headless. No behaviour change.
4. `src/gui/expand_hfs_dialog.rs` (4 -> 0): `controller::expand_hfs::ExpandHfsController`
   (`Open(ExpandSource)` from Inspect's `HostRequest::ExpandHfs`, `SetFlatHfv`, `SetBlockSize`,
   `SetSizeMib`, `PickOutput` -> `Effect::PickFile(purpose::OUTPUT)` with the dialog's file name
   and filter, `Picked`, `Start`, `Close`; `size_bounds` (used space up to the block size's ceiling,
   the HFV cap), `block_choices`, `lines`, `outcome`, `editable`, `can_start`). The expand is a job
   that waits on `hfs_expand_runner::start_hfs_expand` (unchanged, so the bytes are the same) and
   mirrors its step and lines. `rb-cli expand` keeps its own flags and output over the same runner.
   `tests/ui_scenarios/expand_hfs.toml` grows a 4 MiB flat HFV to 12 MiB.
   Behaviour: the target size is clamped when it is edited rather than every frame (same
   result); the Save As picker is titled ("Save Expanded Volume").
5. `src/gui/floppy_convert_dialog.rs` (8 -> 0): `controller::floppy_convert::FloppyConvertController`
   (`Open(Option<PathBuf>)`, `PickSource` / `PickOutput` -> `Effect::PickFile`, `Picked`,
   `SetTarget`, `Start`, `Close`; `source_kind`, `source_error`, `same_format`, `outcome`,
   `editable`, `can_start`) over `model::floppy_convert` (`FloppyTarget`, `probe_source`: the
   256-byte sniff and size cap; `convert`: `convert_floppy_container` and the summary line). The
   sniff and the conversion are jobs. `rb-cli floppy convert` keeps its verb (its single and bulk
   modes print their own lines over the same engine call).
   `tests/ui_scenarios/floppy_convert.toml` rewrites an XDF as an HDM.
   Behaviour: the source is sniffed in a job (it was read on the UI thread when the dialog opened
   or a file was picked); the window has no title-bar X and Choose File... is disabled while a
   conversion runs (closing it then used to drop the dialog with the worker still writing, and a
   new source reset the form under it); the result shows the "Wrote N bytes" line under the
   summary instead of in a scroll list; the pickers are titled.
6. `src/gui/remote_browser.rs` (12 -> 0): `controller::remote_pick::RemotePickController` (cfg
   `remote`, which every GUI build has): `Connect` -> `Effect::AskText(purpose::CONNECT)` with the
   browser pane's `validate_remote_addr` (re-asked with the error on a bad answer; the default port
   is added as before), the connect and the "Pick Another Image" reopen (`BrowseOn`) as jobs, each
   folder read as a job (`Enter`), `Up`, `OpenImage` (hands the host a `RemoteImage` through
   `take_request()` and collapses), `Disconnect`; `rows()` (`PickRow`: openable images tinted only
   while browsing the host), `hint`, `working`, `error`, `take_log` (the same "connected to ..."
   lines). `tests/remote_filesystem.rs` `inspect_remote_pick` drives it against a loopback
   daemon (the ui scenarios have no daemon).
   Behaviour: the connect prompt is the shared text prompt and validates the address as you type
   (a port outside 1..65535, spaces or a slash are refused; it accepted anything non-empty and
   failed at connect); a connect or reopen shows the shared job widget instead of a spinner;
   Disconnect is ignored while a connect is still running (its button was already disabled).
7. `src/gui/resize_popup.rs` (22 -> 0): `controller::resize::ResizeController` starts from
   Inspect's `ResizePlan` (`Open`) and owns the size grid (`ResizeRow`: `SetMode` stamps Original /
   Minimum, `SetSizeText` edits a Custom row), the Current / After bars (`layout()`, as
   `partition_edit::LayoutBar`s plus their shared scale), `Preview` (`PreviewRow`s or
   `plan_error`), `AcceptDeviceRisk`, `Apply` and `Cancel`, and tells the host to re-read the table
   when a write ends (`take_disk_changed`, which the tab turns into `InspectAction::DiskChanged`).
   `model::partition_resize` holds the one size-field parser (`parsed_new_size`, `stamp_mib`, with
   the popup's regression tests) and `run_resize`, the popup's worker body moved verbatim (open
   read-write or the device handle, VHD detection, `apply_resize`, the shrunk-file trim). The
   write is a job over it through `run_mirrored` (engine lines to the log panel as they come,
   bytes to the bar). `tests/resize_suite` `the_resize_controller_writes_the_same_bytes_as_the_engine`
   checks the controller's bytes against `compute_resize_plan` + `apply_resize` on an IRIX disk;
   `tests/ui_scenarios/resize_shrink.toml` refuses a bad size, then shrinks an EFS partition and
   checks the file was trimmed. `rb-cli resize` resizes one filesystem inside its partition (a
   different operation) and is unchanged.
   Behaviour: the write's progress is the shared job widget (step, percentage, rate, time left);
   its Cancel still only logs "Resize cancellation requested..." because `apply_resize` has no stop
   point (as before).

Shells' engine references 238 -> 181 (`resize_popup.rs` 22, `remote_browser.rs` 12,
`floppy_convert_dialog.rs` 8, `progress.rs` 7, `expand_hfs_dialog.rs` 4, `alto_resize_dialog.rs` 2,
`squashfs_budget_dialog.rs` 2, each -> 0 and enforced).

### Behaviour changes

- GUI: listed with each step above. In short: the Alto summary and write, the floppy source sniff,
  and the remote reopen run as jobs instead of on the UI thread; pickers are the shared
  `EffectHost` ones (titled); the remote connect prompt validates the address as it is typed; the
  floppy converter cannot be closed or re-sourced while it writes; the resize and remote waits use
  the shared job widget.
- TUI: none (it has none of these dialogs).
- CLI: none. `rb-cli expand`, `rb-cli floppy convert` and `rb-cli resize` keep their own argument
  handling and output; sharing the controllers' validation would change their messages.

### Left

- Resize's Cancel does nothing but log a line: `partition::resize::apply_resize` has no
  cancellation point. Making it stop safely mid-move is an engine change for a later slice.
- The TUI has none of these dialogs; each is now a controller a TUI screen can drive headless.
- The remote picker could become the browser pane with a "pick image" profile instead of its own
  small listing controller.
- Pickers are still blocking rfd calls on the UI thread (`EffectHost`).
- Shells still reaching the engine: `tui_app.rs` (57), `optical_tab.rs` (33), `gui/mod.rs` (22),
  `optical/browse_view.rs` (19), `archives_tab.rs` (12), `metadata_editor.rs` (11),
  `bulk_convert_dialog.rs` (7), `file_detail.rs` (5), and a few one- to three-reference helpers.

## Slice 16: the Optical tab

`src/gui/optical_tab.rs` (1,398 lines, 33 engine references) held the Optical tab in the view: the
drive list and its scan job, the "Add remote daemon" connect on a `spawn_guarded` thread, the
detect job, Eject and CHD Info on the UI thread, the output rules, and three worker threads (rip,
rip to CHD, conversion) polled each frame. `src/optical/browse_view.rs` (1,451 lines, 19) was an
egui widget living in the lib: the disc open, folder reads, preview, tree listing and the export
of the marked entries all ran on the UI thread, and extraction on its own thread.
`src/gui/optical_audio.rs` (3) ran CD-DA playback on a detached thread. The TUI's rip screen listed
drives and ran `run_rip` on its own `spawn_guarded` thread.

### Steps

1. (bb8bebbf) Model and controller. The bodies moved out of the views unchanged, so the files a
   rip, conversion, extraction or export writes are the same bytes:
   - `model::optical_ops`: `OutputFormat` (labels, extensions, `converts_from`: never to the
     source's own format), `detect`, `default_output_name`, `rip`, `run_conversion`,
     `rip_to_chd` (the temporary BIN/CUE beside the CHD, the cancel relays), `chd_info`, `eject`.
   - `model::optical_browse`: `DiscSource { path, physical }`, `open_disc` (the probe, the
     NKit hint, the boot areas), `DiscChoice` (`read_root`, `list_dir`, `preview`,
     `tree_listing`, `extract` with its counts reported through the job, `export` through
     `fs::export_selection`), `fs_choice_labels`, `marked_export_entries`, `FileContent`,
     `hex_dump`, `format_size`.
   - `model::cd_audio_player` (cfg `audio`): `read_tracks` and `play`, the rodio body as a job
     (cancel is Stop).
   - `controller::optical::OpticalController` (`mod.rs`, `run.rs`, `remote.rs` cfg `remote`):
     source mode, the drive list (`FindDrives` at start-up logs "Found N optical drive(s)",
     `RefreshDrives` logs "Refreshed: ..."), `SelectDrive`, `Eject`, `PickImage` / `OpenImage`
     (`HostRequest::RememberRecent`), the detect (re-run on every source change; it reopens the
     browser when one was open), `BrowseContents`, `CloseDisc`, `ChdInfo`, the output form
     (`SetOutputFormat` refuses a format `format_choices` disables, `PickOutput` with the
     suggested name, `SetEjectAfter`), `Start(Option<ChdChoice>)` (a Custom CD choice is saved to
     config.json; `saved_chd_custom` restores it with the CD rules), `Cancel`, the shared bar
     (`progress() -> OpticalBar`), `last_result`, the remote dialog (`OpenAddRemote`,
     `ConnectRemote(addr)` as a job, the recent addresses), `wait_idle`.
   - `controller::optical::browse::DiscBrowseController`: the disc tree (`Expand` lists a folder
     in a job), `SelectFs` (hybrid side or boot area), `Select` (a file's preview in a job),
     marks, `Export` (`PickFile` / `PickFolder` by format), `SetForkMode`, `Extract`
     (`PickFolder`, then a job; `CancelExtract`), `Tree` / `CopyTree` / `SaveTree` / `CloseTree`.
     The tab controller forwards `OpticalAction::Browse` and the browser's pickers.
   - `controller::optical::audio::AudioPlayerController` (cfg `audio`; `AudioAction` always):
     the track list read in a job on each source change, the player window, `Play` / `Stop`,
     `now_playing` (`PlayState::Preparing` / `Playing(secs)`).
2. (352fbfc6) The egui tab, `src/gui/optical_browse_view.rs` (moved out of the lib, now view
   functions over `DiscBrowseController`) and `src/gui/optical_audio.rs` render and map. The tab
   keeps only widget state: the CHD options control, the remote address being typed, the recent
   list mirror and whether it drives the shared bar. `optical::browse_view` is gone.
3. (4b1448bb) The TUI's rip screen drives a headless `OpticalController`: the drive scan, the
   pick, the rip with its bar, Esc's cancel and the result
   (`the_optical_screen_starts_the_rip_through_the_controller`).
4. (1c6605ae) `optical_tab.rs`, `optical_browse_view.rs` and `optical_audio.rs` join `ENFORCED`
   (`src/optical/browse_view.rs` leaves `SHELLS`). The ui scenario vm `optical`
   (`tests/ui_scenarios/optical_convert_browse_extract.toml`) converts the hybrid fixture disc
   to BIN/CUE, browses its HFS side, previews and extracts a forked file (AppleDouble) and lists
   the tree. A scenario may name the features it needs (`requires = ["optical"]`); a build
   without them (the vintage manifest) skips it.

Shells' engine references 181 -> 113 (`optical_tab.rs` 33 -> 0, `optical/browse_view.rs` 19 ->
gone, `optical_audio.rs` 3 -> 0, `tui_app.rs` 57 -> 44).

### Why the disc browser is not the shared browser pane

The browser pane already opens optical images in Commander (`commander_source::open_optical`,
hybrid sides through `OpticalFilesystem`), so one pane with an optical profile was the first
choice. It does not fit cleanly yet:

- The Optical tab browses a drive's disc through the device-backed reader
  (`open_physical_filesystem`, the elevation check); the pane has no physical-drive source.
- HP LIF / PReP boot areas are filesystem choices here; the pane offers only the hybrid sides.
- Extraction writes through its own walker (fork modes, Mac dates from opticaldiscs'
  timestamps, `sanitize_filename`); the pane's extract goes through the screen's
  `ExtractOptions` and the rusty-backup `FileEntry` translation. Moving onto it could change the
  bytes an extraction writes, which this slice must not do.
- The Inspect profile is an editing surface (staging, edit modes, metadata editors) that a
  read-only disc never uses; the game identity and the hybrid picker would be optical-only
  additions to it.

So the browser is its own small controller over `model::optical_browse`. Merging the two is a
decision for later (below). (Slice 18 made it: the gaps above are closed there.)

### Behaviour changes

- GUI: the start-up drive scan is a job, so the window opens without waiting for it; "Found N
  optical drive(s)" is logged when it ends.
- GUI: Eject, CHD Info and the remote connect run in jobs (Eject and CHD Info ran on the UI
  thread). The disc browser's open, filesystem switch, folder reads, file preview, tree listing,
  tree save and the export of the marked entries run in jobs (all ran on the UI thread) and show
  "Loading..." or the shared job widget meanwhile. Extraction shows the shared job widget
  ("Extracting n/m files: path", bytes, rate, time left, Cancel) instead of its own bar.
- GUI: changing the source mode detects the new mode's source (the per-frame check could leave
  the other mode's disc info showing); an image dropped or handed over by Inspect while another
  tab's job runs is detected at once (it waited for the controls to re-enable).
- GUI: a drive the browser cannot open unelevated shows the error (the browser stayed hidden with
  it set); a file whose disc cannot be reopened for the preview shows "Cannot open filesystem:
  ..." (the preview spun "Loading..." for good); a folder whose read fails shows the error and is
  not read again until the filesystem is switched or reopened (the view re-read it every frame on
  the UI thread).
- GUI: the image and output pickers are the shared `EffectHost` ones, titled ("Open Disc Image",
  "Save Output As"); the browser's extract, export and tree-save pickers keep their titles and now
  start in the home folder on Linux like the others.
- GUI: closing the Tree View window drops its text (Tree builds it again); the remote address
  field clears whenever the dialog closes (it cleared only after a connect).
- GUI: the CD-DA track list is read in a job on a source change (it was read on the UI thread);
  playback is a job (it was a detached thread); same states, labels and Stop.
- TUI: the drive scan is a job ("Looking for optical drives..." while it runs); picking a drive
  detects the disc and the options step shows it ("Disc: format / filesystem label"); after Esc
  cancels a rip the final result replaces "Cancel requested..."; a Start the controller refuses
  reads "Cannot start the rip: <why>"; a panic reads "Rip failed: <message>" (it read "rip thread
  panicked").
- CLI: none.

`rb-cli optical rip` / `convert` keep their own loops (`drain_rip` / `drain_convert` print the
progress and log lines, `--device rb://...` resolves its own target) and `rb-cli optical extract`
is a different implementation (case-collision modes, POSIX modes, tar output); driving the
controller would change what they print or write.

### Decisions for the user

- Move the disc browser onto the browser pane (an `optical` profile) once the pane has a
  physical-drive source and boot areas and an extraction through it is shown to write the same
  bytes, or keep the two.
- `rusty_backup::theme` stays in the lib (it was there for the old widget); it could move back to
  `src/gui`.

### Left

- The TUI has no convert, CHD rip, disc browser or player; each is an existing `OpticalAction`
  (its image-ops menu still pre-fills `rb-cli optical ...` command lines).
- `rb-cli optical` verbs could share `model::optical_ops` / `optical_browse` once their output is
  reconciled with the GUI's.
- Pickers are still blocking rfd calls on the UI thread (`EffectHost`).
- Shells still reaching the engine: `tui_app.rs` (44), `gui/mod.rs` (22), `archives_tab.rs` (12),
  `metadata_editor.rs` (11), `bulk_convert_dialog.rs` (7), `file_detail.rs` (5), and a few one- to
  three-reference helpers.

## Slice 17: the rest of the egui shell

What was left in `src/gui` after slice 16 reached the engine 69 times: the app shell
`gui/mod.rs` (22), `archives_tab.rs` (12), `metadata_editor.rs` (11), `bulk_convert_dialog.rs`
(7), `file_detail.rs` (5), `journal_view.rs` (3), `source_picker.rs` and `settings_dialog.rs` (2
each), and one each in `ui_logger.rs`, `size_mode_row.rs`, `partition_bar.rs`, `fonts.rs` (a
comment) and `chd_options_ui.rs`. One step per file or group; each commit builds.

### Steps

1. (ef7b0d5c) Small helpers. `controller::app` re-exports plain data only: `format_size`,
   `SizeMode`, `Notify` (the wake a shell implements), the picker extension lists
   (`DISK_IMAGE_EXTS`, `MAC_ARCHIVE_EXTS`, `archives_picker_exts`) and, cfg `chd`, the CHD codec
   choices (`controller::app::chd`). The journal viewer's transaction classification moves to the
   browser pane (`journal_txn_classes`, `journal_block_class`). `controller::settings::
   SettingsController` owns the Settings form, its save (and the Windows file associations) and
   Clear Recent Files over a `ConfigStore` (`try_save` reports a failed write), so its test and
   scenario never touch config.json.
2. (cdeaeab7) `file_detail.rs`: the preview's text-or-binary classification (`FileContent`,
   `detect_content_type`, the embedded HFS file-type table) moves to `model::entry_detail`.
3. (66f0d042) `metadata_editor.rs`: `controller::browser::metadata` (re-exported as
   `controller::commander::metadata`) holds the File Info editors' rules: each row's effective
   value (a staged edit over the on-disk one), the type/creator and ProDOS dictionaries, and the
   field parsers. The egui editors stage the same `MetadataEdit`s.
4. (48a3bcd6) `bulk_convert_dialog.rs`: `controller::bulk_convert::BulkConvertController` owns the
   form (folders through `PickFolder`), the scan job, the review (ticks, extension), the
   output-conflict question (`ResolveConflicts`: Skip / Overwrite / Back) and the conversion, a
   job waiting on the unchanged `bulk_convert_runner::start_bulk_convert`. The app's progress
   strip draws `progress()`. The TUI Bulk screen drives the controller headless.
   `tests/ui_scenarios/bulk_convert.toml`.
5. (226808ed) Explicit `as_str` / `as_slice` / `as_path` at the call sites steps 2-4 added, for
   mrustc.
6. (e6ac5261, 5ae0bdce) `archives_tab.rs`: `controller::archives::ArchivesController` opens an
   archive in a job (`model::mac_archive_extract`, the bodies moved from the tab and the TUI),
   keeps the rows and ticks, extracts all or the ticked entries in a job (the same
   `extract_all` / `extract_filtered`), and writes a single-entry disk image payload for Inspect
   (`HostRequest::OpenInInspect`). The TUI Archives screen drives it headless.
   `tests/ui_scenarios/archives_open_extract.toml`.
7. (a3bb09b2) `gui/mod.rs`: `controller::app` becomes a directory. `app/shell.rs`: the active tab,
   `route_drop`, `busy_elsewhere`, `share_backup` (the backup folder Restore and Inspect share:
   publish a new one, adopt only when empty), the device list (`load_devices` at start-up with
   the same log lines, `RefreshDevices` as a job), the elevation gate and its texts,
   `RequestElevation` / `ConfirmElevation`, `picker_start_folder`. `app/update.rs` (cfg `gui`):
   the start-up release check and the Windows in-app install as jobs. Inspect gains
   `InspectSource::ImageFile`: an image file as the user named it is prepared
   (`model::image_prep`, floppy containers kept) in a job, then opened, so no view calls
   `prepare_disk_image_path`; the source picker hands back the picked path. The unused
   `open_disk_image_for_reading` is gone. `tests/ui_scenarios/{app_shell,settings}.toml`.
8. (81425388) The colour theme lives in the GUI again (`gui::theme`; nothing outside `src/gui`
   used it, so nothing had to move into the controller or model). `model::update_runner` has no
   caller left and is removed.
9. (5e5d5552) `scripts/check-boundaries.sh` enforces all of `src/gui` (a planted reference in a
   new file fails it); CONTRIBUTING's list follows.

Shells' engine references 113 -> 20 (all of `src/gui` 69 -> 0; `tui_app.rs` 44 -> 20).

### Behaviour changes

- GUI: the Bulk Convert, Archives and Settings pickers are the shared `EffectHost` ones (titled:
  "Choose Source Folder", "Choose Output Folder", "Open Mac Archive", "Extract All To",
  "Extract Selected To"). Bulk Convert's run is a job; its strip text, log lines and the
  "cancellation requested" line are unchanged.
- GUI: an archive extraction is a job (it was a `spawn_guarded` thread); its warnings, result
  line and bar text are the same. An archive dropped or handed over while an extraction runs
  still opens (the running extraction keeps its own copy).
- GUI: a dropped, command-line or Recent image opens through Inspect's prepare job (it ran
  `prepare_disk_image_path` on the UI thread; with Inspect's flags that step never decodes, so
  the opened file is the same). The Recent list records the dropped path itself. A failed
  prepare logs "Failed to materialize ... - opening raw bytes" for every route (Recent logged
  "Failed to decompress" at error level).
- GUI: the update check is a job (it was a detached thread); the Windows install is a job over
  `update::download_and_apply_update` (it was `model::update_runner`'s thread), same states and
  buttons.
- TUI: the Bulk screen scans in a job ("Scanning the source folder..." while it runs) and asks
  before overwriting outputs that already exist (s Skip / o Overwrite / Esc Back; it overwrote
  silently); after Esc cancels a run, the final counts replace "Cancel requested...".
- TUI: the Archives screen opens and extracts through the controller's jobs ("Opening the
  archive..." / "Extracting..." on the status line instead of the task box); the status texts
  are unchanged.
- CLI: none. `rb-cli convert` and `rb-cli archive` keep their own loops and output; driving the
  controllers would change what they print.

### Decisions for the user

- The Inspect prepare step (`InspectSource::ImageFile`) is a pass-through for every format today
  (Inspect keeps floppy and gzip containers so edits go back into them). Keep the job as the
  one place a future decode would run, or drop it and open the path directly.

### Left

- `src/cli/verbs/tui_app.rs` still reaches the engine 20 times (its Inspect, Backup and Restore
  screens' device lists and image probes, the update check, the palette).
- The TUI keeps its own tab bar, device list and update check; the `controller::app` shell rules
  are there for it.
- The optical disc browser onto the shared browser pane: slice 18.
- Pickers are still blocking rfd calls on the UI thread (`EffectHost`).

## Slice 18: the disc browser on the browser pane

User decision (2026-11-02): one browser, not two. The Optical tab's disc browser
(`DiscBrowseController` + `src/gui/optical_browse_view.rs`) moves onto the browser pane, and the
four gaps slice 16 listed are closed in the pane rather than special-cased in a view.

### Steps

1. (3896661c) Docs: CLAUDE.md § Amiga no longer names the removed
   `gui::materialize_amiga_image_path`.
2. (fd5944fd) The bytes, pinned first. `controller::optical::identity_tests` drives the slice-16
   browser over five discs (the hybrid ISO 9660 + HFS+ fixture, the same disc as BIN/CUE and as
   CHD, a new Rock Ridge + Joliet fixture `tests/fixtures/optical/rock_ridge.iso.zst` with a
   symlink, a deep tree, an empty file, a 63-character name and two names that collide on the
   host, and that disc with a PReP boot partition patched into sector 0) and every filesystem
   choice: the tree text, extraction of the root in each of the six fork modes and of each
   top-level entry, the export of the marked entries in all eleven formats and loose in each fork
   mode, and the preview. 288 digests in `GOLDEN`, stable across runs.
3. (d3f64b98) The pane gains a disc source, a new source kind rather than a view special case:
   - `BrowserPaneAction::OpenDisc(DiscSource)` (refused unless the profile opens images),
     `open_fixed_disc` / `CommanderController::open_disc` for a host. `DiscSource { path,
     physical }` lives in `model::commander_descend`, so every build has the action; without
     `optical` the open fails with "compiled without optical-disc support".
   - `model::optical_browse::open_source` probes an image (`DiscImageInfo::open`) or a drive
     (`open_physical`; `ensure_physical_enabled` first), finds the boot areas, and opens choice 0
     as a rusty-backup filesystem (`DiscChoice::filesystem`, `OpticalFilesystem::from_inner`
     named by the opticaldiscs display name: "ISO 9660", "HFS", "HFS+", or the boot area's label).
     A failed probe is the source error ("Failed to open disc image: ...", which is what an
     unelevated drive gives); a choice that does not open (the NKit hint included) leaves the
     other choices offered.
   - The filesystem choices are the pane's partition picker (`partition_labels`): the primary,
     the hybrid sides, the HP LIF / PReP boot areas (labels: `optical_choice_labels`, the ones
     Commander already showed). `SelectPartition` switches them even on a fixed source. A drive
     offers only its primary (its reader always presents that one). The game line is
     `disc_game()`.
   - Workers reopen the choice with `ReopenRecipe::Disc(DiscChoice)` (`StageSource::disc()`).
     Commander's own disc open goes through the same source.
   - Byte identity: extraction from a disc runs the Optical tab's walker
     (`HostCopyJob::DiscExtract` -> `DiscChoice::extract_path`, progress through the copy job's
     status); the pane's `ListTree` on a disc is the disc listing (`DiscChoice::tree_listing`);
     an export of a disc's selection goes in path order once per path, as the marked map did.
     The export itself is the screen's generic export, unchanged.
   - `PaneProfile::optical()`: fixed source, read-only, no staging, no rename / new folder /
     delete, no wrappers, combined tree, side-panel details, no tools.
   - `identity_tests` gains the pane runner; all 288 cases match except the preview of a Rock
     Ridge symlink (decision below).
4. (a2c3d9bd) The tab moves over. `OpticalController` no longer hosts a browser: Browse Contents
   pushes `HostRequest::OpenBrowser(DiscSource)`, a new or closed disc pushes
   `HostRequest::CloseBrowser` (and the detect reopens it, as before), and the view reports its
   Close with `OpticalAction::BrowserClosed`; `is_browsing()`. `optical::disc_browser(services,
   keymap)` builds the browser (`CommanderController::single` + `PaneProfile::optical`, exports
   with AppleDouble forks). The egui tab hosts `BrowseView::optical()`: the Inspect tab's browse
   view (the same combined tree, mark checkboxes, export bar, side panel and Extract) with a
   "Disc Browser" header (filesystem picker, Game, Tree, View, Close); the Inspect-only extras
   (tar export, Open as Disk Image, the Mac archive actions, Edit as text, Show IDs) stay off for
   a disc. `src/gui/optical_browse_view.rs` (361 lines) and `controller::optical::browse`
   (596) are gone; `model::optical_browse` keeps the open, the walker and the tree (654 -> 495
   lines). The controller tests and `tests/ui_scenarios/optical_convert_browse_extract.toml`
   drive the pane the way a shell does.

### Behaviour changes

- GUI (Optical tab): the browser is the Inspect browse view: rows show the pane's columns and
  the side panel shows the pane's File Info (Commander's metadata rows and preview: a symlink
  shows its target instead of an empty preview); the View menu offers the flat list and the
  folder tree + list (remembered as `optical`); the shared keys work once the tree is clicked;
  the root is a fixed `/` row; the filesystem picker reads "RETRO (ISO 9660)" / "RETRO (HFS+
  (Mac))" (Commander's labels) instead of "ISO 9660 (RETRO)"; a drive offers only its primary
  filesystem (its hybrid choices opened the primary anyway).
- GUI: extraction shows the copy job's window ("Copying <path>", bytes, Cancel) instead of
  "Extracting n/m files: path", and ends with "Copied N file(s) to the extract folder." instead
  of "Extraction complete: N files extracted."; extracting over existing files asks Overwrite /
  Skip / Cancel first (it overwrote silently).
- GUI: the export bar's default format follows the filesystem (HFS / HFS+ -> MAR, ISO 9660 ->
  loose files) instead of always MAR, and its result is the copy job's status line.
- GUI: the tree window has no Show IDs box for a disc; its Copy and Save are the browse view's.
- Commander (GUI and TUI): a disc's volume type reads "ISO 9660" / "HFS" / "HFS+" instead of
  "Iso9660" / "Hfs" / "HfsPlus" (so HFS discs default to MAR exports); a disc whose primary
  does not open still offers its other filesystems; the selection of a disc exports in path
  order.
- TUI: none beyond Commander's disc readout. CLI: none (`rb-cli optical` keeps its verbs).

### Decisions (answered 2026-11-02)

- **Extraction: collapse to one extractor, in a crate** (slice 19). Today the disc walker
  stays behind the pane (`HostCopyJob::DiscExtract`) because the generic `model::extract`
  writes different bytes in 33 of 288 cases. The single extractor picks, per case: Rock Ridge
  symlinks become real symlinks (a small text file where the host refuses symlinks, e.g.
  Windows without the privilege); a whole-disc extract's top folder is the volume name
  (today `_` or a `/`-derived name); AppleDouble / MacBinary headers carry the file's real
  dates from the disc; BinHex writes `.hqx`. The identity goldens are recaptured for exactly
  those cases.
- **Symlink preview:** accepted; the pane shows the target (`identity_tests::PANE_DIFFERS`).
- **Picker labels:** "volume (format)", as Commander had, in both; the format name is italic
  in egui (slice 18b).

### Left

- The TUI rip screen has no Browse Contents; it could host `disc_browser` as the Explorer does
  (the TUI Commander already opens disc images read-only in a pane through the same source).
- A remote daemon's drive is rip-only: the pane has no remote disc source.
- `rb-cli optical extract` is still its own implementation (case-collision modes, POSIX modes,
  tar output).

## Slice 18b: hybrid discs list each Mac file once; italic format names

Found while checking slice 18 in the GUI (2026-11-02).

- **Duplicate rows on a hybrid disc's ISO 9660 side.** Mac hybrid CDs store a file's
  resource fork on the ISO side as an *associated file* (file-flags bit 2, ECMA-119
  §9.1.6) named like the data file; `opticaldiscs` listed it as a second file. Fixed in
  **opticaldiscs 0.16.1** (danifunker/opticaldiscs-rs d9574d3, published): the associated
  file becomes the partner's resource fork (`resource_fork_size`, `read_resource_fork`);
  an orphan stays listed. rusty-backup pins 0.16.1 (2ca0c689); only the root lockfile
  carries opticaldiscs. The 63 identity cases for the hybrid discs' ISO side (`fs0`) were
  recaptured: one row per file, forks in AppleDouble / MacBinary / BinHex / SeparateRsrc
  extracts, Native still errors off macOS (as on the HFS side), and Zip export now works
  (it failed on the duplicate names).
- **Italic format names** (3119ea34). The pane serves `partition_label_parts()` (`LabelParts`:
  lead, format, tail) beside `partition_labels()`; `gui::theme::format_label` draws the
  format italic in the disc browser header and both Commander pickers. The TUI keeps the
  joined text. `model::commander_descend::OPTICAL_FS_NAMES` is the one name table for the
  label builder and the splitter.

## Slice 19: one extractor, in a crate

User decision (2026-11-02): one extractor, living in a crate. The survey found six walkers
that copy a filesystem tree out to host files:

| Walker | Used by | Differs in |
|---|---|---|
| `model::extract` | Commander / Inspect Extract (`HostCopyJob::Extract`) | forks through `fs_mac::fork_export` (an AppleDouble sidecar for Finder info alone); ProDOS `#TTAAAA` names; QNX 2 RS -> LF; a root folder named `_`; symlinks only on Unix; one bad entry skipped |
| `DiscChoice::extract_path` (`model::optical_browse`) | a disc behind the pane (`HostCopyJob::DiscExtract`) | its own fork writer: AppleDouble only with a resource fork, Mac dates from opticaldiscs' timestamps; BinHex writes the data fork; a symlink becomes an empty file; any error stops it |
| `rb-cli optical extract` (`src/cli/verbs/optical.rs`) | the CLI | as the disc walker, plus case-collision modes on a case-insensitive host, POSIX modes, `--path` / `--recursive`, the root's contents straight into `--to`, one bad entry skipped and counted; `--tar` writes a tar instead |
| `export_selection::folder_recurse` | Export as loose files / BinHex / gzip / zstd per file | the root's contents into the folder, symlinks skipped, any error stops it |
| `commander_ops::copy_image_entries_to_host` | F6 copy from an image pane to a host pane | an Overwrite / Skip / Fail answer per file, symlinks skipped |
| `rb-cli get` (`src/cli/verbs/get.rs`) | the CLI | `cp`-shaped: globs, a file to a named target, data forks only, symlinks always as text |

### Plan

1. `crates/fs-extract` (MIT, 0.1.0, beside `fs-tar` under "Filesystem tools" in
   crate_split_plan.md): `extract` (an entry: a file, a folder, or a whole volume) and
   `extract_contents` (a folder's children) over any `disk_core::filesystem::Filesystem`,
   writing forks through `fs_mac::fork_export` (no encoder of its own). `Options` holds the
   fork mode, whether Finder info alone earns an AppleDouble sidecar, symlinks (create, or
   skip), errors (skip the entry, or stop), existing files (overwrite, skip, fail), the CLI's
   case-collision modes, POSIX modes, recursion and whether a volume root becomes a folder
   named after the volume. A `Host` trait carries cancel, progress, the skipped / failed /
   renamed reports, the host name (ProDOS suffix) and a per-file writer override (QNX 2
   text, gzip / zstd per file). `count` and `existing_outputs` (the up-front conflict check)
   move there too. No app types.
2. `fs-mac` 0.1.1: `fork_export::export_file_with_fork_opts` with `ForkExportOptions {
   finder_info_sidecar }`; the old function keeps its behaviour.
3. `OpticalFilesystem::with_extract_metadata()`: entries carry Mac dates (from opticaldiscs'
   timestamps) and POSIX mode / owner. Only the extractors ask for it, so listings, exports
   and archives over a disc keep their bytes.
4. Callers: `model::extract` becomes the app's `Host` over `HostCopyStatus`; a disc source
   goes through it too (`HostCopyJob::DiscExtract` and `DiscChoice::extract_path` go), with
   the disc walker's settings (no Finder-info-only sidecar, stop on error);
   `export_selection`'s folder outputs and `copy_image_entries_to_host` call the crate;
   `rb-cli optical extract --to` calls it with the CLI's settings. Kept: `--tar` (a tar
   writer, not an extractor: its GNU headers carry the disc's modes and owners, which no
   host folder holds) and `rb-cli get` (a `cp`-shaped verb with its own documented
   contract: data forks only, symlinks as text, glob targets).
5. The byte changes, and only these (versus the old disc walker, and where they apply
   versus `model::extract`):
   - **R1** symlinks are real symlinks on the host; where the host refuses one (Windows
     without the privilege, any error), a small text file holding the target (`target\n`,
     as `rb-cli get` writes).
   - **R2** a whole-volume extract's top folder is the volume name, sanitised for the host
     (it was `_`); a volume with no name keeps `_`.
   - **R3** AppleDouble / MacBinary headers carry the file's dates from the source
     filesystem (the disc walker did; the pane's disc listing did not).
   - **R4** BinHex mode writes `.hqx` files (the disc walker wrote the data fork).
   The identity goldens are recaptured only where these rules explain the change.

## Decisions taken 2026-11-02 (assumed accepted)

All behaviour changes listed in slices 11-18 stand. In particular:
- TUI **Minimum** restore uses the GUI's per-partition minimum (slice 13).
- The TUI partition write addresses X68000 partitions by byte offset (slice 14).
- Inspect's image prepare job stays as the one place a future decode would run (slice 17).
- The TUI Bulk screen asks before overwriting outputs (slice 17).
- Still open: **Resize's Cancel does nothing** (slice 15). `apply_resize` has no safe stop
  point; leave it until a resize can stop without leaving a half-moved partition.

## Where Phase 2 stands (2026-11-02)

- Slices 1-18b done. `src/gui` is enforced whole at 0 engine references; the TUI Commander
  and Explorer files are enforced. Shell engine references: 979 at the start of Phase 2,
  **20** now, all in `src/cli/verbs/tui_app.rs` (device lists, image probes, the update
  check, the palette).
- CI was green on every slice through 18 (Solaris 9 / mrustc included); 3119ea34 was
  pushed with CI running. Commits after 3119ea34 are local until the user says push.
- Not yet run on the Phase 2 code: the full fixture regression suite (last full run
  450/450 at 7fc076cd, before slice 10). Suggest it (about 95 min) before Phase 2 exit.

## Next steps

1. **Slice 19: one extractor, in a crate.** A new MIT crate (working name `fs-extract`)
   over `disk-core` and `fs-mac` (the fork encoders already live in `fs-mac` /
   `arc-mac`; `disk-core` can't hold it because `fs-mac` depends on `disk-core`). It
   replaces `model::extract`, the disc walker in `model::optical_browse`, and
   `src/fs/export_selection.rs`'s walk where it duplicates; then `rb-cli optical extract`
   moves onto it if its collision / POSIX / tar modes fit. Byte rules: the four choices
   above; everything else byte-identical (prove it with the identity tests and the
   Commander / Inspect extract tests). Version, publish (publish-crates.yml) and update the
   root, vintage, ppc and sol9 lockfiles.
2. **Slice 20: the TUI's last 20 references** in `tui_app.rs`: drive `controller::app`
   (shell rules, device list, update check) and the existing controllers; enforce the whole
   TUI in `scripts/check-boundaries.sh`.
3. **TUI parity gaps** the controllers already support (Backup partition selection / split
   / CHD codec / remote; Restore single-partition / new-disk / Build Disk; Inspect disk
   tools / Calc min / Export; Optical convert / Browse Contents / player; Inspect dialogs):
   add them, or record each as N/A with a reason in `docs/platform_matrix.md` (Phase 2 exit).
4. **Pickers off the UI thread:** egui still calls rfd synchronously; route `PickFile` /
   `PickFolder` through the portal-friendly async dialogs (needed by GTK4 anyway).
5. **Full regression suite** (`regression-tests`, ~95 min) and a GUI smoke on macOS and
   Windows, then close Phase 2 in `native_frontends_plan.md`.
6. **Phase 4 (GTK4) or Phase 3 (AppKit)** per `native_frontends_plan.md`; the GTK4 and Qt6 /
   Kirigami dev packages are installed on m900.
