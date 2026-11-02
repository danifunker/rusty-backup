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
| 10 | One browser-pane controller with profiles; Inspect browse view and TUI explorer move onto it | steps 1-2 done (4a1918a1, TUI Explorer); step 3 (Inspect browse view) open |

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
- `folder_tree`: a folder tree beside the listing (the TUI Explorer's left pane).
- `tools: Tools { fsck, repair, bless, text_edit, import, metadata, transforms }`.

Constructors: `PaneProfile::commander()` (today's Commander pane exactly: every source,
staging, wrappers, no tools), `explorer()` (fixed source, no staging, folder tree, every
tool), `inspect()` (fixed source, staging, bless / text / import / metadata), and
`.read_only()` on any of them.

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

Step 2 (TUI Explorer):

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

Each slice: `cargo test` for what it touched, `tests/ui_scenarios.rs`,
`scripts/check-boundaries.sh`, the GUI build (on a machine without GTK headers,
swap `rfd` to `xdg-portal` for the check and swap it back; never commit the swap),
and the vintage proxy build from CONTRIBUTING.md § Rust 1.73 floor.
