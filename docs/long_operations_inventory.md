# Long-running operations inventory (2026-10-29)

This is the work list for Phase 1 of [native_frontends_plan.md](native_frontends_plan.md). It records every slow or potentially slow operation reachable from the GUI and the TUI, how it starts today, and what its "wait" state looks like.

Line numbers are a snapshot. Re-check them before editing.

**Path shorthand:**
- `inspect` = `src/gui/inspect_tab.rs`
- `browse` = `src/gui/browse_view.rs`
- `backup` / `restore` = `src/gui/{backup,restore}_tab.rs`
- `pane` / `cmdr` = `src/gui/commander/{pane,mod}.rs`
- `tui` = `src/cli/verbs/tui_app.rs`

**How each operation starts:**
- **SG**: `model::worker::spawn_guarded`
- **RAW**: a raw `thread::spawn`
- **RAW+cu**: a raw spawn wrapped in `catch_unwind`
- **RUN**: a model runner's spawn function
- **SYNC**: runs on the UI thread

## Totals

| Front-end | How started | Distinct operations |
|---|---|---|
| GUI | SYNC (on the UI thread) | about 45 |
| GUI | RAW `thread::spawn` | 21 (4 with `catch_unwind`) |
| GUI | Model runner spawn functions | about 20 (only min_size and bulk_convert are panic-guarded) |
| GUI | `spawn_guarded` | 8 call sites (backup ×3, restore ×5) |
| TUI | Async | 7 (backup, restore, rip, bulk, image write, checksum, connect) |
| TUI | SYNC | everything else |

## Worst wait-state bugs (fix first)

1. **Backup source scan** (backup:1536) runs the elevation prompt, `PartitionTable::detect`, and `partition_minimum_size` for every partition on the UI thread. Image preview (backup:1719) does the same for images.
2. **Browse "Apply staged edits"** (browse:4247) runs `open_editable`, every write, and the sync on the UI thread with no progress. Tar import (3166) and export selection (1638) have the same problem.
3. **Entering CHD edit mode** (browse:2701) copies the whole CHD on the UI thread (`chd_edit.rs:341`).
4. **`prepare_disk_image_path`** (`gui/mod.rs:293`) gunzips `.hdz` (full HDD size) on the UI thread when a file comes from a picker, the recent-files list, or drag-and-drop.
5. **fsck and repair** run on the UI thread, including over remote links: inspect:4538/4805, browse:1114/5770, tui:5088/5113.
6. **Archive sniff during render** (browse:841) reads 16 MB per selection. If the FS open fails, it retries the full open every frame.
7. **Remote I/O on the UI thread:**
   - `list_rip_devices` with daemons attached, at startup and on refresh (optical_tab:163)
   - `remote_browser` `listing.enter` (:241)
   - Commander remote navigation
8. **Partition-table apply and Make Bootable** (inspect:1572/1394) prompt for elevation and write on the UI thread.
9. **TUI host-to-host copy** busy-waits inside the event handler (tui:2671). More generally, every synchronous TUI operation freezes the screen with no "working" frame, because drawing only happens between events (tui:3569).
10. **Missing panic guards.** A panic leaves `finished=false`, so the UI spins forever. Affected:
    - the Inspect worker (inspect:2966)
    - export, chd_expand, repack, swab, cache, hfs_expand, physical_write and provision
    - checksum, every `commander_ops` spawn, archive_edit
    - GUI rip and convert
11. **The busy gate** (`gui/mod.rs:875`) ignores Inspect jobs (export, CHD expand, repack, remote backup, resize) and the browse and Commander writers.

## Status structs: missing cancel and determinate progress

| Gap | Status structs |
|---|---|
| **No cancel** | InspectStatus, CacheStatus, BlockCacheScan, SwabStatus, ExpandStatus, RepackStatus, ChecksumStatus, ApplyStatus, PreflightStatus, WrapperOpenStatus, BrowseOpenStatus, VolumeLabelStatus, FloppyConvertStatus, TreeStatus, TarExportProgress, ExtractStatus (archives), RemoteOpStatus / RemoteConnectStatus, UpdateRunStatus |
| **Not determinate** | InspectStatus, ExpandStatus, FloppyConvertStatus, MinSizeStatus, VolumeLabelStatus, PreflightStatus, WrapperOpenStatus, TreeStatus, TarExportProgress (counts only, no total), RemoteOpStatus |
| **Partly determinate** | BrowseOpenStatus (only during the carve scan), BlockCacheScan (only through its inner cache) |
| **Has cancel but no panic guard** | ExportStatus, ChdExpandStatus, PhysicalWriteStatus, ResizeStatus, StageCopyStatus, HostCopyStatus, ExtractionProgress, ConvertProgress, RipProgress (GUI path) |

## By area

### Backup / Restore

| Operation | GUI | TUI | Start | Progress | Cancel | Notes |
|---|---|---|---|---|---|---|
| Backup | backup:2565 | tui:6126 | RAW+cu | bytes | yes | |
| Remote backup (Backup tab) | backup:2316 | - | RAW+cu | bytes | yes | |
| Remote backup (Inspect) | inspect:2521 | - | RAW+cu | spinner only | no button | the bytes are tracked but never shown |
| VHD whole-disk export | backup:1063 | - | SG | bytes | yes | |
| Source scan (elevate, detect, min sizes) | backup:1536 | - | **SYNC** | none | no | freezes the UI |
| Image preview | backup:1719 | - | **SYNC** | none | no | freezes on large images |
| Deferred min-size | backup:1215, inspect:4871 | - | RUN | phase | yes | |
| Remote connect / load | backup:2004, restore:2038 | tui:2442 | SG / RAW | indeterminate | no | |
| Full / remote / single-partition / new-disk restore | restore:1781/1964/2221/2956 | tui:6683 | SG | bytes | yes | |
| Build disk / provision | restore:2557 | - | RUN | bytes | yes | no panic guard |
| Target partition scan | restore:1253 | tui:1399 | **SYNC** | none | no | elevation prompt on the UI thread |
| Backup metadata load | restore:1300, inspect:2816 | tui:3807/6600 | **SYNC** | none | no | |
| Physical disk export / write | physical_disk_export:527 | tui:6478 | RUN | bytes | yes | no panic guard; target read happens during render (:385) |
| `prepare_disk_image_path` | mod:293 and 7 callers | - | **SYNC** | none | no | |

### Inspect

| Operation | GUI | TUI | Start | Progress | Cancel | Notes |
|---|---|---|---|---|---|---|
| Inspect | inspect:2893 | tui:3848 (SYNC) | RAW, no guard | step text | no | if it panics, Re-inspect stays disabled |
| Volume labels | inspect:5045 | - | RUN | none | no | |
| Export (4 variants) | inspect:2225 | - | RUN | bytes | yes | no panic guard; not covered by the busy gate |
| Expand CHD | inspect:1341 | - | RUN | bytes | yes | no panic guard |
| Repack / defrag | inspect:4421 | palette | RUN | bytes | **no** | no panic guard |
| Swab16 | inspect:4463 | - | RUN | bytes | **no** | |
| fsck / repair | inspect:4538/4805 | tui:5088/5113 | **SYNC** | none | no | |
| Make Bootable | inspect:1394 | palette | **SYNC** | none | no | |
| Partition table apply | inspect:1572 | - | **SYNC** | none | no | |
| Resize | resize_popup:632 | palette | RAW | bytes | yes | no panic guard |
| HFS expand | expand_hfs_dialog:316 | palette | RUN | step text | **no** | the summary step is SYNC (inspect:4514) |
| Alto resize | alto_resize_dialog:148 | - | **SYNC** | none | no | synchronous by design (doc :6) |
| Floppy convert | floppy_convert_dialog:372 | - | RAW | indeterminate | no | no panic guard |
| Partclone scan | inspect:5392, pane:2624 | - | RUN | blocks | no | loading an existing cache is SYNC (5376) |
| Seekable-zstd cache build | inspect:5407 | - | RAW | bytes | **no** | no panic guard |

### Browse

These operations are GUI-only:

| Operation | Location | Start | Notes |
|---|---|---|---|
| FS open | 569/921/1004 | RUN `spawn_open` | no cancel |
| `take_or_open_fs` | 6150 | **SYNC** | runs a full FS open |
| Directory listing | 1812/4384 | **SYNC** | |
| Preview read and content detection | 1827 | SYNC | |
| Mac-archive sniff | 841/2059 | **SYNC in render** | |
| Extract | 2425 | RAW | bytes + cancel; no panic guard |
| Export selection | 1638 | **SYNC** | |
| Tar export / import | 2904 / 3166 | RAW / **SYNC** | |
| Apply staged edits | 4247 | **SYNC** | |
| CHD edit mode / flatten | 2701 / 2789 | **SYNC** / RAW | |
| Archive edit | 2521/2547 | RUN | no panic guard |
| Open archive / extract modal / export as archive | 3939/3984/4049 | **SYNC** | reads the whole file |
| SquashFS `plan_size` | 3025 | **SYNC** | |
| Text editor load/save, fsck, repair | various | **SYNC** | |
| Tree text | 5812 | RAW | |

### Commander

- **GUI, off the UI thread:**
  - open, wrapper open, preflight
  - apply and remote apply (no cancel)
  - stage copy, host copy, export
  - checksums (no cancel)
  - remote connect, open, and close image
  - None of the `commander_ops` spawns has a panic guard.
- **GUI, SYNC:**
  - `load_source`, `load_archive`, `open_optical_choice`, `resolve_backup`
  - host `read_dir`
  - entering directories (over the network for remote panes)
  - `detail_payload` read
  - wrapper-tree stage copy and `export_now`
- **TUI:** almost everything is SYNC. Only checksum and connect are async. Host-to-host copy busy-waits (tui:2671).

### Optical / Archives / Misc

| Operation | GUI | TUI | Start | Notes |
|---|---|---|---|---|
| Drive enumeration | optical_tab:163, mod:621 | tui:5381 | **SYNC** | includes remote daemon round-trips |
| Disc detect | optical_tab:824 | - | SYNC | |
| Optical browse open/list/read/export/tree | `src/optical/browse_view.rs` | - | **SYNC** | extract is RAW (:978) |
| Rip | optical_tab:889 | tui:7160 | RAW | the GUI path has no panic guard |
| Rip to CHD / convert | optical_tab:952/984 | - | RAW | bytes + cancel |
| Archive open / list | archives_tab:107 | tui:7337 | **SYNC** | decodes the whole file |
| Archive extract | archives_tab:165 | tui:7393 (SYNC) | RAW | no cancel |
| Bulk convert scan / run | bulk_convert_dialog:291, mod:1127 | tui:6918/6959 | SYNC / RUN | |
| Device enumeration | mod:589/1073 | tui:3792/5359 | SYNC | |
| Update check / download | mod:630/815 | tui:5446 (SYNC, network) | RAW / RUN | |
| Remote browser | remote_browser:126/421 | - | RAW | `listing.enter` is SYNC over the network |
| TUI explorer (open, tree, preview, export, edit, import, metadata, bless, mkdir, delete, new disk) | - | tui | **SYNC** | |
