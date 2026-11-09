# Navigation: from tabs to "open a thing, then act on it"

Proposal, drafted 2026-11-09 on `win32-shell` while the Win32 shell had only its skeleton and a
first pass of Inspect. Nothing here is decided; the questions at the end are the user's. Under D8
whatever is decided applies to every shell, so the change belongs in the controllers.

## Why the tabs are in question

The seven tabs (`AppTab`: Backup, Restore, Build Disk, Inspect, Convert, Optical, Archives) mix
two kinds of page:

- **Thing pages**: Inspect (a disk), Optical (a disc), Archives (a Mac archive). You open
  something and look at it.
- **Task pages**: Backup, Restore, Convert, Build Disk. You pick a source *inside* the task.

The same object is opened in several places, each with its own picker, recent list and source
rules. The app then spends code carrying objects between tabs:

| Hand-off | Today |
|---|---|
| Inspect -> Convert, Optical -> Convert | "Convert..." sets a pending path the app drains |
| Inspect -> Optical, Inspect -> Archives | Inspect opens the file, finds it is a disc or an archive, sends it on |
| Archives -> Inspect | "Mount" sends the payload with its temp folder |
| Build Disk -> Inspect | "Open in Inspect" (egui, GTK; not the TUI) |
| Restore <-> Inspect | one backup folder shared through `share_backup` / `ShareStep`, closed from either side |
| Drops | routed by *which tab is showing* (`route_drop`), not by what was dropped |

Every one of these is a seam where the tabs split one piece of work in two. The tab lists
already differ between shells (the TUI has New Disk, Commander and Settings as tabs, in another
order), which D8 calls a bug.

## The proposal

**One window with a sidebar of things, and a page for the selected thing with its actions.**

```
+--------------------+-------------------------------------------------------------+
| OPEN               |  h.img  -  Raw disk image, 1.4 MiB, no partition table       |
|  > h.img        x  |  [Browse] [Check] [Back up...] [Convert...] [Write to disk...]|
|    backup-2026  x  |  [Resize...] [Edit table...] [Expand...] [Make bootable...]   |
|    MYDISC.chd   x  |  ---------------------------------------------------------   |
|                    |  Layout  [=========== HFS 1.4 MiB ===========]               |
| DEVICES            |   #  Type  Volume        Start   Size     Min                |
|    Disk 2 (SD 8GB) |   1  HFS   rusty-backup  0       1.4 MiB  27.5 KiB           |
|    CD drive E:     |                                                              |
|    192.168.1.50    |                                                              |
|                    |                                                              |
| [Open...]  [New...]|                                                              |
| [Commander]        |                                                              |
+--------------------+-------------------------------------------------------------+
| log                                                                              |
+----------------------------------------------------------------------------------+
```

- **Open...** takes any file or folder. What it opens is decided **by content**, once, in the
  model: a disk image, a backup folder (rusty-backup or Clonezilla), a disc image, a Mac archive,
  a wrapped image (`.adz`, `.msa`, ...), or a plain folder. The pieces exist today
  (`detect_image_format_with_path`, `is_browsable_optical`, `classify`, the archive magic
  check); they are spread over Inspect, Convert and Archives and partly go by extension.
- **Devices** are listed in the sidebar, not in four tabs' pickers: physical disks, optical
  drives, and remote daemons' drives.
- **Each kind of thing has one page**, and its actions open the task as a dialog or a sub-page
  with that thing already chosen as the source:

| Thing | Page (today's code) | Actions (today's tab or window) |
|---|---|---|
| Disk image, physical disk, remote image | Inspect | Browse, Check, Back up... (Backup), Convert... (Convert), Write to disk... (Restore's Write Image), Export..., Resize..., Edit table..., Expand..., Make bootable... |
| Backup folder | Inspect's backup view | Restore... (Restore: full disk, one partition, new disk), Browse, Check |
| Disc image, optical drive | Optical | Browse, Play audio, CHD Info, Convert... (image), Rip... (drive) |
| Mac archive | Archives | Extract..., Open the disk image inside (today's Mount: it becomes another open thing) |
| Folder | a short page | Convert all images in it... (Convert's folder mode), Build a CD from it... |
| Nothing (New...) | Build Disk wizard | creates a thing, which then opens in the sidebar |

- **Commander** stays a separate mode (or window): it is a two-pane workspace, not one thing.
  A pane can open anything in the sidebar's list.
- **Drops** open the dropped thing (by content) and select it. A drop onto a page in edit mode
  (an open volume) still adds the file to that volume.

What goes away: the shared-backup plumbing (a backup folder is one thing with two actions), the
convert/optical/archive/inspect forwarding, tab-dependent drop routing, and the per-tab source
pickers and recent lists (one recent list of things).

What stays: every task controller (`BackupController`, `RestoreController`, `ConvertController`,
`BuildDiskController`, `OpticalController`'s rip, `ArchivesController`'s extract) keeps its
logic. Most already accept a source as an action (`OpenImage`, `OpenDropped`, `open`), so they
are opened with the thing preset instead of from their own picker. The one-disk-job lock stays.

## Controller changes

1. **`model`: one `classify_source(path) -> SourceKind`** (disk image, wrapped image, backup
   folder, disc, Mac archive, folder), by content first. Inspect, Convert, Archives and
   Commander's `open_image` use it instead of their own rules.
2. **`controller::app`: a workspace instead of `AppTab`**: the list of open things (each with
   its page's controller), the selected one, the device list, open / close / select actions,
   and `route_drop` by content. `AppTab`, `share_backup` and `ShareStep` go.
3. **Task controllers open with a source**: an `Open*` constructor or action per task where one
   is missing (Restore from a backup thing, Convert from one image or a folder, Rip from a drive).
   Their own source rows stay for the CLI and are hidden in the window shells.
4. **One recent list** of things (`RecentMode` per kind can stay inside it).

Each step is testable without a shell (`tests/ui_scenarios/`), and the tier 8 flows change only
in their first steps: `{ tab = "Inspect" }, { pick = ... }, { activate = "inspect.source.open_file" }`
becomes `{ pick = ... }, { activate = "app.open" }`.

## Per shell

- **Win32**: least sunk cost. Build the sidebar (a tree view) and pages now; its Inspect page
  and Expand HFS dialog carry over unchanged.
- **TUI**: the sidebar is a left column, as Commander's panes are; F-keys and the key map stay.
- **GTK**: libadwaita's `NavigationSplitView` is this layout.
- **AppKit**: a source list (`NSSplitViewController` with a sidebar) is this layout.
- **egui**: it retires once the native shells reach parity (D1, Phase 7). Moving it too is work
  thrown away; the proposal is to leave egui on tabs and make that the one stated D8 exception.

## Order

1. The user decides the questions below.
2. `classify_source` in the model, with tests; Inspect, Convert and Archives call it (no visible
   change).
3. The workspace in `controller::app` beside `AppTab`, with scenarios; tabs map onto it for the
   existing shells meanwhile.
4. Win32 builds on the workspace (sidebar + pages); its flows switch to `app.open`.
5. The TUI, then GTK, switch; `AppTab` and the hand-off code are removed.

## Questions for the user

1. **One thing at a time, or several open at once** in the sidebar? Several is closer to how
   people compare a backup with the disk it came from; one is simpler and is what Inspect does
   today.
2. **egui**: leave it on tabs until it retires (the stated exception), or move it too?
3. **Task as dialog or as page**: does Back up... open a dialog over the disk's page (as Expand
   HFS does today), or replace the page until it finishes?
4. **Commander**: a mode as today, or a second window?
5. **Devices**: listed in the sidebar always (and scanned at start), or only after
   "Show Physical Devices" (today's elevation gate)?
