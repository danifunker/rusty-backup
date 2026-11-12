# Capabilities by shell

What each front-end can do, as the tier 8 flows prove it. One row per capability a flow tags
(`capabilities = [...]` in `regression-tests/cases/tier8/*.toml`), one column per shell the flows
drive, and the flows behind each row.

- **yes**: a flow with that capability runs on that shell and passes (expected failures are listed
  in `regression-tests/data/known-failures.toml`).
- **-**: no flow covers it on that shell. That is not the same as missing: see § Gaps.

The table is generated from the manifests and checked in CI by `scripts/check-capabilities.py`
(`--write` regenerates it). A new row is a new capability tag on a flow, and needs the user's OK
(docs/native_frontends_plan.md, Phase 2b). egui has no column: no driver reaches it, and it is the
reference the shells are measured against. Per-feature limits by build target live in
[platform_matrix.md](platform_matrix.md).

<!-- capabilities:begin -->
| Capability | TUI | GTK | Win32 | AppKit | Proved by |
|---|---|---|---|---|---|
| `app.start` | - | - | - | yes | `flow.shell.appkit-start` |
| `archives.extract` | - | yes | yes | - | `flow.archives.sit-extract-all` |
| `archives.open` | - | yes | yes | - | `flow.archives.sit-extract-all` |
| `backup.format` | yes | yes | yes | yes | `flow.backup.image-vhd`, `flow.backup.image-zstd` |
| `backup.remote-source` | - | yes | yes | - | `flow.network.backup-remote-connect` |
| `backup.run` | yes | yes | yes | yes | `flow.backup.image-vhd`, `flow.backup.image-zstd` |
| `backup.source-image` | yes | yes | yes | yes | `flow.backup.image-vhd`, `flow.backup.image-zstd` |
| `browse.edit` | - | yes | yes | yes | `flow.browse.inspect-edit-text`, `flow.browse.inspect-new-folder` |
| `browse.edit-text` | - | yes | yes | yes | `flow.browse.inspect-edit-text` |
| `browse.extract` | yes | yes | yes | yes | `flow.browse.inspect-extract-all`, `flow.optical.chd-browse-extract`, `flow.optical.iso-browse-extract` |
| `browse.new-folder` | - | yes | yes | yes | `flow.browse.inspect-new-folder` |
| `build.cd-efs` | yes | yes | yes | yes | `flow.build.efs-cd` |
| `build.cd-from-folder` | yes | yes | yes | yes | `flow.build.efs-cd` |
| `build.format` | - | yes | yes | yes | `flow.build.mbr-fat`, `flow.build.mbr-ntfs12` |
| `build.open-inspect` | - | yes | yes | yes | `flow.build.mbr-ntfs12` |
| `build.partitioned` | - | yes | yes | yes | `flow.build.mbr-fat`, `flow.build.mbr-ntfs12` |
| `commander.apply` | yes | yes | yes | yes | `flow.commander.zip-to-image`, `flow.files.extract`, `flow.files.inject` |
| `commander.archive-browse` | yes | yes | yes | yes | `flow.commander.open-zip`, `flow.commander.zip-to-image` |
| `commander.copy` | yes | yes | yes | yes | `flow.commander.zip-to-image`, `flow.files.extract`, `flow.files.inject` |
| `commander.host-folder` | yes | yes | yes | yes | `flow.commander.zip-to-image`, `flow.files.extract`, `flow.files.inject` |
| `convert.disc` | - | yes | yes | - | `flow.convert.iso-to-chd` |
| `inspect.browse` | - | yes | yes | yes | `flow.browse.inspect-edit-text`, `flow.browse.inspect-extract-all`, `flow.browse.inspect-new-folder` |
| `inspect.expand-hfs` | yes | yes | yes | - | `flow.inspect.hfs-expand`, `flow.inspect.hfs-shrink` |
| `inspect.open-backup` | - | yes | yes | yes | `flow.restore.share-with-inspect` |
| `inspect.open-image` | yes | yes | yes | yes | `flow.browse.inspect-edit-text`, `flow.browse.inspect-extract-all`, `flow.browse.inspect-new-folder`, `flow.inspect.hfs-expand`, `flow.inspect.hfs-shrink` |
| `inspect.remote-backup` | yes | yes | yes | - | `flow.network.remote-inspect-backup` |
| `inspect.remote-open` | yes | yes | yes | - | `flow.network.remote-inspect-backup` |
| `optical.browse_contents` | yes | yes | yes | - | `flow.optical.chd-browse-extract`, `flow.optical.iso-browse-extract` |
| `optical.cdda-play` | - | yes | yes | - | `flow.audio.cdda-play` |
| `optical.chd` | yes | yes | yes | - | `flow.convert.iso-to-chd`, `flow.optical.chd-browse-extract` |
| `optical.open-image` | yes | yes | yes | - | `flow.optical.chd-browse-extract`, `flow.optical.iso-browse-extract` |
| `optical.remote-drive` | - | yes | yes | - | `flow.network.optical-add-remote` |
| `restore.backup-folder` | yes | yes | yes | yes | `flow.restore.folder-to-image`, `flow.restore.share-with-inspect` |
| `restore.remote-target` | - | yes | yes | - | `flow.network.restore-to-remote-image` |
| `restore.run` | yes | yes | yes | yes | `flow.network.restore-to-remote-image`, `flow.restore.folder-to-image` |
| `restore.target-image` | yes | yes | yes | yes | `flow.restore.folder-to-image` |
| `restore.write-image` | - | yes | yes | - | `flow.disk_write.image-file` |
| `settings.update-check` | - | yes | yes | - | `flow.settings.save-and-reopen` |
<!-- capabilities:end -->

## Gaps

Every GTK and Win32 cell but `app.start` is **yes** (Phase 5's exit criterion for Win32; the 13 flows written with
the Win32 shell were extended to GTK on 2026-11-10 and run under WSL, regression-tests/FLOWS.md).
The other columns' dashes, by reason:

- **By design**
  - TUI, `optical.cdda-play`: the TUI has no CD-DA player; audio needs a desktop audio stack
    (platform_matrix.md, Optical row).
- **Proved by every flow**
  - `app.start` on TUI, GTK and Win32: every flow those shells run starts them first. Only the
    AppKit skeleton's stand-in flow (`flow.shell.appkit-start`) carries the tag, until its first
    screen has flows; the stand-in goes then.
- **Under way**
  - AppKit: slice 0 (the skeleton) is in (Phase 3); its column is its checklist.
- **No TUI flow yet**: the TUI has these screens (platform_matrix.md's feature rows) but the flows
  list only `gtk` and `win32`: Archives, the Inspect browse view and its edits, Build Disk's
  partitioned path, Convert, the disk writer, Restore's hand-off to Inspect, the remote Backup /
  Restore / Optical windows and Settings.
- **GTK's own suite**: `scripts/gtk4-gui-suite.py` (AT-SPI, run in the `build-gtk4` CI job) is
  not a tier 8 flow, so this table does not count it.
