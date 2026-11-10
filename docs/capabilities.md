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
| `archives.extract` | - | - | yes | - | `flow.archives.sit-extract-all` |
| `archives.open` | - | - | yes | - | `flow.archives.sit-extract-all` |
| `backup.format` | yes | yes | yes | - | `flow.backup.image-vhd`, `flow.backup.image-zstd` |
| `backup.remote-source` | - | - | yes | - | `flow.network.backup-remote-connect` |
| `backup.run` | yes | yes | yes | - | `flow.backup.image-vhd`, `flow.backup.image-zstd` |
| `backup.source-image` | yes | yes | yes | - | `flow.backup.image-vhd`, `flow.backup.image-zstd` |
| `browse.edit` | - | - | yes | - | `flow.browse.inspect-edit-text`, `flow.browse.inspect-new-folder` |
| `browse.edit-text` | - | - | yes | - | `flow.browse.inspect-edit-text` |
| `browse.extract` | yes | yes | yes | - | `flow.browse.inspect-extract-all`, `flow.optical.chd-browse-extract`, `flow.optical.iso-browse-extract` |
| `browse.new-folder` | - | - | yes | - | `flow.browse.inspect-new-folder` |
| `build.cd-efs` | yes | yes | yes | - | `flow.build.efs-cd` |
| `build.cd-from-folder` | yes | yes | yes | - | `flow.build.efs-cd` |
| `build.format` | - | - | yes | - | `flow.build.mbr-fat` |
| `build.partitioned` | - | - | yes | - | `flow.build.mbr-fat` |
| `commander.apply` | yes | yes | yes | - | `flow.commander.zip-to-image`, `flow.files.extract`, `flow.files.inject` |
| `commander.archive-browse` | yes | yes | yes | - | `flow.commander.open-zip`, `flow.commander.zip-to-image` |
| `commander.copy` | yes | yes | yes | - | `flow.commander.zip-to-image`, `flow.files.extract`, `flow.files.inject` |
| `commander.host-folder` | yes | yes | yes | - | `flow.commander.zip-to-image`, `flow.files.extract`, `flow.files.inject` |
| `convert.disc` | - | - | yes | - | `flow.convert.iso-to-chd` |
| `inspect.browse` | - | - | yes | - | `flow.browse.inspect-edit-text`, `flow.browse.inspect-extract-all`, `flow.browse.inspect-new-folder` |
| `inspect.expand-hfs` | yes | yes | yes | - | `flow.inspect.hfs-expand`, `flow.inspect.hfs-shrink` |
| `inspect.open-backup` | - | - | yes | - | `flow.restore.share-with-inspect` |
| `inspect.open-image` | yes | yes | yes | - | `flow.browse.inspect-edit-text`, `flow.browse.inspect-extract-all`, `flow.browse.inspect-new-folder`, `flow.inspect.hfs-expand`, `flow.inspect.hfs-shrink` |
| `inspect.remote-backup` | yes | yes | yes | - | `flow.network.remote-inspect-backup` |
| `inspect.remote-open` | yes | yes | yes | - | `flow.network.remote-inspect-backup` |
| `optical.browse_contents` | yes | yes | yes | - | `flow.optical.chd-browse-extract`, `flow.optical.iso-browse-extract` |
| `optical.cdda-play` | - | yes | yes | - | `flow.audio.cdda-play` |
| `optical.chd` | yes | yes | yes | - | `flow.convert.iso-to-chd`, `flow.optical.chd-browse-extract` |
| `optical.open-image` | yes | yes | yes | - | `flow.optical.chd-browse-extract`, `flow.optical.iso-browse-extract` |
| `optical.remote-drive` | - | - | yes | - | `flow.network.optical-add-remote` |
| `restore.backup-folder` | yes | yes | yes | - | `flow.restore.folder-to-image`, `flow.restore.share-with-inspect` |
| `restore.remote-target` | - | - | yes | - | `flow.network.restore-to-remote-image` |
| `restore.run` | yes | yes | yes | - | `flow.network.restore-to-remote-image`, `flow.restore.folder-to-image` |
| `restore.target-image` | yes | yes | yes | - | `flow.restore.folder-to-image` |
| `restore.write-image` | - | - | yes | - | `flow.disk_write.image-file` |
| `settings.update-check` | - | - | yes | - | `flow.settings.save-and-reopen` |
<!-- capabilities:end -->

## Gaps

Every Win32 cell is **yes** (Phase 5's exit criterion). The other columns' dashes, by reason:

- **By design**
  - TUI, `optical.cdda-play`: the TUI has no CD-DA player; audio needs a desktop audio stack
    (platform_matrix.md, Optical row).
- **Not started**
  - AppKit: the shell does not exist yet (Phase 3); its column is its checklist.
- **No flow on that shell yet**: the flow was written with the Win32 shell and lists only
  `win32`. Whether GTK or the TUI has the screen is in platform_matrix.md's feature rows; where
  it does, the flow needs extending, not the shell:
  - Archives: `archives.open`, `archives.extract`.
  - Backup and Restore from or to a remote daemon: `backup.remote-source`,
    `restore.remote-target`.
  - Inspect's browse view and editing: `inspect.browse`, `browse.edit`, `browse.edit-text`,
    `browse.new-folder`, and `inspect.open-backup` (a backup shared from Restore).
  - Build Disk's partitioned path: `build.partitioned`, `build.format`.
  - Convert: `convert.disc`.
  - The disk writer: `restore.write-image`.
  - Optical's Add remote daemon: `optical.remote-drive`.
  - Settings: `settings.update-check`.
