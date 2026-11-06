# Filesystem limits and caveats

Restore, Build Disk and `new volume` all warn about the same things, because they all read one
table: `src/model/fs_limits.rs`. A new fact goes there once and reaches every front end.

## How it works

- `VolumeFacts` says what a volume is. Restore reads it from the first 4 KB of the partition in the
  backup, so it sees the real filesystem and not a guess from the partition type. A blank format
  gets it from `FsKind` (`VolumeFacts::of_kind`).
- `caveats(facts, size)` lists what applies at that size, worst first.
- `CaveatLevel` is `Info`, `Warning` or `Blocked`. `Blocked` means the volume cannot exist at that
  size, so Restore's Start stays disabled and `rb-cli restore` stops before touching the target.
- Restore shows them above the Restore button, in the TUI form, and on stderr from `rb-cli
  restore`. Build Disk shows them on Review. `new volume` prints them as notes.

## Adding a fact

A fact that applies from some size up is one row in `RULES`. A new filesystem also needs a
`FsFamily` variant and a line in `VolumeFacts::from_head` and `of_kind`. Add only what a source or
an emulator confirms: these strings tell people what their machine can do.

## What the table says today

| Filesystem | From | Level | Says |
|---|---|---|---|
| HFS | 2 GB | Info | Needs System 7.5 or later |
| HFS | past its block limit | Info | Rebuilt with bigger blocks, needs temporary space |
| HFS | past 4063 MB | Blocked | Classic HFS stops there |
| HFS+ | any size | Info | Needs Mac OS 8.1 or later; a 68k Mac cannot start up from it |
| FAT12, FAT16 | past the cluster limit | Warning | The volume stops there; the rest of the partition is unused |
| FAT16 | over 32 MB | Info | Needs MS-DOS 3.31 or later |
| FAT16 | over 2 GB | Warning | 64 KB clusters: Windows NT only |
| FAT32 | any size | Info | Needs Windows 95 OSR2 or later; not MS-DOS 6.22 or Windows NT 4 |
| exFAT | any size | Info | Needs Vista SP1 (XP with KB955704), Mac OS X 10.6.5 or Linux 5.4 |
| exFAT | past its FAT | Warning | The volume stops there |
| NTFS | over 7.8 GB | Info | Windows NT 4 only starts from the first 7.8 GB of its system partition |
| ProDOS | over 32 MB | Warning | The volume stops there |
| AFFS `DOS\2`, `DOS\3` | any size | Info | Needs Kickstart 2.0 or later |

Left out because sources disagree: the Kickstart version for the directory-cache (`DOS\4`, `DOS\5`)
and long-name (`DOS\6`, `DOS\7`) modes. Sources also differ on whether 4 GB HFS arrived in System 7.5
or 7.5.2; a full-size volume is verified on 7.5.5 (`docs/evidence/`).

Not covered: a Clonezilla backup has no caveats, because its partition data is not read at load
time. A partitionless backup always restores at its original size, so only version notes apply.

## HFS rebuild

HFS has 65535 allocation blocks and fixes its block size at format time, so a volume can outgrow its
own blocks. `src/fs/hfs_rebuild.rs` clones it into a temp file at a bigger block size, checks it, and
only then overwrites the old one. Build Disk and all four Restore paths (per-partition, single-file
CHD, Clonezilla, single partition) share it.

A bootable System 7.5.5 disk restored from 70 MiB to 600 MiB this way starts up to the desktop on a
Quadra 800 under QEMU (`docs/evidence/restore-rebuilt-system755-quadra.png`). That run had its Mac
driver copied back by hand, because a zstd or raw backup does not keep the driver partition.

The temp file sits beside an image target, or in the system temp folder for a device; it holds about
as much as the volume's files. Point `TMPDIR` at a larger drive if that folder is short of room, and
note that on Linux `/tmp` is often held in RAM.

VHD export and Inspect's Resize do not rebuild: they stop with the 65535-block message, and Expand
HFS covers them.
