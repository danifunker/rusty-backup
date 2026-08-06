# `rb-cli` cookbook

Longform, end-to-end recipes for `rb-cli`. Each one walks through a
real workflow: explaining intent, the choice of flags, and what to
check at each step. For copy-paste snippets see
[`cli-examples.md`](cli-examples.md); for the full flag matrix see
[`cli-reference.md`](cli-reference.md).

Recipes assume you've installed shell completions
(`rb-cli install-completions`) and dropped a config file via
`rb-cli config init`. Sample paths use macOS conventions (`/dev/diskN`,
`/Volumes/…`); Linux uses `/dev/sdX` / `/dev/sr0`, Windows uses
`\\.\PhysicalDriveN` / `\\.\E:`.

## Contents

1. [Back up a vintage Mac SCSI disk, then restore it later](#1-back-up-a-vintage-mac-scsi-disk-then-restore-it-later)
2. [Expand an HFS volume from a 32 MB SCSI drive into a 2 GB image](#2-expand-an-hfs-volume-from-a-32-mb-scsi-drive-into-a-2-gb-image)
3. [Shrink an IRIX disk by re-encoding to CHD](#3-shrink-an-irix-disk-by-re-encoding-to-chd)
4. [Build a custom HFS image from a host directory](#4-build-a-custom-hfs-image-from-a-host-directory)
5. [Build an IRIX software CD from a folder of tardists](#5-build-an-irix-software-cd-from-a-folder-of-tardists)
6. [Rip and archive a CD-ROM library to CHD](#6-rip-and-archive-a-cd-rom-library-to-chd)
7. [Drive complex flows from a single batch script](#7-drive-complex-flows-from-a-single-batch-script)
8. [Pick the right partition every time](#8-pick-the-right-partition-every-time)

---

## 1. Back up a vintage Mac SCSI disk, then restore it later

**Goal.** Take a backup of a 4 GB SCSI disk attached over a USB-SCSI
bridge (so it appears as `/dev/disk6`), produce a single-file CHD with
SHA-256 checksums, and prove the restore works against a fresh image.

```bash
# 1. Confirm the disk is there and not the system disk.
rb-cli show devices --format text
# Look for the row tagged "removable"; note its path (e.g. /dev/disk6).

# 2. Make the backup. Single-file CHD with SHA-256 is the canonical shape.
rb-cli backup /dev/disk6 ~/MacBackups/quadra-system7 \
    --name quadra-system7-$(date +%Y%m%d) \
    --format chd --checksum sha256
```

The backup folder ends up at
`~/MacBackups/quadra-system7/quadra-system7-YYYYMMDD/` and contains
`metadata.json`, `mbr.json` (or `apm.json` for Mac disks), and a
single `<name>.chd` with the disk image inside. Verify the checksum
matched by checking `metadata.json["partitions"][N]["checksum_ok"]`:

```bash
jq '.partitions[].checksum_ok' ~/MacBackups/quadra-system7/quadra-system7-*/metadata.json
# Every line should print: true
```

**Restoring** to a fresh image (don't write back to the original disk
until you've verified):

```bash
rb-cli restore ~/MacBackups/quadra-system7/quadra-system7-* \
    ~/MacBackups/restored.hda
```

To restore to a physical replacement disk later:

```bash
rb-cli show devices                       # confirm the right /dev/diskN
rb-cli restore ~/MacBackups/quadra-system7/quadra-system7-* \
    /dev/disk7 --device --yes
```

`--device` switches in the sector-aligned writer + safety preflight;
`--yes` is required for any device target. The preflight will refuse
the system disk and any device with mounted partitions. Add
`--write-to-system-disk` to override — useful only on disposable
machines.

## 2. Expand an HFS volume from a 32 MB SCSI drive into a 2 GB image

**Goal.** Take a backup of a tiny classic-HFS volume and re-floor it
into a 2 GB volume so System 7 / NetBSD / MAME can use the extra
space. Classic HFS is capped at 65535 allocation blocks; growing past
~ 2 GB requires bumping the block size, which means re-laying-out the
catalog — the GUI's "Expand HFS Volume…" dialog and the new
`rb-cli expand` verb both wrap that flow.

```bash
# 1. Identify the source partition. APM disks usually have one Apple_HFS.
rb-cli inspect ~/MacBackups/quadra-system7-restored.hda
# Read the row you want: `#` is the number for @N, `slot` the one for @sN.
# Suppose the Apple_HFS row shows `#` 2. See recipe 8 if you're scripting.

# 2. Expand it. The verb picks the smallest block size whose 65535-block
#    ceiling holds --size; specify --block-size yourself only to force a
#    larger value (for example to share block size across a multi-volume
#    disk set).
rb-cli expand ~/MacBackups/quadra-system7-restored.hda@2 \
    --size 2G \
    --output ~/MacBackups/quadra-system7-2gb.hda
```

The verb logs each phase: building the blank target, walking the
source catalog, cloning files, running the post-clone fsck, and
finally wrapping the result in a fresh APM disk image with the
source's drivers + driver-descriptor metadata preserved. Mount the
output in MAME or a Mac to confirm it boots.

If the cloned volume fails post-fsck with a new error code that the
source didn't have, the verb aborts before writing the APM. Check
`docs/hfs_btree_capacity.md` and rerun with `--block-size` one tier
larger — that usually means the catalog needed more headroom than
the auto-sizer allotted.

### Producing a BasiliskII HFV instead of an APM disk

BasiliskII / SheepShaver mount a **flat HFV** — a bare classic-HFS
volume with no partition table — rather than an APM/SCSI disk. Add
`--to-hfv` to the same `expand` verb to write one. HFV volumes are
classic-HFS-only and capped at 2047 MB (the 2 GiB signed-32-bit
boundary classic Mac OS won't cross), so `--size` must stay under that.

```bash
# Convert (and optionally resize) an Apple_HFS partition into a .hfv:
rb-cli expand ~/MacBackups/quadra-system7-restored.hda@2 \
    --size 500M --to-hfv \
    --output ~/Basilisk/system7.hfv

# Re-floor an existing .hfv to a bigger block size / size:
rb-cli expand ~/Basilisk/old.hfv@1 --size 1G --to-hfv \
    --output ~/Basilisk/old-1gb.hfv
```

To create a *blank* HFV from scratch, use `new volume hfv`:

```bash
rb-cli new volume hfv ~/Basilisk/scratch.hfv --size 100M --name "Mac HD"
```

A backup of an `.hfv` restores straight back to a byte-identical `.hfv`
(`rb-cli backup disk.hfv bk/ && rb-cli restore bk/<stamp> out.hfv`) —
the volume has no partition table, so restore writes the bare volume at
sector 0.

## 3. Shrink an IRIX disk by re-encoding to CHD

**Goal.** An SGI disk dump from a 9 GB U160 SCA drive is mostly empty
trailing zeros. Compress it to CHD and watch the size collapse:

```bash
# 1. Confirm the SGI volume header (svh) is at sector 0.
rb-cli inspect /tank/irix-octane.img

# 2. Re-encode. shrink keeps the SGI layout, drops trailing zero hunks.
rb-cli shrink /tank/irix-octane.img --output /tank/irix-octane.chd
```

The shrink verb is SGI-aware: it understands the SGI partition table
("volume header"), keeps the partition table intact at sector 0, and
writes a single-file CHD that `chdman info` recognises. Real-world
ratios on lightly-used IRIX disks are 5×–20× depending on how much
of the disk is actually written.

To go back from CHD to a flat image (for `mount -t efs` on Linux, or
to feed an emulator that wants raw):

```bash
rb-cli convert /tank/irix-octane.chd /tank/restored/ --format raw \
    --extension img
# Output: /tank/restored/irix-octane.img
```

## 4. Build a custom HFS image from a host directory

**Goal.** You've got a folder full of Mac apps and resource forks
sitting in a host filesystem (or AppleDouble sidecars from `cp` on
macOS). Assemble them into an HFS boot disk in one shot.

> **Just need the files in?** `rb-cli import boot.dsk ./contents /System`
> copies a whole tree in one command. Use the `batch` flow below when you
> want per-file **type/creator** codes inferred from extensions — that
> inference is what `batch-template` adds, and `import` does not do it.
> On a non-Mac filesystem, reach for `import` first.

The flow is: `batch-template` to generate a starter script,
hand-edit if needed, then `batch` to apply.

```bash
# 1. Create the blank target volume (no GUI required).
rb-cli new volume hfs ~/Builds/boot.dsk --size 80M --name "Boot Disk"

# 2. Generate a script that mirrors a host folder into the target.
rb-cli batch-template ~/Builds/contents \
    --target ~/Builds/boot.dsk \
    --dst /System \
    --exclude '*.tmp' --exclude '.DS_Store' \
    --out ~/Builds/populate.json

# 3. Review the JSON.
jq '.operations[0:5]' ~/Builds/populate.json
# Each entry is { "op": "mkdir", ... } or
# { "op": "put", "src": ..., "dst": ..., "type": "...", "creator": "..." }.

# 4. Dry-run.
rb-cli batch ~/Builds/populate.json --dry-run

# 5. Apply for real.
rb-cli batch ~/Builds/populate.json
```

The template's built-in extension table handles common cases
(`.txt` → `TEXT`/`ttxt`, `.gif` → `GIFf`/`ogle`, etc.); anything not
listed gets `BINA`/`????`. Edit the JSON to fix any wrong inferences
before running `batch`. The batch worker collects every op,
preflights paths against the live FS state, then applies all of them
under one `sync_metadata` at the end — so partial failures don't leave
the volume in a half-written state.

**Want a CD-ROM instead of a bare volume?** Swap step 1 for
`rb-cli optical new mac-hfs ~/Builds/disc.iso --size 600M --name "Boot
Disk"` (or `mac-hfsplus` for Mac OS 8.1 and later) and address the volume
as `disc.iso@1` from there on. That wraps the same HFS volume in an Apple
Partition Map, which is what a Mac CD-ROM actually is; `--from-dir` fills
it in the same command if you don't need per-file type/creator inference.
No ISO 9660 side is written, so the disc is Mac-only.

**A folder of `.sit` downloads?** Add `--expand-archives` (to `optical new`
or to `import`) and they are unstuffed onto the volume rather than copied
in packed — both forks and each file's Finder type/creator carried across,
so the target Mac can run them without a copy of StuffIt Expander. With
`--size auto` the disc is sized to what the archives *unpack* to.

## 5. Build an IRIX software CD from a folder of tardists

**Goal.** You've collected `.tardist` packages for an SGI running IRIX
5.3 and want a disc the machine can mount and `inst` can read.

An IRIX-mountable disc isn't ISO 9660 — it's an SGI volume header with
the EFS filesystem in **slot 7 typed SYSV**, with CD geometry (1 head ×
32 sectors). `optical new sgi-efs` writes exactly that shape, and
`--from-dir` fills it in the same command.

```bash
# 1. See what you're working with.
du -sh ~/sgi-stuff-5-3

# 2. Format and fill in one step.
rb-cli optical new sgi-efs ~/irix53.iso \
    --size 600M --name IRIX53 \
    --bytes-per-inode 32768 \
    --from-dir ~/sgi-stuff-5-3

# 3. Verify before burning.
rb-cli ls   ~/irix53.iso@1 /
rb-cli fsck ~/irix53.iso@1
```

**On `--bytes-per-inode`.** The default density is ~1 inode per 4 KiB,
which on a 600 MB disc spends ~20 MB on inode tables. Real IRIX CDs are
far sparser. With a few dozen large packages, `32768` reclaims most of
that. It only affects metadata, never your data.

**Archives: leave them, or unpack them?** This is the real decision, and
it depends on what the disc is for.

*Leave them* (the default) when the target machine's own installer
consumes the archive. IRIX `inst` reads a `.tardist` as-is, so unpacking
would actually get in the way. The summary tells you archives were found
so you know the option exists.

*Unpack them* with `--expand-archives` when you want the contents
browsable. Each archive lands in a directory named after it. Add
`--flatten-folders` to merge every archive into **one** root instead —
which is what `inst` wants from a distribution directory, since you point
it at a single directory holding all the product images rather than
re-pointing it once per package:

```bash
rb-cli optical new sgi-efs ~/irix53.iso \
    --size auto --name IRIX53 \
    --from-dir ~/sgi-stuff-5-3 \
    --expand-archives --flatten-folders
```

`--size auto` measures the folder and sizes the disc to it — and with
`--expand-archives` it measures what the archives *become*, not their
compressed size, because a tree of `.tar.gz` can easily triple on the way
in and a disc sized off the packed total would run out mid-copy.

Flattening makes overlapping entries normal rather than exceptional: SGI
freeware tardists all ship the same shared `fw_common*` product, and
source tarballs commonly both root at `usr/`. So `--flatten-folders`
skips entries that already exist and reports the count; pass `--force` to
overwrite instead.

Mount it on the SGI with:

```
mount -t efs -o ro /dev/dsk/dks0d<N>s7 /CDROM
```

The same flags work on `new hd sgi-efs` for a dvh-wrapped IRIX hard disk,
and the TUI's new-image wizard exposes the CD-ROM class with an optional
source folder if you'd rather not type flags.

## 6. Rip and archive a CD-ROM library to CHD

**Goal.** A drawer of vintage CD-ROMs needs to be ripped to a stable
archival format. CHD with the CD profile is what MAME and DuckStation
both consume natively.

```bash
# Per disc: rip raw, then re-encode to CHD.
rb-cli optical rip --device /dev/disk5 \
    --output ~/CDRips/$(date +%Y%m%d)-disc1.cue \
    --format bincue --eject

rb-cli optical convert \
    ~/CDRips/20260519-disc1.cue \
    ~/CDArchive/disc1.chd \
    --format chd
```

To process a whole folder of already-ripped BIN/CUE pairs:

```bash
rb-cli convert ~/CDRips ~/CDArchive --format chd-cd
```

For an HFS-formatted CD (Apple software, old shareware compilations),
extract the file tree to the host for inspection:

```bash
rb-cli optical browse  ~/CDRips/hfs-installer.iso
rb-cli optical extract ~/CDRips/hfs-installer.iso \
    --to ~/HFSCDs/Installer \
    --resource-forks appledouble
```

The browse output is a tree-style listing showing sizes plus
HFS type/creator codes; extract handles the resource-fork plumbing in
whichever shape your downstream consumer wants (AppleDouble sidecars,
MacBinary single-file, native `..namedfork/rsrc` on a Mac, etc.).

## 7. Drive complex flows from a single batch script

**Goal.** You've got a CI job that needs to populate a fresh image with
a Finder, a couple of extensions, and a few launch documents — and the
inputs come from elsewhere in the pipeline. Hand-write the script and
check it into git.

```json
{
  "schema": "rb-cli-batch/1",
  "target": "out/boot.dsk@1",
  "default_options": { "force": true, "creator": "MACS", "type": "FNDR" },
  "operations": [
    { "op": "mkdir", "path": "/System" },
    { "op": "mkdir", "path": "/System/Extensions" },
    { "op": "put", "src": "build/Finder",    "dst": "/System/Finder" },
    { "op": "put", "src": "build/StartupExt","dst": "/System/Extensions/StartupExt" },
    { "op": "put", "src": "/dev/null", "dst": "/Welcome.txt",
                   "type": "TEXT", "creator": "ttxt", "zero": 1024 },
    { "op": "rm", "path": "/Trash", "recursive": true }
  ]
}
```

```bash
# Build the target image first, then apply.
rb-cli new volume hfs out/boot.dsk --size 4M --name "CI Build"
rb-cli batch ci-populate.json --continue-on-error
```

`--continue-on-error` keeps the batch going past non-fatal failures
(a missing file, a duplicate filename without `force: true`) and
prints a summary at the end. Drop the flag for production builds where
any failure should abort the run. Combined with `rb-cli fsck` as the
last CI step, you get reliable, reviewable, idempotent image builds.

---

## Where things live

- **Engine code** (filesystem readers, partition parsers, backup
  orchestrator, optical workers): `src/{fs,partition,backup,optical,rbformats}`.
- **GUI–CLI shared workers**: `src/model/*_runner.rs`. Both surfaces
  push state into `model::status::*` structs and drain them with their
  own progress pumps.
- **CLI verbs**: `src/cli/verbs/`. Each verb is intentionally thin —
  if a verb is doing more than parsing args + calling into the engine,
  the engine probably needs another helper.

When adding a new feature, follow the pattern: build the engine
function in `src/`, expose it through a `model::*_runner` if it's
long-running, then write a thin wrapper in `src/cli/verbs/`. The GUI
follows the same path through `src/gui/`.

---

## 8. Pick the right partition every time

**Goal.** Aim a verb at one partition of a multi-partition disk and be
sure you hit the one you meant — interactively, and in a script that has
to keep working.

`rb-cli inspect` prints both numbers you can type:

```
$ rb-cli inspect ~/MacBackups/os9-120gb.img
Partition table: APM

idx  slot  type                         start_lba            size  flags
  1     6  Apple_HFS (untitled)              1216        39.1 GiB  boot
  2     7  Apple_HFS (untitled 2)        81921216        72.7 GiB  boot
```

- **`@N`** takes the `idx` column — the position in that list. Portable
  across every partition table.
- **`@sN`** takes the `slot` column — the table's own numbering, spelled
  the way the platform spells it. Here `@s6` is the partition macOS calls
  `disk4s6`; on an SGI disk `@s0` is the slice IRIX calls 0.

```bash
rb-cli ls        ~/MacBackups/os9-120gb.img@1  /
rb-cli ls        ~/MacBackups/os9-120gb.img@s6 /     # the same partition
rb-cli show fs-info ~/MacBackups/os9-120gb.img@s7
```

**In scripts, prefer `@sN`.** The position depends on which partitions
rusty-backup currently considers browsable, and that set grows as the tool
learns about more partition types — a disk whose driver partitions were once
listed will renumber when they stop being. The slot names the table itself and
does not move.

Amiga RDB disks can be addressed by device name, which is what an Amiga user
actually knows:

```bash
rb-cli ls ~/amiga/workbench.hdf@DH0 /
rb-cli ls ~/amiga/workbench.hdf@dh1 /       # case-insensitive
```

The same forms work on the flags, so a filename containing `@` is never a
problem:

```bash
rb-cli write install.img /dev/disk6 --partition s7 --yes
rb-cli backup /dev/disk6 ~/backups --partitions s6,s7
```

GPT disks have no slot form — the parser drops unused entries, so a truthful
entry number can't be recovered. `@sN` says so and points at `@N`.

Full details, including how each table numbers its slots:
[docs/partition-selectors.md](partition-selectors.md).
