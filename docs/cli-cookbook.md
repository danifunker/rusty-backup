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
5. [Rip and archive a CD-ROM library to CHD](#5-rip-and-archive-a-cd-rom-library-to-chd)
6. [Drive complex flows from a single batch script](#6-drive-complex-flows-from-a-single-batch-script)

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
# Note which partition is the Apple_HFS one. Suppose it's @2.

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

To create a *blank* HFV from scratch, use `new --fs hfv`:

```bash
rb-cli new ~/Basilisk/scratch.hfv --fs hfv --size 100M --name "Mac HD"
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

The flow is: `batch-template` to generate a starter script,
hand-edit if needed, then `batch` to apply.

```bash
# 1. Create the blank target volume (no GUI required).
rb-cli new ~/Builds/boot.dsk --fs hfs --size 80M --name "Boot Disk"

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

## 5. Rip and archive a CD-ROM library to CHD

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

## 6. Drive complex flows from a single batch script

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
rb-cli new out/boot.dsk --fs hfs --size 4M --name "CI Build"
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
