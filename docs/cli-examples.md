# `rb-cli` examples

Recipe-style snippets. Each block is self-contained and runs against the
tempdir / fixture images noted at the top. The canonical grammar lives in
`rb-cli --help` and the auto-generated `docs/cli-reference.md`.

Topics:

- [Inspect what's on a disk](#inspect-whats-on-a-disk)
- [Create + populate an HFS floppy](#create--populate-an-hfs-floppy)
- [Fill an image from a host folder](#fill-an-image-from-a-host-folder)
- [Build an IRIX EFS CD-ROM](#build-an-irix-efs-cd-rom)
- [Round-trip a real device through backup + restore](#round-trip-a-real-device-through-backup--restore)
- [Use globs to extract or remove many files](#use-globs-to-extract-or-remove-many-files)
- [Drive everything from a batch JSON script](#drive-everything-from-a-batch-json-script)
- [Structured output for scripted consumers](#structured-output-for-scripted-consumers)

## Inspect what's on a disk

```bash
# Detect partition table + per-partition type
rb-cli inspect disk.hda

# Filesystem volume info for partition 2
rb-cli show fs-info disk.hda@2

# CHD metadata (codec, hunk size, version)
rb-cli show chd-info disk.chd

# All host block devices, JSON shape for tooling
rb-cli show devices --format json | jq '.result[] | select(.removable)'
```

## Create + populate an HFS floppy

```bash
rb-cli new volume hfs disk.dsk --size 800K --name "Boot Disk"

# Copy a host file in with its 4-char type/creator codes
rb-cli put disk.dsk ./Finder /System/Finder --type FNDR --creator MACS

# Pre-allocate a zero-filled file (e.g. a results scratchpad)
rb-cli put disk.dsk --zero 4096 --dst /Results.jsonl --type TEXT --creator ttxt

# Make a directory and copy a file into it
rb-cli mkdir disk.dsk /System/Extensions
rb-cli put   disk.dsk ./Extensions/ATM /System/Extensions/ATM

# Stamp the boot blocks verbatim (HFS-only)
rb-cli put disk.dsk --boot ./bootblocks.bin
```

## Fill an image from a host folder

`put` copies one file; `import` copies a whole tree in one pass, with no
tarball to stage first.

```bash
# The folder's *contents* land at the destination (default `/`).
rb-cli import disk.hda ./stuff

# ...or under an existing subdirectory. Like `untar`, the destination
# must already exist -- `mkdir` it first.
rb-cli mkdir  disk.hda /extras
rb-cli import disk.hda ./stuff /extras

# Re-running over an existing tree: pick a conflict policy explicitly.
rb-cli import disk.hda ./stuff --skip-existing   # leave what's there
rb-cli import disk.hda ./stuff --force           # replace it
```

Entries are visited in sorted order, so the same tree always produces the
same on-disk layout. Symlinks are recreated where the filesystem supports
them and counted where it can't (FAT/HFS), rather than aborting the run;
the same goes for names the target can't store and for hardlinks / device
nodes. macOS `._*` sidecars are skipped unless you pass
`--include-appledouble`.

Tar archives found in the tree are copied in as opaque files by default.
`--expand-archives` unpacks them instead, into a directory named after
each:

```bash
rb-cli import disk.hda ./downloads --expand-archives
```

Detection is a content sniff for the `ustar` magic, not an extension
match — so an oddly-named archive (IRIX `.tardist`, say) is still found,
and a gzipped *disk image* is not mistaken for one.

## Build an IRIX EFS CD-ROM

An IRIX-mountable disc is an SGI volume header with the EFS filesystem in
slot 7 typed SYSV. `--from-dir` formats and fills in one command.

```bash
# Blank 600 MB disc (a CD-R), to fill later.
rb-cli optical new sgi-efs irix.iso --size 600M --name IRIX53

# Format and fill in one step; `auto` sizes the disc to the folder
# rather than to a media size.
rb-cli optical new sgi-efs irix.iso --size auto --name IRIX53 \
    --from-dir ~/sgi-stuff

# Real IRIX CDs are sparser than our default ~1 inode/4 KiB. With a
# handful of large files, reclaim the inode tables:
rb-cli optical new sgi-efs irix.iso --size 600M --bytes-per-inode 32768 \
    --from-dir ~/sgi-stuff

rb-cli ls   irix.iso@1 /
rb-cli fsck irix.iso@1
```

Mount it on IRIX with `mount -t efs -o ro /dev/dsk/dks0d<N>s7 /CDROM`.

For a folder of `.tardist` packages, `--flatten-folders` unpacks every
archive into a single root instead of one directory each — the shape
`inst` expects, since you point it at one directory holding all the
product images:

```bash
rb-cli optical new sgi-efs irix.iso --size auto --name IRIX53 \
    --from-dir ~/sgi-stuff --expand-archives --flatten-folders
```

Merging makes overlap normal rather than exceptional — SGI freeware
tardists all ship the same `fw_common*` product — so `--flatten-folders`
skips entries that already exist and reports the count. Pass `--force` to
overwrite instead.

The same `--from-dir` / `--size auto` flags work on `new hd sgi-efs` for a
dvh-wrapped IRIX hard disk, and the TUI's new-image wizard offers the
CD-ROM class with an optional source folder.

## Create a blank NTFS volume

```bash
# Auto geometry (cluster size chosen from the volume size, 512-byte sectors)
rb-cli new volume ntfs disk.img --size 64M --name DATA

# Pick the cluster and sector size explicitly
rb-cli new volume ntfs disk.img --size 1G --cluster-size 64K --sector-size 4096
```

The result is a bare single-partition NTFS superfloppy (no MBR/GPT), validated
to mount under ntfs-3g; the size is rounded down to a whole cluster.

## Round-trip a real device through backup + restore

```bash
# Survey the source first
sudo rb-cli inspect /dev/disk3

# Back it up to a CHD with SHA256 per-partition checksums
sudo rb-cli backup /dev/disk3 ./backups --format chd --checksum sha256 \
    --name MyDisk-$(date +%Y-%m-%d)

# Restore the entire disk back onto a different device
sudo rb-cli restore ./backups/MyDisk-2026-05-19 /dev/disk4 \
    --device --yes
```

## Use globs to extract or remove many files

```bash
# List every .txt at any depth
rb-cli ls disk.hda /**/*.txt

# Same, anchored only under /System
rb-cli ls disk.hda '/System/**/*.txt'

# Brace expansion + exclude
rb-cli ls disk.hda '/Apps/*.{bin,exe}' --exclude '/Apps/uninstall.exe'

# Recursively remove every match (deepest-first, atomic-ish — one sync)
rb-cli rm disk.hda '/Apps/temp/*' -r
```

When a name itself contains a glob metacharacter (`* ? [ ] { }` — common on
classic-Mac volumes), pass `--literal` (`-L`) to address it verbatim instead of
treating it as a pattern:

```bash
# A folder literally named "Columns ][ 1.1" — without --literal the `][`
# parses as an unclosed character class and the command fails.
rb-cli ls --literal disk.hda '/Games/1992/Columns ][ 1.1'

# Extract or remove an exact path containing brackets or braces
rb-cli get-binhex --literal disk.hda '/Games/Foo/Bar [data].rsrc' out.hqx
rb-cli rm --literal disk.hda '/Apps/Maze Wars+ {old}'
```

## Drive everything from a batch JSON script

`script.json`:

```json
{
  "schema": "rb-cli-batch/1",
  "target": "./disk.dsk",
  "default_options": { "creator": "MACS" },
  "operations": [
    { "op": "mkdir", "path": "/System Folder" },
    { "op": "put", "src": "./build/Finder", "dst": "/System Folder/Finder",
      "type": "FNDR" },
    { "op": "put", "src": "./build/System", "dst": "/System Folder/System",
      "type": "ZSYS", "force": true },
    { "op": "rm", "path": "/Trash", "recursive": true }
  ]
}
```

```bash
# Dry-run first to see the resolved plan
rb-cli batch script.json --dry-run

# Apply: one sync_metadata at the end, stop-on-first-failure
rb-cli batch script.json

# Or push through and report failures at the end
rb-cli batch script.json --continue-on-error
```

## Structured output for scripted consumers

Every read-only verb supports `--format json|yaml|csv|tsv`. The JSON / YAML
shapes share the same envelope:

```json
{
  "schema_version": 1,
  "status": { "error": false, "code": 0, "message": null },
  "result": { ... }
}
```

```bash
# fs-info for tooling
rb-cli show fs-info disk.hda@2 --format json | jq '.result.free_blocks'

# CSV table of every device
rb-cli show devices --format csv

# TSV partmap
rb-cli show partmap disk.hda --format tsv
```

CSV / TSV is rejected for nested-result verbs (`inspect`, `fs-info`,
`chd-info`); use JSON or YAML for those.
