# `rb-cli` examples

Recipe-style snippets. Each block is self-contained and runs against the
tempdir / fixture images noted at the top. The canonical grammar lives in
`rb-cli --help` and the auto-generated `docs/cli-reference.md`.

Topics:

- [Inspect what's on a disk](#inspect-whats-on-a-disk)
- [Create + populate an HFS floppy](#create--populate-an-hfs-floppy)
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
rb-cli new disk.dsk --fs hfs --size 800K --name "Boot Disk"

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
