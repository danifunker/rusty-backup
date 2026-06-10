# `rb-cli` reference

_Auto-generated from the clap argument definitions in `src/cli/`.  Re-run `cargo run --example generate_cli_reference` after grammar changes._

## Synopsis

```
Usage: rb-cli [OPTIONS] <COMMAND>
```

## Global options

**Options**

- `--log-level` — Diagnostic verbosity for stderr logs. Falls back to `[defaults] log-level` from the config; built-in default `warn`
- `-q` / `--quiet` — Suppress all stderr output except errors and the final result. Mutually exclusive with `--log-level debug|trace`
- `--progress` — Progress bar behavior. Built-in default `auto`; `never` is the safest setting inside CI / cron / wrapper scripts
- `--color` — ANSI color usage. Honors the `NO_COLOR` env var when set. Built-in default `auto`
- `--log-file` — Mirror full trace-level log output to PATH regardless of `--log-level`. Useful on Windows cmd where redirection is awkward
- `--config` — Path to a config file. Overrides the platform default location. See `rb-cli config path` for what that location is

## Verbs

### `api`

Unstable scratch namespace for low-level operations. Kept as a deprecated alias for the flat verbs above; grammar inside `api` is expected to churn — do not depend on it from durable scripts

```
Usage: api <COMMAND>
```

### `api apm`

Apple Partition Map (APM) disk operations

```
Usage: apm <COMMAND>
```

### `api apm info`

Print the partition map of an APM disk image

```
Usage: info <IMAGE>
```

**Arguments**

- `<IMAGE>` — 

### `api hfs`

Classic-HFS image operations (create, browse, edit single-partition .dsk images)

```
Usage: hfs <COMMAND>
```

### `api hfs get`

Extract an HFS file to the host

```
Usage: get [OPTIONS] <IMAGE> <MAC_PATH> <HOST_FILE>
```

**Arguments**

- `<IMAGE>` — 
- `<MAC_PATH>` — 
- `<HOST_FILE>` — 

**Options**

- `--partition` — 

### `api hfs info`

Print volume name, sizes, and counts for an HFS image

```
Usage: info [OPTIONS] <IMAGE>
```

**Arguments**

- `<IMAGE>` — 

**Options**

- `--partition` — APM partition index to open (1-based). If unset and the image is an APM disk, the sole Apple_HFS partition is used

### `api hfs ls`

List a directory inside the HFS volume

```
Usage: ls [OPTIONS] <IMAGE> [PATH]
```

**Arguments**

- `<IMAGE>` — 
- `<PATH>` — Mac path (use `/` separators). Defaults to root

**Options**

- `--partition` — 

### `api hfs new`

Create a fresh blank HFS volume at the given path

```
Usage: new [OPTIONS] <IMAGE>
```

**Arguments**

- `<IMAGE>` — Image file to create. Overwritten if it already exists

**Options**

- `--size` — Volume size, accepting plain bytes or `K`/`KiB`/`M`/`MiB` suffixes (e.g. `800K`, `5M`). Defaults to 800K (an 800 KiB floppy)
- `--name` — HFS volume name (1..=27 Mac Roman bytes). Defaults to `MacIIBench`
- `--block-size` — HFS allocation block size in bytes. Must be a non-zero multiple of 512. When unset, the smallest size that keeps `total_blocks <= 65535` is chosen automatically (e.g. 512 for floppies, larger for multi-MiB SCSI images)

### `api hfs put`

Copy a host file into the HFS volume

```
Usage: put [OPTIONS] <IMAGE> <HOST_FILE> <MAC_PATH>
```

**Arguments**

- `<IMAGE>` — 
- `<HOST_FILE>` — Source file on the host
- `<MAC_PATH>` — Destination Mac path inside the volume. The parent directory must already exist

**Options**

- `--type` — HFS 4-character type code. Defaults to `BINA`
- `--creator` — HFS 4-character creator code. Defaults to `????`
- `--force` — Overwrite an existing entry at the destination path
- `--partition` — 

### `api hfs put-boot`

Overwrite the 1024-byte boot block region at offset 0. The source must be at most 1024 bytes and is written verbatim — no padding, no HFS B-tree touch. Operates on the file's byte 0 regardless of any APM wrapping

```
Usage: put-boot <IMAGE> <BB_FILE>
```

**Arguments**

- `<IMAGE>` — 
- `<BB_FILE>` — 

### `api hfs put-zero`

Pre-allocate a zero-filled file at the given Mac path. Useful for reserving a results file the boot ROM will fill in

```
Usage: put-zero [OPTIONS] <IMAGE> <MAC_PATH> <SIZE>
```

**Arguments**

- `<IMAGE>` — 
- `<MAC_PATH>` — 
- `<SIZE>` — Number of zero bytes to allocate

**Options**

- `--type` — 
- `--creator` — 
- `--force` — 
- `--partition` — 

### `api hfs rm`

Delete a file from the HFS volume

```
Usage: rm [OPTIONS] <IMAGE> <MAC_PATH>
```

**Arguments**

- `<IMAGE>` — 
- `<MAC_PATH>` — 

**Options**

- `--partition` — 

### `api hfs validate`

Run the lightweight HFS integrity check on the image

```
Usage: validate [OPTIONS] <IMAGE>
```

**Arguments**

- `<IMAGE>` — 

**Options**

- `--partition` — 

### `api sgi`

SGI/IRIX disk operations

```
Usage: sgi <COMMAND>
```

### `api sgi shrink`

Re-encode an IRIX disk image into a CHD whose logical size matches the SGI volume header's used floor. Drops trailing zero padding past `max(first + blocks)` over all non-empty partition entries. Accepts a raw `.img` or an existing `.chd` as input; always writes a CHD. Refuses to overwrite the source or an existing output file

```
Usage: shrink <INPUT> <OUTPUT>
```

**Arguments**

- `<INPUT>` — Source image (raw `.img` or `.chd`). Must contain an SGI volume header at sector 0
- `<OUTPUT>` — Destination CHD path. Must end in `.chd`, must not already exist, and must not resolve to the same file as `input`

### `backup`

Back up a disk image or device to a backup folder

```
Usage: backup [OPTIONS] <SOURCE> <DEST>
```

**Arguments**

- `<SOURCE>` — Source: an image file or a block-device path
- `<DEST>` — Destination directory. The backup is written under `DEST/<name>/`. The directory is created if it doesn't exist

**Options**

- `--name` — Backup name (the subdirectory under `DEST`). Defaults to the source file's stem with a date suffix
- `--format` — Output format. Defaults to `chd`, or the `[backup] format` value from the config file when set
- `--checksum` — Checksum to record per file. Defaults to `sha256`, or the `[backup] checksum` value from the config file when set
- `--sector-by-sector` — Skip filesystem-aware compaction; copy every sector verbatim
- `--partitions` — Per-partition filter — comma-separated 1-based indices to include (e.g. `1,3,4`). Default is "all partitions"
- `--split-size` — Split each output stream after this many MiB (Zstd / Raw only)

### `batch`

Apply a JSON-described sequence of FS operations to an image as one transaction-like batch

```
Usage: batch [OPTIONS] <SCRIPT>
```

**Arguments**

- `<SCRIPT>` — Path to the batch JSON script

**Options**

- `--target` — Override the script's `target` field (`path` or `path@N`)
- `--dry-run` — Validate + print the plan, don't apply
- `--continue-on-error` — Continue with remaining ops after a non-fatal failure. Default is stop-on-first-error

### `batch-template`

Generate a starter `batch` JSON script from a host directory

```
Usage: batch-template [OPTIONS] --target <TARGET> <HOSTDIR>
```

**Arguments**

- `<SOURCE>` — Host directory to mirror

**Options**

- `--target` — Target image (and optional partition) the batch script will modify. Written verbatim into the script's `target` field
- `--dst` — Destination directory inside the target filesystem (`/` for root)
- `--out` — Write the script here. Defaults to stdout
- `--include` — Include only paths matching these globs (repeatable). Default is "all"
- `--exclude` — Exclude paths matching these globs (repeatable). Exclude wins on conflict
- `--icase` — Glob matching is case-insensitive
- `--default-type` — Default HFS type code for files with no extension match
- `--default-creator` — Default HFS creator code for files with no extension match

### `bless`

Inspect or set the bootable System Folder on an HFS / HFS+ volume (`set` / `show` / `pick`)

```
Usage: bless <COMMAND>
```

### `bless pick`

Interactively browse the volume's folders and pick one to bless

```
Usage: pick <IMAGE>
```

**Arguments**

- `<IMAGE>` — Image reference (`path` or `path@N`)

### `bless set`

Bless the folder at PATH (mark it as the bootable System Folder)

```
Usage: set <IMAGE> <PATH>
```

**Arguments**

- `<IMAGE>` — Image reference (`path` or `path@N`)
- `<PATH>` — Absolute Mac path of the folder to bless (e.g. `/System Folder`)

### `bless show`

Print the volume's current blessed System Folder

```
Usage: show <IMAGE>
```

**Arguments**

- `<IMAGE>` — Image reference (`path` or `path@N`)

### `chmeta`

Change the type and/or creator code on an existing HFS / HFS+ / ProDOS file

```
Usage: chmeta [OPTIONS] <IMAGE> <PATH>
```

**Arguments**

- `<IMAGE>` — Image reference (`path` or `path@N`)
- `<PATH>` — Absolute Mac path of the file to update

**Options**

- `--type` — New 4-character type code
- `--creator` — New 4-character creator code (HFS / HFS+ only)

### `completions`

Emit a shell-completion script to stdout

```
Usage: completions <SHELL>
```

**Arguments**

- `<SHELL>` — Shell to generate completions for

### `config`

Manage the rbcli.conf config file

```
Usage: config <COMMAND>
```

### `config init`

Write a commented template to the user's config location (or to `--path PATH`). Refuses to overwrite an existing file unless `--force`

```
Usage: init [OPTIONS]
```

**Options**

- `--path` — Override the destination path
- `--force` — Overwrite an existing file

### `config path`

Print the resolved config-file path (whether it exists or not)

```
Usage: path
```

### `config show`

Print the loaded config as `section.key = value` lines

```
Usage: show [OPTIONS]
```

**Options**

- `--path` — 

### `convert`

Re-encode one or more disk images into a chosen output format

```
Usage: convert [OPTIONS] <IN> <OUT>
```

**Arguments**

- `<SOURCE>` — Source file or folder. When a folder, every convertible file is processed
- `<DEST>` — Destination folder. Created if absent

**Options**

- `--format` — Output format
- `--extension` — Output extension (no leading dot). Defaults to the format's natural extension (chd, vhd, img, …)
- `--bincue-multi-bin` — For BIN/CUE output, write one .bin per track instead of a single concatenated .bin. No effect for other formats
- `--overwrite` — Overwrite destination files that already exist. Without this, existing outputs are skipped with a warning

### `expand`

Expand a classic-HFS volume to a new size + allocation block size by cloning into a fresh APM disk image (default) or a bare HFS image (`--to-hfv`). Accepts APM-wrapped sources or raw single- partition HFS images

```
Usage: expand [OPTIONS] --size <SIZE> --output <OUTPUT> <IMAGE>
```

**Arguments**

- `<IMAGE>` — Source image reference (`path` or `path@N` for the classic HFS partition)

**Options**

- `--size` — Target volume size in bytes. Accepts suffixes (`K`, `M`, `G`)
- `--block-size` — Allocation block size in bytes. One of: 4096, 8192, 16384, 32768, 65536. If omitted, picks the smallest block size whose 65535-block ceiling can hold `--size`
- `--output` — Destination path for the new image. Created (or truncated)
- `--to-hfv` — Write a flat BasiliskII HFV (bare classic-HFS volume, no partition table) instead of an APM disk image. Capped at 2047 MB. Use this to produce a `.hfv` for BasiliskII / SheepShaver

### `floppy`

Floppy-container verbs (convert / info) for XDF, HDM, DIM, D88

```
Usage: floppy <COMMAND>
```

### `floppy convert`

Convert a floppy image between XDF / HDM / DIM / D88 formats. The output format is inferred from the destination extension

```
Usage: convert [OPTIONS] <INPUT> <OUTPUT>
```

**Arguments**

- `<INPUT>` — Source floppy image (.xdf, .hdm, .dim, .d88) — or a directory of floppy images when paired with `--to`
- `<OUTPUT>` — Destination path. For a file input the target format is taken from the extension; for a directory input pass a directory here and use `--to <fmt>` to pick the output format

**Options**

- `--to` — Output format for directory (bulk) mode. Required when `input` is a directory; ignored for single-file mode (extension wins there)
- `--recursive` — Walk the input directory recursively. Only meaningful in bulk mode

### `floppy info`

Print the detected container kind and geometry for a floppy image

```
Usage: info <INPUT>
```

**Arguments**

- `<INPUT>` — Floppy image to inspect

### `fsck`

Check (and optionally repair) a filesystem

```
Usage: fsck [OPTIONS] <IMAGE>
```

**Arguments**

- `<IMAGE>` — Image reference (`path` or `path@N` for the 1-based partition index)

**Options**

- `--checkonly` — Scan only. Never prompt, never repair. Exits non-zero on issues
- `--repair` — Auto-repair detected issues without prompting
- `--prompt-timeout` — Seconds to wait for an interactive repair confirmation before resolving to "No" (default 30; or `[fsck] prompt-timeout` from the config file when set). `0` waits indefinitely (TTY only)
- `--format` — Output format. `text` (default) emits the human-readable report; `json` / `yaml` emit a status-wrapped envelope mirroring the other read-only verbs. `csv` / `tsv` are rejected — the report is nested

### `get`

Extract a file, directory tree, or glob match from a filesystem to the host

```
Usage: get [OPTIONS] <IMAGE> <SRC> <DST>
```

**Arguments**

- `<IMAGE>` — Image reference (`path` or `path@N` for the 1-based partition index)
- `<SRC>` — Source path or glob inside the filesystem. Patterns containing `*`, `?`, `[`, or `{` walk the volume and extract every match
- `<DST>` — Destination path on the host. Single-match: the literal target file. Multi-match or directory source: a directory under which matched entries are laid out (created if it doesn't exist)

**Options**

- `-r` / `--recursive` — Recursively extract directories (literal dir source or glob match against a directory). Without this flag, matched directories are skipped with a warning
- `--exclude` — Exclude paths matching this glob. Repeatable. Exclude always wins over `--include` / the positional source
- `--ignore-case` — Match case-insensitively regardless of the target's native rule
- `--case-sensitive` — Match case-sensitively regardless of the target's native rule
- `--force` — Overwrite existing host files. Mutually exclusive with `--skip-existing`
- `--skip-existing` — Skip silently when a host file already exists. Mutually exclusive with `--force`. Without either flag, an existing destination is a hard error
- `--password` — Password for encrypted containers (currently: WinImage IMZ)
- `--fs-type` — Force a specific filesystem dispatch. The main use is `cpm:<preset>` for CP/M images (which have no on-disk signature). Valid CP/M presets: `amstrad_data`, `amstrad_sys`, `amstrad_pcw`, `einstein`, `svi328_cpm`, `altair_8in`, `altair_cf`, `multicomp`, `zx_plus3`. Other strings (e.g. `human68k`, `qdos`) are also accepted and forwarded to the partition_type_string dispatch

### `get-binhex`

Extract a file and encode it as BinHex 4.0 (.hqx), preserving both forks and the type/creator codes

```
Usage: get-binhex [OPTIONS] <IMAGE> <SRC> <DST>
```

**Arguments**

- `<IMAGE>` — Image reference (`path` or `path@N`)
- `<SRC>` — Source path inside the filesystem
- `<DST>` — Destination `.hqx` path on the host

**Options**

- `--password` — Password for encrypted containers (currently: WinImage IMZ)

### `grow`

Grow a disk image by `--add SIZE` of trailing zero-padding so a subsequent `partmap` edit can place a new partition

```
Usage: grow --add <ADD> <IMAGE>
```

**Arguments**

- `<IMAGE>` — Image to grow

**Options**

- `--add` — Bytes of zero-padding to add at the end (e.g. `512M`, `2G`)

### `inspect`

Whole-disk aggregate read-only view (partition table + per-partition summary + CHD metadata when applicable)

```
Usage: inspect [OPTIONS] <IMAGE>
```

**Arguments**

- `<IMAGE>` — Image path. `inspect` always reads the whole disk — there is no `@N` form. For per-partition detail use `show fs-info IMG@N`

**Options**

- `--format` — Output format
- `--password` — Password for encrypted containers (currently: WinImage IMZ)

### `install-completions`

Install shell completions to the user-scoped canonical location

```
Usage: install-completions [OPTIONS]
```

**Options**

- `--shell` — Force the shell instead of auto-detecting from `$SHELL` / `$PSModulePath`
- `--prefix` — Override the install prefix (rarely needed). When set, the file is written under `PREFIX/<canonical-subdir>`
- `--print` — Print the script to stdout instead of writing to disk
- `--uninstall` — Remove the installed completion file. No-op if it doesn't exist

### `locate`

Print the absolute byte offset and length of a file inside an image (HFS only today). Output is JSON so build scripts that patch disk offsets into boot blocks can parse it with `jq`

```
Usage: locate [OPTIONS] <IMAGE> <PATH>
```

**Arguments**

- `<IMAGE>` — Image reference (`path` or `path@N` for the 1-based partition index)
- `<PATH>` — Path inside the filesystem (Mac path conventions; `/` is the separator — `:` is rejected for the same reason as the other verbs)

**Options**

- `--format` — Output format. `json` is the default because the load-bearing consumer is a build script

### `ls`

List a directory inside a filesystem

```
Usage: ls [OPTIONS] <IMAGE> [PATH]
```

**Arguments**

- `<IMAGE>` — Image reference (`path` or `path@N` for the 1-based partition index)
- `<PATH>` — Path or glob pattern inside the filesystem (use `/` as the separator). A plain path lists that directory's contents; patterns containing `*`, `?`, `[`, or `{` walk the volume and emit one line per match

**Options**

- `--exclude` — Exclude paths matching this glob. Repeatable. Exclude always wins over `--include` / a positional path
- `--ignore-case` — Treat case-insensitively, regardless of the target's native rule
- `--case-sensitive` — Treat case-sensitively, regardless of the target's native rule
- `--password` — Password for encrypted containers (currently: WinImage IMZ)
- `--fs-type` — Force a specific filesystem dispatch. The main use is `cpm:<preset>` for CP/M images (which have no on-disk signature). Valid CP/M presets: `amstrad_data`, `amstrad_sys`, `amstrad_pcw`, `einstein`, `svi328_cpm`, `altair_8in`, `altair_cf`, `multicomp`, `zx_plus3`. Other strings (e.g. `human68k`, `qdos`) are also accepted and forwarded to the partition_type_string dispatch

### `mkdir`

Create a directory inside a filesystem

```
Usage: mkdir <IMAGE> <PATH>
```

**Arguments**

- `<IMAGE>` — Image reference (`path` or `path@N` for the 1-based partition index)
- `<PATH>` — Directory path to create. The parent must exist (no `-p`-style auto-creation in Phase B)

### `new`

Create a blank single-partition image (superfloppy or, in Phase D, partition-table-wrapped)

```
Usage: new [OPTIONS] --fs <FS> <IMAGE>
```

**Arguments**

- `<IMAGE>` — Image file to create. Overwritten if it already exists

**Options**

- `--fs` — Filesystem to format
- `--size` — Volume size, accepting plain bytes or `K`/`KiB`/`M`/`MiB`/`G`/`GiB` suffixes (e.g. `800K`, `5M`). Defaults to 800K (an 800 KiB floppy)
- `--name` — Volume label/name. Defaults to `rusty-backup`. HFS: up to 27 Mac Roman bytes. FAT: up to 11 chars (uppercased; non-ASCII → `_`). EFS: 6-byte fname/fpack. AFFS: up to 30 bytes
- `--block-size` — HFS allocation block size in bytes. Must be a non-zero multiple of 512. When unset, the smallest size that keeps `total_blocks <= 65535` is chosen automatically. Ignored for other filesystems
- `--catalog-size` — HFS Catalog B-tree initial size in bytes (rounded up to a whole allocation block). When unset, scales with volume size like hformat (~0.5%, clump-aligned, 24-block floor). Ignored for other filesystems
- `--extents-size` — HFS Extents-overflow B-tree initial size in bytes (rounded up to a whole allocation block). When unset, ~half the catalog size. Ignored for other filesystems
- `--affs-variant` — AFFS variant byte (0=OFS, 1=FFS, 2=OFS+intl, 3=FFS+intl, 4=OFS+dircache, 5=FFS+dircache). Defaults to 1 (FFS)

### `new-x68k-hdd`

Build a self-bootable Sharp X68000 HDD image (SASI / SCSI) with an X68K partition table + IPL stub + Human68k partition, optionally pre-populated by cloning a Human68k donor floppy

```
Usage: new-x68k-hdd [OPTIONS] <IMAGE>
```

**Arguments**

- `<IMAGE>` — Image file to create. Overwritten if it already exists

**Options**

- `--size` — Disk size, accepting plain bytes or `K`/`KiB`/`M`/`MiB`/`G`/`GiB` suffixes (e.g. `8M`, `16M`). Defaults to `16M` — large enough for a full Human68k system clone plus room for user files
- `--variant` — Sharp HDD controller convention to emit
- `--stub` — Which byte-0 IPL stub to write. `print` (default) renders a status banner via IOCS; `halt` is the bare minimum 2-byte halt loop
- `--partitions` — Number of Human68k partitions to carve out (1-8). The disk's data area is split equally; partition 1 (slot 0) is the one that gets `--system-disk` files and the optional `--boot-sector-donor` overlay. Other partitions are formatted blank FAT12/16. Defaults to 1 — multi-partition only matters when you want separate volumes for system / games / scratch on the same HDD
- `--system-disk` — Optional donor Human68k system floppy (flat `.img` or `.dim` / `.D88` / `.xdf` / `.hdm` container). When present, the builder recursively clones every file and subdirectory from the donor into the output partition. Without this flag, three seed text files (`HELLO.TXT`, `MISTER.TXT`, `README.TXT`) are written for engine validation
- `--boot-sector-donor` — Optional donor *real* Sharp X68000 SCSI HDD whose Human68k partition boot sector (Sharp IPL Copyright 1990 SHARP) we'll extract and overlay onto the output partition. Eliminates the post-build `SWITCH.X /HD` step — the HDD self-boots straight to `C:>` on every power-on
- `--builtin-boot-sector` — Use the **in-tree Hero Soft V1.10 boot sector** (1024 bytes, SHA1 `3e88955020de2191441e5829ee5a6e95890a3212`) instead of requiring `--boot-sector-donor PATH`. SCSI only

### `optical`

Optical-media verbs (rip / convert / browse / extract)

```
Usage: optical <COMMAND>
```

### `optical browse`

List the file tree on an optical disc image

```
Usage: browse <SOURCE>
```

**Arguments**

- `<SOURCE>` — Optical disc image (.iso, .cue, .chd)

### `optical convert`

Re-encode an optical image into a different format

```
Usage: convert --format <FORMAT> <SOURCE> <DEST>
```

**Arguments**

- `<SOURCE>` — Source image (.iso, .cue, or .chd)
- `<DEST>` — Destination file. Extension is *not* auto-derived — pass it explicitly

**Options**

- `--format` — Output format

### `optical extract`

Extract files from an optical disc image into a host folder

```
Usage: extract [OPTIONS] --to <TO> <SOURCE>
```

**Arguments**

- `<SOURCE>` — Optical disc image (.iso, .cue, .chd)

**Options**

- `--to` — Destination folder (created if absent)
- `--resource-forks` — How to handle HFS resource forks. Ignored on non-HFS discs. Defaults to `appledouble`, or `[optical] resource-forks` from the config file when set

### `optical rip`

Rip a physical CD/DVD drive to a disk image file

```
Usage: rip [OPTIONS] --device <DEVICE> --output <OUTPUT>
```

**Options**

- `--device` — Source drive (e.g. `/dev/sr0`, `disk6`, `\\.\E:`). See `rb-cli show devices`
- `--output` — Output path: `.iso` for `--format iso`, `.cue` for `--format bincue`
- `--format` — 
- `--eject` — Eject the disc after a successful rip

### `partmap`

Edit the partition table (add / resize / delete / set-type / set-bootable). Partition *data* is never moved

```
Usage: partmap <COMMAND>
```

### `partmap add`

Add a new partition entry

```
Usage: add [OPTIONS] --start-lba <START_LBA> --size <SIZE> <IMAGE>
```

**Arguments**

- `<IMAGE>` — Image to modify

**Options**

- `--start-lba` — Start LBA (512-byte sector). MBR / GPT: linear LBA. APM: block #
- `--size` — Partition size in bytes (accepts `K`/`M`/`G` suffixes)
- `--type-byte` — MBR type byte (decimal or `0xNN`). Ignored for non-MBR tables
- `--type-string` — GPT type GUID string, or APM type string (`"Apple_HFS"`, etc.)
- `--bootable` — Mark active/bootable

### `partmap apply`

Apply a JSON script of edits as one transaction

```
Usage: apply [OPTIONS] <IMAGE> <SCRIPT>
```

**Arguments**

- `<IMAGE>` — 
- `<SCRIPT>` — JSON script with the same schema as `PartitionEditScript` below (an `edits` array of typed entries)

**Options**

- `--dry-run` — Validate + print the plan, don't apply

### `partmap delete`

Delete a partition entry (zeroes the slot)

```
Usage: delete <IMAGE> <INDEX>
```

**Arguments**

- `<IMAGE>` — 
- `<INDEX>` — 

### `partmap move`

Move a partition entry to a new start LBA (does not move data)

```
Usage: move --start-lba <START_LBA> <IMAGE> <INDEX>
```

**Arguments**

- `<IMAGE>` — 
- `<INDEX>` — 

**Options**

- `--start-lba` — 

### `partmap resize`

Resize an existing partition entry (changes size only — data is not moved)

```
Usage: resize --size <SIZE> <IMAGE> <INDEX>
```

**Arguments**

- `<IMAGE>` — 
- `<INDEX>` — 1-based partition index

**Options**

- `--size` — 

### `partmap set-bootable`

Toggle the bootable flag (MBR active-partition bit; RDB flag)

```
Usage: set-bootable [OPTIONS] <IMAGE> <INDEX>
```

**Arguments**

- `<IMAGE>` — 
- `<INDEX>` — 

**Options**

- `--bootable` — 

### `partmap set-type`

Change a partition's type byte / GUID / APM type string

```
Usage: set-type [OPTIONS] <IMAGE> <INDEX>
```

**Arguments**

- `<IMAGE>` — 
- `<INDEX>` — 

**Options**

- `--type-byte` — MBR type byte (decimal or `0xNN`)
- `--type-string` — GPT type GUID / APM type string

### `put`

Copy a host file (or zero-fill / write boot blocks) into a filesystem

```
Usage: put [OPTIONS] <IMAGE> [HOST_FILE] [DST]
```

**Arguments**

- `<IMAGE>` — Image reference (`path` or `path@N` for the 1-based partition index)
- `<HOST_FILE>` — Host file to copy. Required when not using `--zero` or `--boot`
- `<DST>` — Destination path inside the filesystem (cp-like positional)

**Options**

- `--zero` — Pre-allocate N zero bytes instead of copying a host file. Pair with `--dst`
- `--dst` — Explicit destination flag; use this with `--zero` where the positional `DST` slot is awkward
- `--boot` — Write the 1024-byte boot-block region of the image verbatim. HFS-only today
- `--type` — 4-character type code (HFS / HFS+ / ProDOS). Defaults to `BINA`, or `[put] type` from the config file when set
- `--creator` — 4-character creator code (HFS / HFS+ only). Defaults to `????`, or `[put] creator` from the config file when set
- `--force` — Overwrite an existing entry at the destination path
- `--print-offset` — After writing the file, also print the same JSON envelope `locate` would have produced — absolute byte offset, length, fragmented flag. One-shot for build scripts that need to patch disk offsets immediately after placing a payload. HFS-only, matches the locate verb's scope; ignored (with a warning) for the `--zero` and `--boot` shapes since there's no host file to describe
- `--fs-type` — Force a specific filesystem dispatch. The main use is `cpm:<preset>` for CP/M images (which have no on-disk signature). Valid CP/M presets: `amstrad_data`, `amstrad_sys`, `amstrad_pcw`, `einstein`, `svi328_cpm`, `altair_8in`, `altair_cf`, `multicomp`, `zx_plus3`. Other strings (e.g. `human68k`, `qdos`) are also accepted and forwarded to the partition_type_string dispatch

### `put-binhex`

Decode a BinHex 4.0 (.hqx) file and write it (both forks + Finder info) into a filesystem

```
Usage: put-binhex [OPTIONS] <IMAGE> <HOST_FILE>
```

**Arguments**

- `<IMAGE>` — Image reference (`path` or `path@N`)
- `<HOST_FILE>` — BinHex 4.0 (`.hqx`) file on the host

**Options**

- `--dst-dir` — Destination directory inside the filesystem (`/` for root). The filename comes from the BinHex header. Defaults to `/`
- `--rename` — Override the filename from the BinHex header
- `--force` — Overwrite an existing entry at the destination path

### `put-macbinary`

Put a MacBinary I / II archive: both forks + full Finder info in one shot (HFS today)

```
Usage: put-macbinary [OPTIONS] <IMAGE> <HOST_FILE>
```

**Arguments**

- `<IMAGE>` — Image reference (`path` or `path@N`)
- `<HOST_FILE>` — MacBinary I / II archive on the host

**Options**

- `--dst-dir` — Destination directory inside the filesystem (`/` for root). The filename comes from the MacBinary header. Defaults to `/`
- `--rename` — Override the filename from the MacBinary header
- `--force` — Overwrite an existing entry at the destination path

### `reformat`

Reformat a partition in place, leaving the partition table intact (HFS only today)

```
Usage: reformat [OPTIONS] --fs <FS> <IMAGE>
```

**Arguments**

- `<IMAGE>` — Image reference (`path` or `path@N`)

**Options**

- `--fs` — Filesystem to format the partition with. Only `hfs` is supported today
- `--name` — New volume name. HFS: up to 27 Mac Roman bytes
- `--block-size` — HFS allocation block size in bytes (non-zero multiple of 512). Defaults to the smallest size that keeps total_blocks <= 65535
- `--catalog-size` — HFS Catalog B-tree initial size in bytes. Defaults to hformat-style scaling (~0.5% of the partition)
- `--extents-size` — HFS Extents-overflow B-tree initial size in bytes. Defaults to ~half the catalog size

### `repack`

Defragment a Human68k (X68000) partition in place: clone it into a fresh, contiguously-packed volume and write that back. Reclaims holes the in-place resizer can't (it keeps cluster byte-offsets)

```
Usage: repack [OPTIONS] <IMAGE>
```

**Arguments**

- `<IMAGE>` — Image reference (`path` or `path@N` for the 1-based partition index)

**Options**

- `--size` — New filesystem size in bytes (default: the partition's current size). Accepts suffixes (`K`, `M`, `G`). Must not exceed the partition capacity

### `resize`

Resize the filesystem at IMG@N to a new size (FAT/NTFS/exFAT/HFS+/ ext/btrfs/SFS/PFS3/AFFS/EFS — whichever magic matches)

```
Usage: resize --size <SIZE> <IMAGE>
```

**Arguments**

- `<IMAGE>` — Image reference (`path` or `path@N` for the 1-based partition index)

**Options**

- `--size` — New filesystem size in bytes. Accepts suffixes (`K`, `M`, `G`)

### `restore`

Restore a backup folder to a target image or device

```
Usage: restore [OPTIONS] <BACKUP_DIR> <TARGET>
```

**Arguments**

- `<BACKUP_DIR>` — Source backup folder (the directory containing `metadata.json`)
- `<TARGET>` — Target image file or block-device path

**Options**

- `--target-size` — Target size in bytes (defaults to the original disk size from the backup metadata)
- `--size` — Per-partition size policy. Defaults to `original`, or `[restore] size` from the config file when set
- `--alignment` — Partition alignment policy. Defaults to `original`, or `[restore] alignment` from the config file when set
- `--device` — Treat `TARGET` as a block device (enables sector-aligned writes and the full device-write safety preflight in [`crate::cli::device_safety`])
- `--yes` — Confirm destructive write to the target (required for device targets). For file targets the flag is a no-op
- `--write-to-system-disk` — Allow writing to the system boot disk (refused by default; only meaningful with `--device`)
- `--write-zeros-to-unused` — Write zeros to unused filesystem space

### `rm`

Delete a file or directory from a filesystem

```
Usage: rm [OPTIONS] <IMAGE> <PATH>
```

**Arguments**

- `<IMAGE>` — Image reference (`path` or `path@N` for the 1-based partition index)
- `<PATH>` — Path or glob pattern inside the filesystem. Patterns containing `*`, `?`, `[`, or `{` walk the volume and delete every match

**Options**

- `-r` / `--recursive` — Recursively delete directories (matches will include directories without this flag, but they get rejected unless --recursive)
- `--exclude` — Exclude paths matching this glob from deletion. Repeatable. Exclude always wins over the positional pattern
- `--ignore-case` — Match case-insensitively regardless of the target's native rule
- `--case-sensitive` — Match case-sensitively regardless of the target's native rule
- `--fs-type` — Force a specific filesystem dispatch. The main use is `cpm:<preset>` for CP/M images (which have no on-disk signature). Valid CP/M presets: `amstrad_data`, `amstrad_sys`, `amstrad_pcw`, `einstein`, `svi328_cpm`, `altair_8in`, `altair_cf`, `multicomp`, `zx_plus3`. Other strings (e.g. `human68k`, `qdos`) are also accepted and forwarded to the partition_type_string dispatch

### `setrsrc`

Write the resource fork of an existing HFS / HFS+ file from a host file

```
Usage: setrsrc --from-file <FROM_FILE> <IMAGE> <PATH>
```

**Arguments**

- `<IMAGE>` — Image reference (`path` or `path@N`)
- `<PATH>` — Absolute Mac path of the file whose resource fork should be replaced

**Options**

- `--from-file` — Host file whose contents become the new resource fork

### `setvolname`

Rename the volume at IMG[@N] (HFS only today)

```
Usage: setvolname <IMAGE> <NAME>
```

**Arguments**

- `<IMAGE>` — Image reference (`path` or `path@N`)
- `<NAME>` — New volume name. HFS: 1..=27 Mac Roman bytes

### `show`

Focused read-only queries

```
Usage: show <COMMAND>
```

### `show chd-info`

Print CHD metadata for a `.chd` image

```
Usage: chd-info [OPTIONS] <IMAGE>
```

**Arguments**

- `<IMAGE>` — 

**Options**

- `--format` — 

### `show devices`

List host block devices (disks attached to this machine)

```
Usage: devices [OPTIONS]
```

**Options**

- `--removable-only` — Filter to removable devices only
- `--format` — 

### `show fs-info`

Print filesystem-level metadata (volume name, sizes, counts)

```
Usage: fs-info [OPTIONS] <IMAGE>
```

**Arguments**

- `<IMAGE>` — 

**Options**

- `--format` — 

### `show partmap`

Print the partition table of a disk image (APM-only today)

```
Usage: partmap [OPTIONS] <IMAGE>
```

**Arguments**

- `<IMAGE>` — 

**Options**

- `--format` — Output format. `csv`/`tsv` produce one row per partition entry

### `shrink`

Re-encode a disk image into a CHD with trailing zero padding dropped (SGI/IRIX today)

```
Usage: shrink <INPUT> <OUTPUT>
```

**Arguments**

- `<INPUT>` — Source image (raw `.img` or `.chd`). Must contain an SGI volume header at sector 0
- `<OUTPUT>` — Destination CHD path. Must end in `.chd`, must not already exist, and must not resolve to the same file as `input`

### `sit`

Read classic StuffIt and Compact Pro archives (list / extract; accepts .sit, .sea, .cpt, and their BinHex-wrapped .hqx forms)

```
Usage: sit <COMMAND>
```

### `sit create`

Create a StuffIt archive from host files (.hqx / .bin / plain)

```
Usage: create [OPTIONS] <OUTPUT> <INPUTS>...
```

**Arguments**

- `<OUTPUT>` — Output path. A `.sit` extension writes a raw archive; a `.hqx` extension BinHex-wraps it (the classic `.sit.hqx` format)
- `<INPUTS>` — Input files. Each may be a BinHex `.hqx`, a MacBinary `.bin`, or a plain file (with an optional `._name` / `.rsrc` sidecar)

**Options**

- `--rle` — Compress forks with RLE90 (method 1) instead of storing uncompressed

### `sit extract`

Extract a StuffIt archive to a directory on the host

```
Usage: extract [OPTIONS] <ARCHIVE> <DEST>
```

**Arguments**

- `<ARCHIVE>` — StuffIt or Compact Pro archive (`.sit`, `.sea`, `.cpt`, or `.hqx`)
- `<DEST>` — Destination directory on the host (created if missing)

**Options**

- `--format` — Container format for the extracted files

### `sit list`

List the entries in a StuffIt archive

```
Usage: list <ARCHIVE>
```

**Arguments**

- `<ARCHIVE>` — StuffIt or Compact Pro archive (`.sit`, `.sea`, `.cpt`, or `.hqx`)

### `terminal`

Open an interactive rb-cli shell (rustyline-based REPL)

```
Usage: terminal
```

### `write`

Stream an image file onto a block device

```
Usage: write [OPTIONS] <IMAGE> <DEVICE>
```

**Arguments**

- `<IMAGE>` — Source image file
- `<DEVICE>` — Destination block-device path: - Linux: `/dev/sdX` or `/dev/nvmeXnY` - macOS: `/dev/diskN` / `/dev/rdiskN` - Windows: `"\\.\PhysicalDriveN"` (quote for PowerShell)

**Options**

- `--yes` — Required confirmation. Skips the prompt but never the safety summary printed on stderr
- `--write-to-system-disk` — Allow writing to the system boot disk (refused by default)

