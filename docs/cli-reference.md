# `rb-cli` reference

_Auto-generated from the clap argument definitions in `src/cli/`.  Re-run `cargo run --example generate_cli_reference` after grammar changes._

## Synopsis

```
Usage: rb-cli [OPTIONS] <COMMAND>
```

## Global options

**Options**

- `--log-level` — Diagnostic verbosity for stderr logs
- `-q` / `--quiet` — Suppress all stderr output except errors and the final result. Mutually exclusive with `--log-level debug|trace`
- `--progress` — Progress bar behavior. `auto` is the default; `never` is the safest setting inside CI / cron / wrapper scripts
- `--color` — ANSI color usage. Honors the `NO_COLOR` env var when set
- `--log-file` — Mirror full trace-level log output to PATH regardless of `--log-level`. Useful on Windows cmd where redirection is awkward

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
- `--format` — Output format
- `--checksum` — Checksum to record per file
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
- `--prompt-timeout` — Seconds to wait for an interactive repair confirmation before resolving to "No" (default 30). `0` waits indefinitely (TTY only)

### `get`

Extract a file from a filesystem to the host

```
Usage: get <IMAGE> <SRC> <DST>
```

**Arguments**

- `<IMAGE>` — Image reference (`path` or `path@N` for the 1-based partition index)
- `<SRC>` — Source path inside the filesystem
- `<DST>` — Destination path on the host

### `inspect`

Whole-disk aggregate read-only view (partition table + per-partition summary + CHD metadata when applicable)

```
Usage: inspect [OPTIONS] <IMAGE>
```

**Arguments**

- `<IMAGE>` — Image path. `inspect` always reads the whole disk — there is no `@N` form. For per-partition detail use `show fs-info IMG@N`

**Options**

- `--format` — Output format

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
- `--name` — HFS volume name (1..=27 Mac Roman bytes for HFS). Defaults to `MacIIBench`
- `--block-size` — HFS allocation block size in bytes. Must be a non-zero multiple of 512. When unset, the smallest size that keeps `total_blocks <= 65535` is chosen automatically

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
- `--type` — 4-character type code (HFS / HFS+ / ProDOS). Defaults to `BINA`
- `--creator` — 4-character creator code (HFS / HFS+ only). Defaults to `????`
- `--force` — Overwrite an existing entry at the destination path

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
- `--size` — Per-partition size policy
- `--alignment` — Partition alignment policy
- `--device` — Treat `TARGET` as a block device (enables sector-aligned writes and device-write safety summaries). Phase C: enforced via this flag; Phase D's auto-detection lands in src/cli/device_safety.rs
- `--yes` — Confirm destructive write to the target (required for device targets). For file targets the flag is a no-op
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

