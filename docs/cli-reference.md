# `rb-cli` reference

_Auto-generated from the clap argument definitions in `src/cli/`.  Re-run `cargo run --example generate_cli_docs` after grammar changes._

## Synopsis

```
Usage: rb-cli [OPTIONS] [COMMAND]
```

## Global options

**Options**

- `--log-level` — Diagnostic verbosity for stderr logs. Falls back to `[defaults] log-level` from the config; built-in default `warn`
- `-q` / `--quiet` — Suppress all stderr output except errors and the final result. Mutually exclusive with `--log-level debug|trace`
- `--progress` — Progress bar behavior. Built-in default `auto`; `never` is the safest setting inside CI / cron / wrapper scripts
- `--color` — ANSI color usage. Honors the `NO_COLOR` env var when set. Built-in default `auto`
- `--log-file` — Mirror full trace-level log output to PATH regardless of `--log-level`. Useful on Windows cmd where redirection is awkward
- `--config` — Path to a config file. Overrides the platform default location. See `rb-cli config path` for what that location is

**A note on `--format yaml`.** The read-only verbs below list `yaml` alongside
`json` wherever structured output is supported. YAML sits behind the `yaml` build
feature, which is **on by default** - every released desktop and vintage build has
it. It is off in exactly one configuration, the PowerPC/mrustc build
(`rb-cli-ppc`, see [build-ppc-mrustc.md](build-ppc-mrustc.md)), because
`serde_yml`'s backend cannot be transpiled. Where it is off, `yaml` is absent from
`--format`'s accepted values and from `--help`; JSON carries the identical schema,
so a script that must run anywhere should prefer `--format json`.

## Path grammar (in-image paths)

Verbs that take a path *inside* an image (`ls`, `get`, `get-binhex`, `put`,
`put-binhex`, `mkdir`, `rm`, `cp`, `locate`) address it with one of two
grammars:

- **Slash** (default, every filesystem): `/` is the separator. A literal `/`
  inside a single name — legal on classic-Mac HFS / HFS+ volumes, e.g.
  `Oxyd b/w` — is written `\/`; a literal backslash is written `\\`. So
  `rb-cli get-binhex IMG "/Games/Oxyd 3.6/Oxyd b\/w" out.hqx` extracts the
  single file `Oxyd b/w` from the folder `Oxyd 3.6`.
- **Colon** (HFS / HFS+ only): because classic Mac OS reserves `:` as its path
  separator, `:` can never appear in a name, so you may instead write the path
  with `:` separators — the native Mac convention — and then `/` is ordinary
  data needing no escape: `rb-cli get-binhex IMG ":Games:Oxyd 3.6:Oxyd b/w"
  out.hqx`. A colon-grammar path is always literal (it never globs).

On every other filesystem `:` is an ordinary filename byte and only the slash
grammar applies. Glob patterns (`*`, `?`, `[`, `{`) use the slash grammar; pass
`--literal` (or use the colon grammar) to address a name containing those
characters verbatim.

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

- `--type` — HFS 4-character type code. Inferred from the file extension when unset (same list as the GUI's type/creator picker), falling back to `BINA` for names the list doesn't recognize
- `--creator` — HFS 4-character creator code. Inferred from the file extension when unset, falling back to `????`
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

### `archive`

Read/write classic Mac archives (list / extract / create; accepts .sit, .sea, .cpt, .mar, and their BinHex-wrapped .hqx forms)

```
Usage: archive <COMMAND>
```

### `archive create`

Create a StuffIt or MAR archive from host files (.hqx / .bin / plain)

```
Usage: create [OPTIONS] <OUTPUT> <INPUTS>...
```

**Arguments**

- `<OUTPUT>` — Output path. `.sit` writes a raw StuffIt archive; `.hqx` BinHex-wraps it (the classic `.sit.hqx` format); `.mar` writes a stored MAR archive (a single file, or several wrapped in a folder named after the output)
- `<INPUTS>` — Input files. Each may be a BinHex `.hqx`, a MacBinary `.bin`, or a plain file (with an optional `._name` / `.rsrc` sidecar)

**Options**

- `--rle` — Compress forks with RLE90 (method 1) instead of storing uncompressed

### `archive extract`

Extract a StuffIt archive to a directory on the host

```
Usage: extract [OPTIONS] <ARCHIVE> <DEST>
```

**Arguments**

- `<ARCHIVE>` — StuffIt, Compact Pro, or MAR archive (`.sit`, `.sea`, `.cpt`, `.mar`, or `.hqx`)
- `<DEST>` — Destination directory on the host (created if missing)

**Options**

- `--format` — Container format for the extracted files

### `archive list`

List the entries in a StuffIt archive

```
Usage: list <ARCHIVE>
```

**Arguments**

- `<ARCHIVE>` — StuffIt, Compact Pro, or MAR archive (`.sit`, `.sea`, `.cpt`, `.mar`, or `.hqx`)

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
- `--defrag` — Defragment FAT partitions: relocate each file's clusters contiguously (boot files first) before imaging. Same output size as ordinary compaction — the restored disk is just defragmented. Non-FAT filesystems are unaffected. (The desktop sibling of cb-dos `/DEFRAG`.)
- `--partitions` — Per-partition filter — comma-separated 1-based indices to include (e.g. `1,3,4`; `1` is the first partition, matching the `img@N` selector). Default is "all partitions"
- `--split-size` — Split the output after this many MiB. Raw (`--format raw`) only: a `.chd` is a single self-contained container and refuses to be split, and the compressed codecs currently ignore this
- `--keep-swap` — Image swap/page files verbatim instead of excluding them. By default a FAT volume's swap/page files (`386SPART.PAR`, `WIN386.SWP`, `PAGEFILE.SYS`, `HIBERFIL.SYS`, `SWAPPER.DAT`) are kept full-size but their content is zeroed (they reinitialize on boot), which the codec crushes; `--keep-swap` images them as-is. (The desktop sibling of cb-dos `/KEEPSWAP`.)

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

### `cbk`

Pack a backup folder into a single `.cbk` container, or unpack one (`cbk pack` / `cbk unpack`). `restore` also reads a `.cbk` directly

```
Usage: cbk <COMMAND>
```

### `cbk pack`

Pack a native backup folder into a single `.cbk` container

```
Usage: pack <FOLDER> <OUT>
```

**Arguments**

- `<FOLDER>` — The backup folder (the directory containing `metadata.json`)
- `<OUT>` — Output `.cbk` file

### `cbk unpack`

Unpack a `.cbk` container back into a native backup folder

```
Usage: unpack <CONTAINER> <FOLDER>
```

**Arguments**

- `<CONTAINER>` — Input `.cbk` file
- `<FOLDER>` — Output folder (created if absent)

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
- `--attrs` — DOS attribute bits (FAT / exFAT). Comma-separated flags, each optionally prefixed `+` to set or `-` to clear: `readonly`, `hidden`, `system`, `archive`. Without a prefix the listed set becomes the whole set, so `--attrs readonly,hidden` clears anything else
- `--protection` — AmigaDOS protection bits (AFFS / PFS3 / SFS), as the letters AmigaDOS itself prints: `hsparwed`. Letters present are set, absent are clear, so `--protection rwed` is the ordinary state and `--protection rwd` marks a file unexecutable

### `chmod`

Change POSIX permission bits on an entry inside an image. Works on every filesystem that stores a Unix mode — ext, EFS, UFS, Minix, JFS, XFS, SquashFS, and HFS+ (its `HFSPlusBSDInfo`, which is how OS X carries permissions)

```
Usage: chmod <IMAGE> <PATH> <MODE>
```

**Arguments**

- `<IMAGE>` — Image reference (`path` or `path@N` for the 1-based partition index)
- `<PATH>` — Absolute path of the entry inside the filesystem
- `<MODE>` — New permission bits in octal, with or without a leading `0` (`755`, `0644`, `4755` for setuid). The file-type bits are kept

### `chown`

Change the owning uid / gid on an entry inside an image. Same filesystem support as `chmod`

```
Usage: chown <IMAGE> <PATH> <OWNER>
```

**Arguments**

- `<IMAGE>` — Image reference (`path` or `path@N` for the 1-based partition index)
- `<PATH>` — Absolute path of the entry inside the filesystem
- `<OWNER>` — New owner as `UID`, `UID:GID`, or `:GID` to change the group alone

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

### `cp`

Copy files / directory trees between two disk images without staging through the host. SRC may be a glob; DST follows `cp` semantics (into an existing directory, or rename to a target)

```
Usage: cp [OPTIONS] <SRC_IMAGE> <SRC> <DST_IMAGE> <DST>
```

**Arguments**

- `<SRC_IMAGE>` — Source image reference (`path` or `path@N` for the 1-based partition index)
- `<SRC>` — Source path or glob inside the source filesystem. Patterns containing `*`, `?`, `[`, or `{` walk the volume and copy every match
- `<DST_IMAGE>` — Destination image reference (`path` or `path@N`)
- `<DST>` — Destination path inside the destination filesystem. Copying into an existing directory (or a path ending in `/`) keeps the source basename; otherwise the destination is the literal target name

**Options**

- `-r` / `--recursive` — Recursively copy directories. Without this, directory sources / matches are skipped with a warning
- `--force` — Overwrite existing destination entries. Mutually exclusive with `--skip-existing`
- `--skip-existing` — Skip when a destination entry already exists. Mutually exclusive with `--force`. Without either, an existing destination is an error
- `--exclude` — Exclude source paths matching this glob. Repeatable. Exclude wins
- `--ignore-case` — Match the source case-insensitively regardless of its native rule
- `--case-sensitive` — Match the source case-sensitively regardless of its native rule
- `--names` — Policy for source names the destination filesystem rejects (too long / illegal characters). Default: truncate
- `--attrs` — Whether to carry FS-specific attributes (type/creator, Unix perms, DOS attribute bits, Amiga bits). Default: preserve
- `--flatten` — Collapse a source tree into the destination directory when the destination filesystem has no subdirectories (CP/M, DFS, CBM, …)
- `--parents` — Auto-create missing destination parent directories
- `--password` — Password for an encrypted source container (WinImage IMZ) or an encrypted source filesystem (APFS FileVault — the volume password or personal recovery key)
- `--src-fs-type` — Force a specific filesystem dispatch for the SOURCE (e.g. `cpm:amstrad_data`). See `get --fs-type`
- `--dst-fs-type` — Force a specific filesystem dispatch for the DESTINATION
- `--carve-full` — Scan the entire source image for recoverable text in the synthetic carve view (NDOS disks). Source-side only

### `du`

Recursive both-fork (data + resource) disk usage of paths inside a filesystem. Unlike `ls`, which reports data-fork sizes only, `du` counts the resource fork too — essential for classic-Mac apps whose code lives in a resource fork over a 0-byte data fork

```
Usage: du [OPTIONS] <IMAGE> [PATH]...
```

**Arguments**

- `<IMAGE>` — Image reference (`path` or `path@N` for the 1-based partition index)
- `<PATHS>` — One or more paths inside the filesystem (use `/` as the separator). Each is measured independently. Defaults to the volume root when none are given. A literal `/` inside a name is written `\/` (and a literal `\` as `\\`); on HFS / HFS+ a `:`-separated path also works

**Options**

- `--depth` — Report subdirectory totals down to this many levels below each PATH (like `du --max-depth`). `0` (default) prints only the totals for the path itself; `1` adds its immediate children, and so on. The full subtree is always summed regardless — depth only controls how much detail is printed
- `--json` — Emit machine-readable JSON. Shorthand for `--format json`
- `--format` — Output format
- `--password` — Password for encrypted containers / filesystems (see `ls`)
- `--inside` — For a `.zip` holding more than one disk image, the archive entry to open
- `--fs-type` — Force a specific filesystem dispatch. The main use is `cpm:<preset>` for CP/M images (which have no on-disk signature). Valid CP/M presets: `amstrad_data`, `amstrad_sys`, `amstrad_pcw`, `einstein`, `svi328_cpm`, `altair_8in`, `altair_cf`, `multicomp`, `zxplus3`. Other strings (e.g. `human68k`, `qdos`) are also accepted and forwarded to the partition_type_string dispatch
- `--carve-full` — Scan the **entire** image for recoverable text in the synthetic carve view (used for disks with no recognized filesystem — e.g. custom bootblock Amiga "NDOS" disks). By default the carve view only scans the first 10 MB. No effect on disks with a real filesystem

### `edit`

Edit a text file inside a filesystem in `$EDITOR`, converting the file's encoding and line endings on the way out and back so the editor never sees (or rewrites) its vintage form

```
Usage: edit [OPTIONS] <IMAGE> <PATH>
```

**Arguments**

- `<IMAGE>` — Image reference (`path` or `path@N` for the 1-based partition index)
- `<PATH>` — Path of the text file inside the filesystem

**Options**

- `--editor` — Editor to run. Defaults to `$VISUAL`, then `$EDITOR`, then `vi` (`notepad` on Windows)
- `--encoding` — Force the file's character encoding instead of inferring it
- `--force-substitute` — Write a best-effort replacement for characters the file's encoding cannot represent, instead of refusing
- `--line-endings` — Write the file with these line endings instead of the ones it has
- `--no-edit` — Convert without opening an editor
- `--force-binary` — Edit the file as text even if it looks binary. Almost never what you want — a round trip through an editor will not preserve arbitrary bytes
- `--fs-type` — Force a specific filesystem dispatch. The main use is `cpm:<preset>` for CP/M images (which have no on-disk signature). Valid CP/M presets: `amstrad_data`, `amstrad_sys`, `amstrad_pcw`, `einstein`, `svi328_cpm`, `altair_8in`, `altair_cf`, `multicomp`, `zxplus3`. Other strings (e.g. `human68k`, `qdos`) are also accepted and forwarded to the partition_type_string dispatch
- `--carve-full` — Scan the **entire** image for recoverable text in the synthetic carve view (used for disks with no recognized filesystem — e.g. custom bootblock Amiga "NDOS" disks). By default the carve view only scans the first 10 MB. No effect on disks with a real filesystem

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
- `--fs-type` — Force a specific filesystem dispatch. The main use is `cpm:<preset>` for CP/M images (which have no on-disk signature). Valid CP/M presets: `amstrad_data`, `amstrad_sys`, `amstrad_pcw`, `einstein`, `svi328_cpm`, `altair_8in`, `altair_cf`, `multicomp`, `zxplus3`. Other strings (e.g. `human68k`, `qdos`) are also accepted and forwarded to the partition_type_string dispatch
- `--carve-full` — Scan the **entire** image for recoverable text in the synthetic carve view (used for disks with no recognized filesystem — e.g. custom bootblock Amiga "NDOS" disks). By default the carve view only scans the first 10 MB. No effect on disks with a real filesystem

### `get`

Extract a file, directory tree, or glob match from a filesystem to the host

```
Usage: get [OPTIONS] <IMAGE> <SRC> <DST>
```

**Arguments**

- `<IMAGE>` — Image reference (`path` or `path@N` for the 1-based partition index)
- `<SRC>` — Source path or glob inside the filesystem. Patterns containing `*`, `?`, `[`, or `{` walk the volume and extract every match. Pass `--literal` to extract a single path verbatim when its name contains those characters. A literal `/` in a name is written `\/` (or use a `:`-separated path on HFS / HFS+, which also forces literal)
- `<DST>` — Destination path on the host. Single-match: the literal target file. Multi-match or directory source: a directory under which matched entries are laid out (created if it doesn't exist)

**Options**

- `-r` / `--recursive` — Recursively extract directories (literal dir source or glob match against a directory). Without this flag, matched directories are skipped with a warning
- `--exclude` — Exclude paths matching this glob. Repeatable. Exclude always wins over `--include` / the positional source
- `-L` / `--literal` — Treat the source as an exact, literal path: never interpret `*`, `?`, `[`, `]`, `{`, `}` as glob metacharacters. Use for names that contain those characters. Conflicts with `--exclude`
- `--ignore-case` — Match case-insensitively regardless of the target's native rule
- `--case-sensitive` — Match case-sensitively regardless of the target's native rule
- `--force` — Overwrite existing host files. Mutually exclusive with `--skip-existing`
- `--skip-existing` — Skip silently when a host file already exists. Mutually exclusive with `--force`. Without either flag, an existing destination is a hard error
- `--password` — Password for encrypted containers (WinImage IMZ, password-protected `.zip` disks) or an encrypted filesystem (APFS FileVault — the volume password or personal recovery key)
- `--inside` — For a `.zip` holding more than one disk image, the archive entry to open (e.g. `--inside backup.img`). Matched by exact name, then case- insensitively, then by basename. Ignored for non-zip sources
- `--fs-type` — Force a specific filesystem dispatch. The main use is `cpm:<preset>` for CP/M images (which have no on-disk signature). Valid CP/M presets: `amstrad_data`, `amstrad_sys`, `amstrad_pcw`, `einstein`, `svi328_cpm`, `altair_8in`, `altair_cf`, `multicomp`, `zxplus3`. Other strings (e.g. `human68k`, `qdos`) are also accepted and forwarded to the partition_type_string dispatch
- `--carve-full` — Scan the **entire** image for recoverable text in the synthetic carve view (used for disks with no recognized filesystem — e.g. custom bootblock Amiga "NDOS" disks). By default the carve view only scans the first 10 MB. No effect on disks with a real filesystem

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

- `--password` — Password for encrypted containers (WinImage IMZ) or an encrypted filesystem (APFS FileVault — the volume password or recovery key)
- `-L` / `--literal` — Accepted for consistency with `ls`/`get`/`rm`; `get-binhex` always treats the source as an exact literal path (it never globs), so glob metacharacters in a name are addressed verbatim with or without it. A literal `/` in a name is written `\/` (or use a `:`-separated path on HFS / HFS+)

### `grow`

Grow a disk image by `--add SIZE` of trailing zero-padding so a subsequent `partmap` edit can place a new partition

```
Usage: grow --add <ADD> <IMAGE>
```

**Arguments**

- `<IMAGE>` — Image to grow

**Options**

- `--add` — Bytes of zero-padding to add at the end (e.g. `512M`, `2G`)

### `import`

Copy a host directory tree INTO a filesystem in an image — the bulk counterpart to `put`, with no tarball needed. `--expand-archives` unpacks tar archives found in the tree instead of copying them in whole

```
Usage: import [OPTIONS] <IMAGE> <DIR> [DEST]
```

**Arguments**

- `<IMAGE>` — Image reference (`path` or `path@N` for the 1-based partition index)
- `<DIR>` — Host directory whose contents are copied in. The directory itself is not created inside the image — its *contents* land under DEST
- `<DEST>` — Destination directory inside the filesystem. Defaults to the root

**Options**

- `--expand-archives` — Unpack tar archives found in the tree into a directory named after each, instead of copying them in verbatim. Detected by content (the `ustar` magic), so IRIX `.tardist` and oddly-named archives are found and a gzipped disk image is not mistaken for one
- `--flatten-folders` — With `--expand-archives`: unpack each archive into the directory that held it rather than into a subdirectory named after it, so every archive shares one root
- `--force` — Overwrite entries that already exist at the destination. Mutually exclusive with `--skip-existing`
- `--skip-existing` — Skip entries that already exist at the destination. Mutually exclusive with `--force`
- `--no-permissions` — Ignore the host's Unix mode and ownership. Imported entries then inherit uid/gid from the directory they land in and take the filesystem's default mode, the same rule `put` follows
- `--include-appledouble` — Import macOS AppleDouble sidecars (`._*`) too. By default they are skipped as Mac metadata cruft
- `--fs-type` — Force a specific filesystem dispatch. The main use is `cpm:<preset>` for CP/M images (which have no on-disk signature). Valid CP/M presets: `amstrad_data`, `amstrad_sys`, `amstrad_pcw`, `einstein`, `svi328_cpm`, `altair_8in`, `altair_cf`, `multicomp`, `zxplus3`. Other strings (e.g. `human68k`, `qdos`) are also accepted and forwarded to the partition_type_string dispatch
- `--carve-full` — Scan the **entire** image for recoverable text in the synthetic carve view (used for disks with no recognized filesystem — e.g. custom bootblock Amiga "NDOS" disks). By default the carve view only scans the first 10 MB. No effect on disks with a real filesystem

### `inspect`

Whole-disk aggregate read-only view (partition table + per-partition summary + CHD metadata when applicable). The `idx` column is the selector: pass it back as `IMG@N`, `--partition N` or `--partitions N`

```
Usage: inspect [OPTIONS] <IMAGE>
```

**Arguments**

- `<IMAGE>` — Image path. `inspect` always reads the whole disk — there is no `@N` form. For per-partition detail use `show fs-info IMG@N`

**Options**

- `--format` — Output format
- `--password` — Password for encrypted containers (currently: WinImage IMZ, and password-protected `.zip` disks)
- `--inside` — For a `.zip` holding more than one disk image, the archive entry to open (e.g. `--inside backup.img`). Matched by exact name, then case- insensitively, then by basename. Ignored for non-zip sources

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
- `<PATH>` — Path inside the filesystem. `/` is the separator; a literal `/` in a name is written `\/`. You may instead use `:` as the separator (the native classic-Mac convention), in which case `/` is plain data — e.g. `:System Folder:Oxyd b/w`

**Options**

- `--format` — Output format. `json` is the default because the load-bearing consumer is a build script
- `-L` / `--literal` — Accepted for consistency with `ls`/`get`/`rm`; `locate` always treats the path as an exact literal path (it never globs), so glob metacharacters in a name are addressed verbatim with or without it

### `ls`

List a directory inside a filesystem

```
Usage: ls [OPTIONS] <IMAGE> [PATH]
```

**Arguments**

- `<IMAGE>` — Image reference (`path` or `path@N` for the 1-based partition index)
- `<PATH>` — Path or glob pattern inside the filesystem (use `/` as the separator). A plain path lists that directory's contents; patterns containing `*`, `?`, `[`, or `{` walk the volume and emit one line per match. Pass `--literal` to address a path verbatim when its name contains those characters

**Options**

- `--exclude` — Exclude paths matching this glob. Repeatable. Exclude always wins over `--include` / a positional path
- `-L` / `--literal` — Treat the path as an exact, literal path: never interpret `*`, `?`, `[`, `]`, `{`, `}` as glob metacharacters. Use for names that contain those characters. Conflicts with `--exclude`
- `--ignore-case` — Treat case-insensitively, regardless of the target's native rule
- `--case-sensitive` — Treat case-sensitively, regardless of the target's native rule
- `-o` / `--owner` — Show each entry's Unix permissions and owner. On a Linux/Unix image the owner ids are resolved to names via the image's own `/etc/passwd` and `/etc/group` (falling back to the raw numbers where there's no entry)
- `--password` — Password for encrypted containers (WinImage IMZ, password-protected `.zip` disks) or an encrypted filesystem (APFS FileVault — the volume password or personal recovery key)
- `--inside` — For a `.zip` holding more than one disk image, the archive entry to open (e.g. `--inside backup.img`). Matched by exact name, then case- insensitively, then by basename. Ignored for non-zip sources
- `--fs-type` — Force a specific filesystem dispatch. The main use is `cpm:<preset>` for CP/M images (which have no on-disk signature). Valid CP/M presets: `amstrad_data`, `amstrad_sys`, `amstrad_pcw`, `einstein`, `svi328_cpm`, `altair_8in`, `altair_cf`, `multicomp`, `zxplus3`. Other strings (e.g. `human68k`, `qdos`) are also accepted and forwarded to the partition_type_string dispatch
- `--carve-full` — Scan the **entire** image for recoverable text in the synthetic carve view (used for disks with no recognized filesystem — e.g. custom bootblock Amiga "NDOS" disks). By default the carve view only scans the first 10 MB. No effect on disks with a real filesystem

### `mac-scsi-bless`

Install an Apple SCSI driver + Driver Descriptor Record into an APM disk so a classic-Mac ROM (e.g. Quadra 800) registers it over SCSI. Operates in place; partition data is never moved. (This registers the driver so the ROM can read the disk — it does not change HFS boot-block behavior.)

```
Usage: mac-scsi-bless [OPTIONS] <IMAGE>
```

**Arguments**

- `<IMAGE>` — APM disk image to make SCSI-bootable, in place

**Options**

- `--driver-from` — Extract the driver from a donor Apple-formatted disk's `Apple_Driver*` partition (most faithful — carries that disk's exact boot metadata)
- `--driver` — Use a raw driver image file (advanced; `pmBootCksum` is unknown for an arbitrary driver, so it is written as 0 — see `--force-cksum-zero`)
- `--builtin-driver` — Use the bundled known-good Apple SCSI driver (this is the default when no driver source is given)
- `--force-cksum-zero` — Force `pmBootCksum = 0`. Some ROMs skip checksum verification then

### `make-bootable`

Auto-detect what a Mac disk needs to boot and apply only the missing pieces: SCSI driver + DDR (full APM disks), boot blocks (copied from a `--boot-from` donor), and a blessed System Folder. Idempotent; a flat HFV is kept flat. Works on flat HFVs and full APM disks alike

```
Usage: make-bootable [OPTIONS] <IMAGE>
```

**Arguments**

- `<IMAGE>` — Disk image to make bootable, in place

**Options**

- `--boot-from` — Bootable donor disk to copy boot blocks from, if the target lacks them (its classic-HFS volume is auto-located and `'LK'`-validated). Without it, missing boot blocks are reported but not synthesized
- `--driver-from` — For a full (APM) disk missing a SCSI driver: extract it from a donor Apple-formatted disk instead of using the bundled driver
- `--bless` — Absolute Mac path of the folder to bless (e.g. `/System Folder`). Defaults to auto-blessing a root folder named "System Folder"
- `--dry-run` — Report what would change without writing anything

### `menu`

Open the interactive terminal UI on its Backup screen. Needs an interactive terminal

```
Usage: menu
```

### `mkdir`

Create a directory inside a filesystem

```
Usage: mkdir [OPTIONS] <IMAGE> <PATH>
```

**Arguments**

- `<IMAGE>` — Image reference (`path` or `path@N` for the 1-based partition index)
- `<PATH>` — Directory path to create. The parent must exist (no `-p`-style auto-creation in Phase B). A literal `/` in the new name is written `\/`; on HFS / HFS+ a `:`-separated path also works

**Options**

- `-L` / `--literal` — Accepted for consistency with `ls`/`get`/`rm`; `mkdir` always treats the path as an exact literal path (it never globs), so glob metacharacters in a name are used verbatim with or without it

### `new`

Create a blank image, grouped by media class: `new floppy <fs>`, `new volume <fs>` (bare superfloppy), or `new hd {x68k|sgi-efs}` (partition-table-wrapped, bootable). CD-ROM images are under `optical new`; multi-partition images go through `batch`

```
Usage: new <COMMAND>
```

### `new floppy`

Blank floppy-geometry single volume (bare, no partition table): FAT / HFS and the fixed-geometry retro filesystems

```
Usage: floppy [OPTIONS] <FS> <IMAGE>
```

**Arguments**

- `<FS>` — Filesystem to format (see the per-value help above)
- `<IMAGE>` — Image file to create. Overwritten if it already exists

**Options**

- `--size` — Volume size (bytes or `K`/`M`/`G` suffixes). Ignored by the fixed-geometry filesystems. Defaults to 800K
- `--name` — Volume label/name. Defaults to `rusty-backup`
- `--block-size` — HFS allocation block size in bytes (multiple of 512). Auto when unset
- `--catalog-size` — HFS Catalog B-tree initial size in bytes. Auto when unset
- `--extents-size` — HFS Extents-overflow B-tree initial size in bytes. Auto when unset
- `--cpm-preset` — CP/M disk-parameter-block preset (required with `cpm`). One of: amstrad_data, amstrad_sys, amstrad_pcw, einstein, svi328_cpm, altair_8in, altair_cf, multicomp, zxplus3
- `--fat32` — FAT only: format FAT32 regardless of size. Without this the type comes from the capacity and only reaches FAT32 above 2 GiB, which cannot express an EFI System Partition - FAT32, and usually 100-512 MiB

### `new hd`

Partition-table-wrapped, self-bootable hard-disk image

```
Usage: hd <COMMAND>
```

### `new hd apm`

APM (Apple Partition Map) disk for classic Mac OS / PowerPC, with partitions you size and type yourself

```
Usage: apm [OPTIONS] --size <SIZE> --partition <PARTITIONS> <IMAGE>
```

**Arguments**

- `<IMAGE>` — Image file to create

**Options**

- `--size` — Total disk size (accepts `K`/`M`/`G` suffixes)
- `--partition` — A partition, repeatable, in disk order: `SIZE[:TYPE[:NAME]]`. SIZE accepts `K`/`M`/`G`, or `rest` for the remaining space (once). TYPE is a value from `partmap types`; NAME is APM/GPT only
- `--fill` — Pour an image into a partition as it is created: `N=PATH`, 1-based, repeatable. Any format the engine can read is decoded on the way in
- `--align` — Alignment for partition starts. Default 1 MiB; use 63s for DOS-era cylinder alignment on vintage machines
- `--force` — Overwrite `image` if it already exists

### `new hd gpt`

GPT (UEFI, modern macOS / Linux) disk with partitions you size and type yourself

```
Usage: gpt [OPTIONS] --size <SIZE> --partition <PARTITIONS> <IMAGE>
```

**Arguments**

- `<IMAGE>` — Image file to create

**Options**

- `--size` — Total disk size (accepts `K`/`M`/`G` suffixes)
- `--partition` — A partition, repeatable, in disk order: `SIZE[:TYPE[:NAME]]`. SIZE accepts `K`/`M`/`G`, or `rest` for the remaining space (once). TYPE is a value from `partmap types`; NAME is APM/GPT only
- `--fill` — Pour an image into a partition as it is created: `N=PATH`, 1-based, repeatable. Any format the engine can read is decoded on the way in
- `--align` — Alignment for partition starts. Default 1 MiB; use 63s for DOS-era cylinder alignment on vintage machines
- `--force` — Overwrite `image` if it already exists

### `new hd mbr`

MBR (DOS / PC) disk with partitions you size and type yourself. The partitions come out empty — fill them with `write --partition N` or format them with `reformat`

```
Usage: mbr [OPTIONS] --size <SIZE> --partition <PARTITIONS> <IMAGE>
```

**Arguments**

- `<IMAGE>` — Image file to create

**Options**

- `--size` — Total disk size (accepts `K`/`M`/`G` suffixes)
- `--partition` — A partition, repeatable, in disk order: `SIZE[:TYPE[:NAME]]`. SIZE accepts `K`/`M`/`G`, or `rest` for the remaining space (once). TYPE is a value from `partmap types`; NAME is APM/GPT only
- `--fill` — Pour an image into a partition as it is created: `N=PATH`, 1-based, repeatable. Any format the engine can read is decoded on the way in
- `--align` — Alignment for partition starts. Default 1 MiB; use 63s for DOS-era cylinder alignment on vintage machines
- `--force` — Overwrite `image` if it already exists

### `new hd rdb`

Amiga Rigid Disk Block (RDB) with partitions you size and type yourself. Cylinder-aligned from `--heads` / `--sectors`; types are AmigaDOS DosType tags (`DOS\\3`, `PFS\\3`, `SFS\\0`, ...)

```
Usage: rdb [OPTIONS] --size <SIZE> --partition <PARTITIONS> <IMAGE>
```

**Arguments**

- `<IMAGE>` — Image file to create

**Options**

- `--size` — Total disk size (accepts `K`/`M`/`G` suffixes)
- `--partition` — A partition, repeatable, in disk order: `SIZE[:TYPE[:NAME]]`. SIZE accepts `K`/`M`/`G`, or `rest` for the remaining space (once). TYPE is a value from `partmap types`; NAME is APM/GPT only
- `--fill` — Pour an image into a partition as it is created: `N=PATH`, 1-based, repeatable. Any format the engine can read is decoded on the way in
- `--align` — Alignment for partition starts. Default 1 MiB; use 63s for DOS-era cylinder alignment on vintage machines
- `--force` — Overwrite `image` if it already exists
- `--heads` — Disk geometry: heads. These tables place partitions on cylinder boundaries, so the geometry sets the default alignment
- `--sectors` — Disk geometry: sectors per track

### `new hd sgi`

SGI volume header (IRIX) with partitions you size and type yourself. Cylinder-aligned from `--heads` / `--sectors`. Unlike `sgi-efs` the partitions come out empty rather than EFS-formatted

```
Usage: sgi [OPTIONS] --size <SIZE> --partition <PARTITIONS> <IMAGE>
```

**Arguments**

- `<IMAGE>` — Image file to create

**Options**

- `--size` — Total disk size (accepts `K`/`M`/`G` suffixes)
- `--partition` — A partition, repeatable, in disk order: `SIZE[:TYPE[:NAME]]`. SIZE accepts `K`/`M`/`G`, or `rest` for the remaining space (once). TYPE is a value from `partmap types`; NAME is APM/GPT only
- `--fill` — Pour an image into a partition as it is created: `N=PATH`, 1-based, repeatable. Any format the engine can read is decoded on the way in
- `--align` — Alignment for partition starts. Default 1 MiB; use 63s for DOS-era cylinder alignment on vintage machines
- `--force` — Overwrite `image` if it already exists
- `--heads` — Disk geometry: heads. These tables place partitions on cylinder boundaries, so the geometry sets the default alignment
- `--sectors` — Disk geometry: sectors per track

### `new hd sgi-efs`

dvh-wrapped IRIX HDD: an SGI volume header + partition table wrapping a formatted EFS root partition, mountable by IRIX 5.3-6.5. Pass `--from-dir` to fill the root filesystem from a host folder in the same step; otherwise it comes out blank for `import` / `put`

```
Usage: sgi-efs [OPTIONS] <IMAGE>
```

**Arguments**

- `<IMAGE>` — Image file to create. Overwritten if it already exists

**Options**

- `--size` — Disk size (plain bytes or `K`/`KiB`/`M`/`MiB`/`G`/`GiB` suffixes, e.g. `50M`), or `auto` to size it to `--from-dir` plus filesystem overhead and headroom. Rounded up to a whole cylinder. Defaults to 50M
- `--from-dir` — Populate the root filesystem from this host directory after formatting. The directory's *contents* land at the volume root
- `--expand-archives` — With `--from-dir`: unpack tar archives found in the tree into a directory named after each, instead of copying them in verbatim. Detected by content, so IRIX `.tardist` files count
- `--flatten-folders` — With `--expand-archives`: unpack each archive into the volume root rather than a subdirectory named after it, so every archive shares one root — the shape IRIX `inst` wants. Overlapping entries are then expected, so this skips entries that already exist unless `--force`
- `--force` — With `--from-dir`: overwrite entries that already exist rather than skipping them
- `--no-permissions` — With `--from-dir`: ignore the host's Unix mode and ownership
- `--include-appledouble` — With `--from-dir`: import macOS AppleDouble sidecars (`._*`) too
- `--name` — EFS volume label (up to 6 bytes; longer is truncated). Defaults to `rusty`
- `--fs` — Root filesystem to format. Only `efs` is supported today
- `--heads` — Heads (tracks per cylinder). Must match the geometry the target drive reports over SCSI: IRIX `fx` rejects the volume header if its geometry disagrees with the drive, which stops the disk from mounting. The IRIS emulator and typical SGI SCSI HDDs report 16 heads; change this only for a drive you know reports otherwise
- `--sectors` — Sectors per track (512-byte sectors). Like `--heads`, must match the drive's reported geometry or IRIX `fx` rejects the label. Default 63 (the IRIS emulator's value; 16 × 63 = 1008-sector cylinders)
- `--inodes` — Approximate total inode count for the EFS root. The formatter scales the cylinder groups to hit roughly this many inodes. Mutually exclusive with `--bytes-per-inode`. When neither is given the density is ~1 inode/4 KiB
- `--bytes-per-inode` — EFS inode density, in bytes per inode (smaller = more inodes). Floored at one inode per 512-byte block. Mutually exclusive with `--inodes`

### `new hd sun`

Sun disk label (SMI VTOC) for SPARC Solaris / SunOS, with slices you size and tag yourself. Cylinder-aligned from `--heads` / `--sectors`; slice 2 is reserved for the whole-disk "backup" alias

```
Usage: sun [OPTIONS] --size <SIZE> --partition <PARTITIONS> <IMAGE>
```

**Arguments**

- `<IMAGE>` — Image file to create

**Options**

- `--size` — Total disk size (accepts `K`/`M`/`G` suffixes)
- `--partition` — A partition, repeatable, in disk order: `SIZE[:TYPE[:NAME]]`. SIZE accepts `K`/`M`/`G`, or `rest` for the remaining space (once). TYPE is a value from `partmap types`; NAME is APM/GPT only
- `--fill` — Pour an image into a partition as it is created: `N=PATH`, 1-based, repeatable. Any format the engine can read is decoded on the way in
- `--align` — Alignment for partition starts. Default 1 MiB; use 63s for DOS-era cylinder alignment on vintage machines
- `--force` — Overwrite `image` if it already exists
- `--heads` — Disk geometry: heads. These tables place partitions on cylinder boundaries, so the geometry sets the default alignment
- `--sectors` — Disk geometry: sectors per track

### `new hd x68k`

Sharp X68000 HDD (SASI / SCSI): X68K partition table + IPL stub + a blank or donor-cloned Human68k partition

```
Usage: x68k [OPTIONS] <IMAGE>
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

### `new hd x68k-table`

Sharp X68000 SCSI/SASI table with partitions you size yourself. Unlike `x68k` this writes only the table -- no IPL stub, no Human68k formatting -- so it is a data disk, not a bootable one

```
Usage: x68k-table [OPTIONS] --size <SIZE> --partition <PARTITIONS> <IMAGE>
```

**Arguments**

- `<IMAGE>` — Image file to create

**Options**

- `--size` — Total disk size (accepts `K`/`M`/`G` suffixes)
- `--partition` — A partition, repeatable, in disk order: `SIZE[:TYPE[:NAME]]`. SIZE accepts `K`/`M`/`G`, or `rest` for the remaining space (once). TYPE is a value from `partmap types`; NAME is APM/GPT only
- `--fill` — Pour an image into a partition as it is created: `N=PATH`, 1-based, repeatable. Any format the engine can read is decoded on the way in
- `--align` — Alignment for partition starts. Default 1 MiB; use 63s for DOS-era cylinder alignment on vintage machines
- `--force` — Overwrite `image` if it already exists

### `new volume`

Blank bare single volume of arbitrary size (a "superfloppy"): the larger filesystems (NTFS, ext, HFS+, EFS, AFFS, …). No partition table

```
Usage: volume [OPTIONS] <FS> <IMAGE>
```

**Arguments**

- `<FS>` — Filesystem to format (see the per-value help above)
- `<IMAGE>` — Image file to create. Overwritten if it already exists

**Options**

- `--size` — Volume size (bytes or `K`/`M`/`G` suffixes). Defaults to 800K
- `--name` — Volume label/name. Defaults to `rusty-backup`
- `--block-size` — HFS/HFS+ allocation block size in bytes (multiple of 512). Auto when unset
- `--catalog-size` — HFS Catalog B-tree initial size in bytes. Auto when unset
- `--extents-size` — HFS Extents-overflow B-tree initial size in bytes. Auto when unset
- `--case-sensitive` — HFS+ only: format a case-sensitive (HFSX) volume
- `--min-catalog` — HFS+ only: minimum catalog B-tree size in bytes (a floor)
- `--fat32` — FAT only: format FAT32 regardless of size. Without this the type is picked from the capacity and only reaches FAT32 above 2 GiB, which cannot express an EFI System Partition - FAT32, and usually 100-512 MiB
- `--affs-variant` — AFFS variant byte (0=OFS, 1=FFS, 2=OFS+intl, 3=FFS+intl, 4=OFS+dircache, 5=FFS+dircache). Defaults to 1 (FFS)
- `--inodes` — EFS only: approximate total inode count. Mutually exclusive with `--bytes-per-inode`
- `--bytes-per-inode` — EFS only: inode density in bytes per inode (smaller = more inodes)
- `--cluster-size` — NTFS only: cluster (allocation unit) size, e.g. `4K`, `64K`. Auto when unset
- `--sector-size` — NTFS only: bytes per sector — 512, 1024, 2048 or 4096. Defaults to 512

### `optical`

Optical-media verbs (drives / rip / convert / browse / extract)

```
Usage: optical <COMMAND>
```

### `optical boot`

Work with El Torito boot images (extract / replace)

```
Usage: boot <COMMAND>
```

### `optical boot extract`

Extract a boot image to a file — then inspect or edit it with the disk-image verbs, and put it back with `optical boot replace`

```
Usage: extract [OPTIONS] --to <TO> <SOURCE>
```

**Arguments**

- `<SOURCE>` — Bootable optical disc image (.iso, …)

**Options**

- `--to` — Destination file for the extracted boot image
- `--index` — Which boot entry to extract (default 0; see `optical info`)

### `optical boot replace`

Replace a boot image with the bytes of a (edited) disk-image file. Raw `.iso` only; same-size replaces in place, a grown image relocates

```
Usage: replace [OPTIONS] --from <FROM> <SOURCE>
```

**Arguments**

- `<SOURCE>` — Bootable optical image to edit — raw `.iso` only

**Options**

- `--from` — Disk-image file whose bytes become the new boot image
- `--index` — Which boot entry to replace (default 0)
- `--media` — Override the emulation/media type (default: keep the entry's current one). One of `floppy1.2` / `floppy1.44` / `floppy2.88` / `no-emulation` / `harddisk`

### `optical browse`

List the file tree on an optical disc image

```
Usage: browse [OPTIONS] <SOURCE>
```

**Arguments**

- `<SOURCE>` — Optical disc image (.iso, .cue, .chd)

**Options**

- `--format` — Output format. `text` (default) prints the human file tree unchanged; `json` / `yaml` emit a machine-readable, deterministically path-sorted listing
- `--hash` — Per-file content hash to attach to each file entry. Structured output only (`--format json`). Currently only `sha256`
- `--filesystem` — Which filesystem to browse on a hybrid Mac/PC disc. `auto` (default) opens the primary (ISO 9660); `hfs` opens the Apple HFS side; `iso` forces the ISO 9660 tree. See `optical info` to see what a disc carries

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

### `optical drives`

List connected physical optical drives and their device paths

```
Usage: drives [OPTIONS]
```

**Options**

- `--remote` — Also query these daemons for their optical drives (repeatable), e.g. `--remote mister.local:7341`. Remote rows print an `rb://...` device arg you can pass straight to `optical rip --device`

### `optical du`

Recursive both-fork (data + resource) disk usage of paths on an optical disc image — the disc counterpart of the top-level `du` verb, for hybrid Mac discs whose apps keep code in the resource fork

```
Usage: du [OPTIONS] <SOURCE> [PATH]...
```

**Arguments**

- `<SOURCE>` — Optical disc image (.iso, .cue, .chd)
- `<PATHS>` — One or more paths inside the disc filesystem (use `/` as the separator). Defaults to the volume root when none are given

**Options**

- `--depth` — Report subdirectory totals down to this many levels below each PATH (`0`, the default, prints only the totals for the path itself). The full subtree is always summed regardless
- `--json` — Emit machine-readable JSON. Shorthand for `--format json`
- `--format` — Output format
- `--filesystem` — Which filesystem to measure on a hybrid Mac/PC disc. `auto` (default) opens the primary (ISO 9660); `hfs` opens the Apple HFS side — the one carrying resource forks. See `optical info` for what a disc holds

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
- `--on-collision` — What to do when two names on a **case-sensitive** disc (UFS, NeXT, Rock Ridge, …) collide only by case on a **case-insensitive** destination (e.g. macOS). Defaults to `rename`, or `[optical] on-collision` from the config. Ignored when the destination is case-sensitive — everything extracts verbatim there
- `--filesystem` — Which filesystem to extract from on a hybrid Mac/PC disc. `auto` (default) uses the primary (ISO 9660); `hfs` extracts the Apple HFS side; `iso` forces the ISO 9660 tree. See `optical info`

### `optical info`

Print volume-level metadata for an optical disc image (leniently)

```
Usage: info [OPTIONS] <SOURCE>
```

**Arguments**

- `<SOURCE>` — Optical disc image (.iso, .cue, .chd)

**Options**

- `--format` — Output format: `text` (default), `json`, or `yaml`

### `optical new`

Create a blank CD-ROM disc image

```
Usage: new <COMMAND>
```

### `optical new sgi-efs`

IRIX EFS CD-ROM (`.iso`): an SGI volume header with the EFS filesystem in slot 7 (typed SYSV, the IRIX EFS-CD convention) and CD geometry. Mounts on IRIX with `mount -t efs <dev>s7`. Pass `--from-dir` to fill it from a host folder in the same step (`--size auto` then sizes the disc to fit, and `--expand-archives --flatten-folders` unpacks a `.tardist` set into one `inst`-ready root); otherwise it comes out blank for `import` / `put`

```
Usage: sgi-efs [OPTIONS] <IMAGE>
```

**Arguments**

- `<IMAGE>` — Image file to create (conventionally `.iso`). Overwritten if it exists

**Options**

- `--size` — Disc size (plain bytes or `K`/`M`/`G` suffixes, e.g. `600M`), or `auto` to size it to `--from-dir` plus filesystem overhead and headroom. Rounded up to a whole 32-sector CD cylinder. Defaults to 600M (a CD-R); keep an explicit size at or below your target media (~650-700 MiB for a CD), and use `auto` for an image you intend to mount rather than burn
- `--from-dir` — Populate the disc from this host directory after formatting it. The directory's *contents* land at the volume root
- `--expand-archives` — With `--from-dir`: unpack tar archives found in the tree into a directory named after each, instead of copying them in verbatim. Detected by content, so IRIX `.tardist` files count
- `--flatten-folders` — With `--expand-archives`: unpack each archive into the volume root rather than a subdirectory named after it, so every archive shares one root. This is the shape IRIX `inst` wants — point it at one directory holding every `.tardist`'s product images instead of re-pointing it per archive. Overlapping entries are then expected (SGI freeware tardists all ship the same `fw_common*` product), so this skips entries that already exist unless `--force` is given
- `--force` — With `--from-dir`: overwrite entries that already exist rather than skipping them. Only meaningful alongside `--flatten-folders`, where archives can legitimately carry the same entry
- `--no-permissions` — With `--from-dir`: ignore the host's Unix mode and ownership
- `--include-appledouble` — With `--from-dir`: import macOS AppleDouble sidecars (`._*`) too
- `--name` — EFS volume label (up to 6 bytes; longer is truncated). Defaults to `rusty`
- `--inodes` — Approximate total inode count for the EFS filesystem. Mutually exclusive with `--bytes-per-inode`. Default density is ~1 inode/4 KiB; real IRIX CDs are sparser (~32 KiB/inode), so pass a larger `--bytes-per-inode` (or fewer `--inodes`) if you only have a handful of large files
- `--bytes-per-inode` — EFS inode density, in bytes per inode (smaller = more inodes). Floored at one inode per 512-byte block. Mutually exclusive with `--inodes`

### `optical rip`

Rip a physical CD/DVD drive to a disk image file

```
Usage: rip [OPTIONS] --device <DEVICE> --output <OUTPUT>
```

**Options**

- `--device` — Source drive: a local path (e.g. `/dev/sr0`, `disk6`, `\\.\E:`) or a remote daemon's drive as `rb://host:port/dev/sr0` (the daemon issues the SCSI reads; this side does the encoding). `rb-cli optical drives` lists local drives
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
- `--type-byte` — MBR type byte (decimal or `0xNN`). Ignored for non-MBR tables. See `partmap types --table mbr`
- `--type-string` — GPT type GUID string, or APM type string (`"Apple_HFS"`, etc.). See `partmap types --table gpt|apm`
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

- `--type-byte` — MBR type byte (decimal or `0xNN`). See `partmap types`
- `--type-string` — GPT type GUID / APM type string. See `partmap types`

### `partmap types`

List the well-known partition type values for a table flavor

```
Usage: types [OPTIONS]
```

**Options**

- `--table` — Table flavor to list types for. Omit to read it from an image
- `--image` — Image whose partition table decides which list to print

### `put`

Copy a host file (or zero-fill / write boot blocks) into a filesystem

```
Usage: put [OPTIONS] <IMAGE> [HOST_FILE] [DST]
```

**Arguments**

- `<IMAGE>` — Image reference (`path` or `path@N` for the 1-based partition index)
- `<HOST_FILE>` — Host file to copy. Required when not using `--zero` or `--boot`
- `<DST>` — Destination path inside the filesystem (cp-like positional). A literal `/` in the name is written `\/`; on HFS / HFS+ a `:`-separated path also works (so `/` is plain data)

**Options**

- `-L` / `--literal` — Accepted for consistency with `ls`/`get`/`rm`; `put` always treats the destination as an exact literal path (it never globs), so glob metacharacters in a name are used verbatim with or without it
- `--zero` — Pre-allocate N zero bytes instead of copying a host file. Pair with `--dst`
- `--dst` — Explicit destination flag; use this with `--zero` where the positional `DST` slot is awkward
- `--boot` — Write the 1024-byte boot-block region of the image verbatim. HFS-only today
- `--boot-from` — Copy the 1024-byte boot-block region from a donor disk that already boots (`path` or `path@N`), instead of from a raw file. The donor's classic-HFS volume is auto-located (flat `.hfv`/`.dsk` at byte 0, or an `Apple_HFS` partition) and its `'LK'` signature validated. The region is written to the *target partition's* first sector, so this works on a flat HFV and on the HFS partition of a full (APM) disk alike — target the HFS partition with `IMG@N` (the DDR / partition map / drivers ahead of it are never touched). Use it to make a bare HFS volume (e.g. an edited infinite-mac disk) bootable. HFS-only today
- `--type` — 4-character type code (HFS / HFS+ / ProDOS). Falls back to `[put] type` from the config file, then — on HFS / HFS+ / MFS — to the file extension (same list as the GUI's type/creator picker), and finally to `BINA` for names the list doesn't recognize
- `--creator` — 4-character creator code (HFS / HFS+ only). Falls back to `[put] creator` from the config file, then to the file extension, and finally to `????`
- `--force` — Overwrite an existing entry at the destination path
- `--no-preserve-meta` — Give the replacement fresh metadata instead of the replaced file's
- `--mode` — Unix permission bits for the new file, as octal (e.g. `755`, `0644`). Unix filesystems only (ext / UFS / XFS / EFS / Minix / SquashFS); ignored on FAT / HFS / exFAT, which have no such concept
- `--uid` — Owner UID for the new file. Unix filesystems only
- `--gid` — Owning GID for the new file. Unix filesystems only. Same precedence as `--uid`
- `--print-offset` — After writing the file, also print the same JSON envelope `locate` would have produced — absolute byte offset, length, fragmented flag. One-shot for build scripts that need to patch disk offsets immediately after placing a payload. HFS-only, matches the locate verb's scope; ignored (with a warning) for the `--zero` and `--boot` shapes since there's no host file to describe
- `--fs-type` — Force a specific filesystem dispatch. The main use is `cpm:<preset>` for CP/M images (which have no on-disk signature). Valid CP/M presets: `amstrad_data`, `amstrad_sys`, `amstrad_pcw`, `einstein`, `svi328_cpm`, `altair_8in`, `altair_cf`, `multicomp`, `zxplus3`. Other strings (e.g. `human68k`, `qdos`) are also accepted and forwarded to the partition_type_string dispatch
- `--carve-full` — Scan the **entire** image for recoverable text in the synthetic carve view (used for disks with no recognized filesystem — e.g. custom bootblock Amiga "NDOS" disks). By default the carve view only scans the first 10 MB. No effect on disks with a real filesystem

### `put-binhex`

Decode a BinHex 4.0 (.hqx) file and write it (both forks + Finder info) into a filesystem

```
Usage: put-binhex [OPTIONS] <IMAGE> <HOST_FILE>
```

**Arguments**

- `<IMAGE>` — Image reference (`path` or `path@N`)
- `<HOST_FILE>` — BinHex 4.0 (`.hqx`) file on the host

**Options**

- `--dst-dir` — Destination directory inside the filesystem (`/` for root). The filename comes from the BinHex header. Defaults to `/`. A literal `/` in a directory name is written `\/`; on HFS / HFS+ a `:`-separated path also works (so `/` is plain data). Defaults to `/`
- `--rename` — Override the filename from the BinHex header
- `--force` — Overwrite an existing entry at the destination path
- `--clear-inited` — Clear the `hasBeenInited` Finder flag (0x0100) on the written file. Use when injecting an app onto a fresh disk so the Finder re-reads its `BNDL` and registers real icons (a file copied with `hasBeenInited` already set is treated as already-catalogued, so it shows a generic icon until a desktop rebuild). Mirrors what a MacBinary install does to byte 73

### `put-macbinary`

Put a MacBinary I / II archive: both forks + full Finder info in one shot (HFS; on MFS both forks + type/creator, extended Finder flags/dates skipped)

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
- `<PATH>` — Path or glob pattern inside the filesystem. Patterns containing `*`, `?`, `[`, or `{` walk the volume and delete every match. Pass `--literal` to delete a single path verbatim when its name contains those characters. A literal `/` in a name is written `\/` (or use a `:`-separated path on HFS / HFS+, which also forces literal)

**Options**

- `-r` / `--recursive` — Recursively delete directories (matches will include directories without this flag, but they get rejected unless --recursive)
- `--exclude` — Exclude paths matching this glob from deletion. Repeatable. Exclude always wins over the positional pattern
- `-L` / `--literal` — Treat the path as an exact, literal path: never interpret `*`, `?`, `[`, `]`, `{`, `}` as glob metacharacters. Use for names that contain those characters. Conflicts with `--exclude`
- `--ignore-case` — Match case-insensitively regardless of the target's native rule
- `--case-sensitive` — Match case-sensitively regardless of the target's native rule
- `--fs-type` — Force a specific filesystem dispatch. The main use is `cpm:<preset>` for CP/M images (which have no on-disk signature). Valid CP/M presets: `amstrad_data`, `amstrad_sys`, `amstrad_pcw`, `einstein`, `svi328_cpm`, `altair_8in`, `altair_cf`, `multicomp`, `zxplus3`. Other strings (e.g. `human68k`, `qdos`) are also accepted and forwarded to the partition_type_string dispatch
- `--carve-full` — Scan the **entire** image for recoverable text in the synthetic carve view (used for disks with no recognized filesystem — e.g. custom bootblock Amiga "NDOS" disks). By default the carve view only scans the first 10 MB. No effect on disks with a real filesystem

### `serve`

Run the network daemon so a remote `rb-cli` (or the GUI / TUI Commander) can browse, read, and write files inside images this host holds (`rb://host:port/img@N`) and on its host filesystem. Writable under the serve root by default; pass `--read-only` for a browse-only daemon. See docs/remote_transfer_plan.md

```
Usage: serve [OPTIONS] [COMMAND]
```

**Options**

- `--bind` — Address to bind, `host:port`. Default binds all interfaces on the rusty-backup port (7341). Ignored by the `service` / `setup` subcommands (those read `rb-daemon.ini`)
- `--root` — Root directory images are served from. Every `rb://` path a client opens is sandboxed under this directory
- `--staging-dir` — Directory for per-session upload staging blobs (write path). Defaults to the system temp dir. On a MiSTer point this at a roomy writable mount, never tmpfs — large uploads would fill RAM
- `--read-only` — Serve reads only: clients can browse and copy *out* of the daemon, but every write (image edit, restore-to-target, host upload / mkdir) is refused. Without this the daemon is writable under its serve root

### `serve service`

Manage the boot service (start/stop/restart/status/install/uninstall)

```
Usage: service <ACTION>
```

**Arguments**

- `<ACTION>` — What to do with the daemon service

### `serve setup`

Open the interactive setup console (the MiSTer Scripts-menu screen)

```
Usage: setup
```

### `setrsrc`

Write the resource fork of an existing HFS / HFS+ / MFS file from a host file

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

Print filesystem-level metadata (type, volume label, used / free space) for any filesystem the engine can open

```
Usage: fs-info [OPTIONS] <IMAGE>
```

**Arguments**

- `<IMAGE>` — 

**Options**

- `--format` — 

### `show partmap`

Print the partition table of a disk image (APM-only today), including the Driver Descriptor Record's driver map and each entry's boot fields

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

### `squashfs`

SquashFS edits with an explicit size budget (`plan` / `put` / `rm`). The format has no in-place write, so committing rebuilds the whole image and its final size can only be bounded, never predicted

```
Usage: squashfs <COMMAND>
```

### `squashfs create`

Build a new SquashFS image from a host directory, as `mksquashfs` does

```
Usage: create [OPTIONS] <DIR> <IMAGE>
```

**Arguments**

- `<DIR>` — Host directory to pack. Its *contents* become the image root, like `mksquashfs DIR IMAGE` (the directory itself is not a top-level entry)
- `<IMAGE>` — Image file to create. Overwritten if it already exists

**Options**

- `--compressor` — Compressor for the new image. `mksquashfs` defaults to gzip; we match it
- `--block-size` — Data block size (a power of two, 4 KiB..=1 MiB). `mksquashfs`'s default is 128 KiB

### `squashfs plan`

Report what the image occupies, what it may grow into, and how well it compressed — the numbers a size budget is chosen from. Writes nothing

```
Usage: plan <IMAGE>
```

**Arguments**

- `<IMAGE>` — Image reference (`path` or `path@N`)

### `squashfs put`

Copy a host file into the image, rebuilding it within a size budget

```
Usage: put [OPTIONS] <IMAGE> [HOST_FILE] [DST]
```

**Arguments**

- `<IMAGE>` — Image reference (`path` or `path@N` for the 1-based partition index)
- `<HOST_FILE>` — Host file to copy. Required when not using `--zero` or `--boot`
- `<DST>` — Destination path inside the filesystem (cp-like positional). A literal `/` in the name is written `\/`; on HFS / HFS+ a `:`-separated path also works (so `/` is plain data)

**Options**

- `-L` / `--literal` — Accepted for consistency with `ls`/`get`/`rm`; `put` always treats the destination as an exact literal path (it never globs), so glob metacharacters in a name are used verbatim with or without it
- `--zero` — Pre-allocate N zero bytes instead of copying a host file. Pair with `--dst`
- `--dst` — Explicit destination flag; use this with `--zero` where the positional `DST` slot is awkward
- `--boot` — Write the 1024-byte boot-block region of the image verbatim. HFS-only today
- `--boot-from` — Copy the 1024-byte boot-block region from a donor disk that already boots (`path` or `path@N`), instead of from a raw file. The donor's classic-HFS volume is auto-located (flat `.hfv`/`.dsk` at byte 0, or an `Apple_HFS` partition) and its `'LK'` signature validated. The region is written to the *target partition's* first sector, so this works on a flat HFV and on the HFS partition of a full (APM) disk alike — target the HFS partition with `IMG@N` (the DDR / partition map / drivers ahead of it are never touched). Use it to make a bare HFS volume (e.g. an edited infinite-mac disk) bootable. HFS-only today
- `--type` — 4-character type code (HFS / HFS+ / ProDOS). Falls back to `[put] type` from the config file, then — on HFS / HFS+ / MFS — to the file extension (same list as the GUI's type/creator picker), and finally to `BINA` for names the list doesn't recognize
- `--creator` — 4-character creator code (HFS / HFS+ only). Falls back to `[put] creator` from the config file, then to the file extension, and finally to `????`
- `--force` — Overwrite an existing entry at the destination path
- `--no-preserve-meta` — Give the replacement fresh metadata instead of the replaced file's
- `--mode` — Unix permission bits for the new file, as octal (e.g. `755`, `0644`). Unix filesystems only (ext / UFS / XFS / EFS / Minix / SquashFS); ignored on FAT / HFS / exFAT, which have no such concept
- `--uid` — Owner UID for the new file. Unix filesystems only
- `--gid` — Owning GID for the new file. Unix filesystems only. Same precedence as `--uid`
- `--print-offset` — After writing the file, also print the same JSON envelope `locate` would have produced — absolute byte offset, length, fragmented flag. One-shot for build scripts that need to patch disk offsets immediately after placing a payload. HFS-only, matches the locate verb's scope; ignored (with a warning) for the `--zero` and `--boot` shapes since there's no host file to describe
- `--fs-type` — Force a specific filesystem dispatch. The main use is `cpm:<preset>` for CP/M images (which have no on-disk signature). Valid CP/M presets: `amstrad_data`, `amstrad_sys`, `amstrad_pcw`, `einstein`, `svi328_cpm`, `altair_8in`, `altair_cf`, `multicomp`, `zxplus3`. Other strings (e.g. `human68k`, `qdos`) are also accepted and forwarded to the partition_type_string dispatch
- `--carve-full` — Scan the **entire** image for recoverable text in the synthetic carve view (used for disks with no recognized filesystem — e.g. custom bootblock Amiga "NDOS" disks). By default the carve view only scans the first 10 MB. No effect on disks with a real filesystem
- `--size` — Ceiling on the rebuilt image, e.g. `512M`. `fit` accepts whatever the rebuild produces. Omitted, the container decides: a bare `.squashfs` grows freely, a partition-hosted image may not outgrow its partition
- `--grow` — Allow the rebuilt image to exceed its *current* size by at most this much, e.g. `64M`. Resolved against the image once it is opened

### `squashfs rm`

Delete a path from the image, rebuilding it within a size budget

```
Usage: rm [OPTIONS] <IMAGE> <PATH>
```

**Arguments**

- `<IMAGE>` — Image reference (`path` or `path@N` for the 1-based partition index)
- `<PATH>` — Path or glob pattern inside the filesystem. Patterns containing `*`, `?`, `[`, or `{` walk the volume and delete every match. Pass `--literal` to delete a single path verbatim when its name contains those characters. A literal `/` in a name is written `\/` (or use a `:`-separated path on HFS / HFS+, which also forces literal)

**Options**

- `-r` / `--recursive` — Recursively delete directories (matches will include directories without this flag, but they get rejected unless --recursive)
- `--exclude` — Exclude paths matching this glob from deletion. Repeatable. Exclude always wins over the positional pattern
- `-L` / `--literal` — Treat the path as an exact, literal path: never interpret `*`, `?`, `[`, `]`, `{`, `}` as glob metacharacters. Use for names that contain those characters. Conflicts with `--exclude`
- `--ignore-case` — Match case-insensitively regardless of the target's native rule
- `--case-sensitive` — Match case-sensitively regardless of the target's native rule
- `--fs-type` — Force a specific filesystem dispatch. The main use is `cpm:<preset>` for CP/M images (which have no on-disk signature). Valid CP/M presets: `amstrad_data`, `amstrad_sys`, `amstrad_pcw`, `einstein`, `svi328_cpm`, `altair_8in`, `altair_cf`, `multicomp`, `zxplus3`. Other strings (e.g. `human68k`, `qdos`) are also accepted and forwarded to the partition_type_string dispatch
- `--carve-full` — Scan the **entire** image for recoverable text in the synthetic carve view (used for disks with no recognized filesystem — e.g. custom bootblock Amiga "NDOS" disks). By default the carve view only scans the first 10 MB. No effect on disks with a real filesystem
- `--size` — Ceiling on the rebuilt image, e.g. `512M`. `fit` accepts whatever the rebuild produces. Omitted, the container decides: a bare `.squashfs` grows freely, a partition-hosted image may not outgrow its partition
- `--grow` — Allow the rebuilt image to exceed its *current* size by at most this much, e.g. `64M`. Resolved against the image once it is opened

### `squashfs verify`

Structurally verify an image: walk every inode, dirent and data block, decompressing everything. Not an fsck — SquashFS has no checksums, so this finds broken structure, not altered content

```
Usage: verify <IMAGE>
```

**Arguments**

- `<IMAGE>` — Image reference (`path` or `path@N`)

### `tar`

Archive a filesystem (or a subtree) to a single `.tar.gz` / `.tar.zst` / `.tar`. Preserves exact case-sensitive names and real symlinks, so extracting on a case-insensitive host won't clobber files that differ only in case

```
Usage: tar [OPTIONS] <IMAGE> <SRC> <OUT>
```

**Arguments**

- `<IMAGE>` — Image reference (`path` or `path@N` for the 1-based partition index)
- `<SRC>` — Source path inside the filesystem to archive — a directory (archived recursively) or a single file. Use `/` for the whole volume
- `<OUT>` — Output archive path. Compression is inferred from the extension (`.tar` = none, `.tar.zst` = zstd, otherwise gzip) unless one of `--gzip` / `--zstd` / `--no-compress` is given

**Options**

- `--exclude` — Exclude entries whose path matches this glob (a directory match prunes its whole subtree). Repeatable
- `--gzip` — Force gzip (`.tar.gz`)
- `--zstd` — Force zstd (`.tar.zst`)
- `--no-compress` — Force no compression (`.tar`)
- `--force` — Overwrite OUT if it already exists
- `--ignore-case` — Match `--exclude` globs case-insensitively (default follows the filesystem's native rule)
- `--case-sensitive` — Match `--exclude` globs case-sensitively (default follows the filesystem's native rule)
- `--password` — Password for encrypted containers (WinImage IMZ, password-protected `.zip` disks) or an encrypted filesystem (APFS FileVault — the volume password or personal recovery key)
- `--inside` — For a `.zip` holding more than one disk image, the archive entry to open (e.g. `--inside backup.img`). Ignored for non-zip sources
- `--fs-type` — Force a specific filesystem dispatch. The main use is `cpm:<preset>` for CP/M images (which have no on-disk signature). Valid CP/M presets: `amstrad_data`, `amstrad_sys`, `amstrad_pcw`, `einstein`, `svi328_cpm`, `altair_8in`, `altair_cf`, `multicomp`, `zxplus3`. Other strings (e.g. `human68k`, `qdos`) are also accepted and forwarded to the partition_type_string dispatch
- `--carve-full` — Scan the **entire** image for recoverable text in the synthetic carve view (used for disks with no recognized filesystem — e.g. custom bootblock Amiga "NDOS" disks). By default the carve view only scans the first 10 MB. No effect on disks with a real filesystem

### `terminal`

Open an interactive rb-cli shell (rustyline-based REPL)

```
Usage: terminal
```

### `tui`

Launch the full-screen terminal UI (preview): a menu-driven ratatui app that runs anywhere rusty-backup does, including serial consoles and vintage terminals. Needs an interactive terminal

```
Usage: tui
```

### `untar`

Import a `.tar.gz` / `.tar.zst` / `.tar` archive's contents INTO a filesystem in an image (the inverse of `tar`). Recreates the tree, streams files in, and recreates symlinks where the target FS supports them

```
Usage: untar [OPTIONS] <IMAGE> <ARCHIVE> [DEST]
```

**Arguments**

- `<IMAGE>` — Image reference (`path` or `path@N` for the 1-based partition index)
- `<ARCHIVE>` — Host archive to import (`.tar.gz` / `.tar.zst` / `.tar`; the compression is detected from the file's contents, not its name)
- `<DEST>` — Destination directory inside the filesystem. Defaults to the root

**Options**

- `--force` — Overwrite entries that already exist at the destination. Mutually exclusive with `--skip-existing`
- `--skip-existing` — Skip entries that already exist at the destination. Mutually exclusive with `--force`
- `--no-permissions` — Ignore the archive's Unix mode and ownership. Imported entries then inherit uid/gid from the directory they land in and take the filesystem's default mode, the same rule `put` follows
- `--include-appledouble` — Import macOS AppleDouble sidecars (`._*`) too. By default they are skipped as Mac metadata cruft
- `--fs-type` — Force a specific filesystem dispatch. The main use is `cpm:<preset>` for CP/M images (which have no on-disk signature). Valid CP/M presets: `amstrad_data`, `amstrad_sys`, `amstrad_pcw`, `einstein`, `svi328_cpm`, `altair_8in`, `altair_cf`, `multicomp`, `zxplus3`. Other strings (e.g. `human68k`, `qdos`) are also accepted and forwarded to the partition_type_string dispatch
- `--carve-full` — Scan the **entire** image for recoverable text in the synthetic carve view (used for disks with no recognized filesystem — e.g. custom bootblock Amiga "NDOS" disks). By default the carve view only scans the first 10 MB. No effect on disks with a real filesystem

### `update`

Check for a newer release and (when built with `--features tui-update`) self-update. Without that feature it reports that updates weren't compiled in and prints the releases URL, exiting non-zero. Pass `--apply` to download and replace this binary in place

```
Usage: update [OPTIONS]
```

**Options**

- `--apply` — After checking, download the newer release and replace this binary in place (requires `--features tui-update`). Without it, `update` only reports what's available. On macOS/Linux this swaps the running `rb-cli` via a temp-file + rename; on Windows it uses the self-replace path

### `write`

Stream an image file onto a block device

```
Usage: write [OPTIONS] <IMAGE> <DEVICE>
```

**Arguments**

- `<IMAGE>` — Source image file
- `<DEVICE>` — Destination block-device path: - Linux: `/dev/sdX` or `/dev/nvmeXnY` - macOS: `/dev/diskN` / `/dev/rdiskN` - Windows: `"\\.\PhysicalDriveN"` (quote for PowerShell)

**Options**

- `--partition` — Write into this 1-based partition instead of the whole disk. The partition's own bounds cap the write; the rest of the disk, including the partition table, is left untouched
- `--yes` — Required confirmation. Skips the prompt but never the safety summary printed on stderr
- `--write-to-system-disk` — Allow writing to the system boot disk (refused by default)

### `xattr`

Read or edit a file's extended attributes (`list` / `set` / `rm`) — the scriptable form of the GUI and TUI File Info panel. SquashFS today; other xattr-bearing filesystems as their write side lands

```
Usage: xattr <COMMAND>
```

### `xattr list`

List the extended attributes on a file

```
Usage: list <IMAGE> <PATH>
```

**Arguments**

- `<IMAGE>` — Image reference (`path` or `path@N`)
- `<PATH>` — Absolute path of the file inside the filesystem

### `xattr rm`

Remove one extended attribute

```
Usage: rm <IMAGE> <PATH> <NAME>
```

**Arguments**

- `<IMAGE>` — Image reference (`path` or `path@N`)
- `<PATH>` — Absolute path of the file inside the filesystem
- `<NAME>` — Fully-qualified attribute name to remove

### `xattr set`

Set (or replace) one extended attribute

```
Usage: set [OPTIONS] <IMAGE> <PATH> <NAME> [VALUE]
```

**Arguments**

- `<IMAGE>` — Image reference (`path` or `path@N`)
- `<PATH>` — Absolute path of the file inside the filesystem
- `<NAME>` — Fully-qualified attribute name, including its namespace prefix — `user.` / `trusted.` / `security.` / `system.` (e.g. `security.capability`). A name without one cannot be stored and is refused
- `<VALUE>` — Attribute value. A `0x`-prefixed string is decoded as raw hex bytes (what a capability struct needs); anything else is stored as its UTF-8 bytes. Mutually exclusive with `--value-file`

**Options**

- `--value-file` — Read the value verbatim from a host file instead, for a binary value too awkward to hex-encode on a command line

