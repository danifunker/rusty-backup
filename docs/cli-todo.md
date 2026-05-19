# CLI surface plan

Planning doc for expanding `rb-cli` (renamed from `rusty-backup-cli`)
from the current `api …` scratch namespace to something that mirrors
the GUI's feature set without nesting commands four levels deep.

### Binary rename

`rusty-backup-cli` → `rb-cli`. Update `[[bin]]` block in `Cargo.toml`
(`name = "rb-cli"`), the install manifests (`.deb`, `.dmg`, AppImage),
and CI artifact names. Keep `rusty-backup-cli` available as a symlink
or shell alias for one release so existing scripts keep working.

## Goals

1. **Short commands.** `rb-cli put IMG HOST MAC` beats
   `rb-cli api hfs put IMG HOST MAC` for every interactive use.
2. **Predictable verbs.** A user who knows the GUI tabs (Backup, Restore,
   Inspect, Optical) should be able to guess the top-level verb.
3. **Filesystem-agnostic where possible.** `ls`, `put`, `get`, `rm`,
   `fsck` should work the same way on HFS, FAT, NTFS, AFFS, PFS3, SFS,
   EFS, etc. — the tool detects the filesystem.
4. **Partition selection inline.** Drop `--partition N`; use `IMG@N`
   syntax instead. Less typing, less ambiguity.
5. **Don't expose every GUI dialog.** Bulk Convert, Expand HFS, the
   resize popup, etc. — only what is actually scriptable / useful from a
   terminal. Things that need interactive review stay in the GUI.

## Survey of GUI codepaths

Grouped by GUI tab, with a CLI-priority annotation:

  - **(must)** — clearly scriptable, common, no good GUI substitute for
    automation.
  - **(should)** — useful but lower priority.
  - **(skip)** — interactive-only, exposes little value on the CLI.

### Backup tab

| GUI action | CLI priority |
|---|---|
| Pick source (device or image) + partition table auto-detect | must |
| Select/deselect partitions to back up | must |
| Per-partition defrag toggle | should |
| Sector-by-sector mode toggle | should |
| Compression choice (Zstd / Raw / VHD / CHD) | must |
| CHD codec + hunk-size tuning | should |
| Checksum type (SHA256 / CRC32) | must |
| Split-archive size | should |
| Backup name | must |
| Whole-disk VHD export with per-partition sizing | should |
| Per-partition VHD/CHD export | should |
| Cancel mid-run | skip (Ctrl-C suffices) |

### Restore tab

| GUI action | CLI priority |
|---|---|
| Restore full disk from backup folder | must |
| Restore single partition from image or backup | should |
| New-disk-from-image (synthesize PT) | should |
| Per-partition size mode (Original/Min/Custom) | must |
| Alignment mode (DosTraditional/Modern1MB/Custom) | should |
| Clonezilla source import | should |
| Confirmation modal | skip (replace with `--yes`) |

### Inspect tab

| GUI action | CLI priority |
|---|---|
| Show partition table (MBR/GPT/APM/RDB/SGI) | must |
| Show CHD codec / hunk metadata | should |
| Show per-partition filesystem + sizes | must |
| Compute minimum size (cheap path) | must |
| Compute minimum size (expensive walk) | should |
| Compute defrag/clone minimum (HFS+/FAT) | should |
| fsck / Check (read-only) | must |
| Repair (writable fsck) | should |
| Expand HFS volume (clone to new block size) | should |
| In-place resize (resize popup) | should |

### Browse view

| GUI action | CLI priority |
|---|---|
| List a directory | must |
| Extract file or folder to host | must |
| Read-only stat / preview | should |
| Add file from host | must |
| New directory | must |
| Delete entry (file or recursive folder) | must |
| Set HFS type/creator | should |
| Set HFS Finder flags / dates | should |
| Bless folder (HFS / HFS+) | should |
| Write resource fork | should |
| Apply staged edits | n/a (CLI applies immediately) |
| Archive-edit (decompress → edit → recompress) | must |
| CHD-edit (write through to source) | must |

### Optical tab

| GUI action | CLI priority |
|---|---|
| List optical drives | should |
| Detect disc / show TOC | must |
| Browse ISO/CD contents | must |
| Extract file from disc | must |
| Rip to ISO | must |
| Rip to BIN/CUE | must |
| Rip to CHD | must |
| Convert between optical formats | must |
| Eject after rip | should |

### Dialogs

| Dialog | CLI priority |
|---|---|
| Bulk Convert | should — fits `convert` verb with glob input |
| Expand HFS | should — single command `expand` |
| Resize popup | should — single command `resize` |
| Physical Disk Export | must — write image to device |
| Settings | skip — config file only |
| Elevation | skip — handled by `sudo` from the shell |

### Format-specific surface that already exists in code

  - `api apm info` — partition-table inspect (subsumed by `inspect`).
  - `api sgi shrink` — re-encode CHD with SGI volume-header floor.
  - `api hfs new / info / ls / put / put-zero / get / rm / put-boot /
    validate` — file operations; should generalize across filesystems.

## Strategy: a flat verb namespace

Keep `api` for now as a deprecated alias (drop in a future release).
Drop the `pt` / `sb` / `sf` second-axis sub-verbs. Use a flat set of
top-level verbs that match what the user is *doing*, not which layer of
the disk they're poking at. The tool figures out the layer.

A key delineation: **`ls` lists files inside a filesystem**, **`show
partmap` lists partitions inside a disk image**. The two never mix.
Other focused read-only queries live under `show` too (`show chd-info`,
`show fs-info`), while `inspect` is the everything-at-once aggregator
that mirrors the GUI Inspect tab.

Proposed verbs:

| Verb | Purpose | Mirrors GUI |
|---|---|---|
| `backup`   | Disk → backup folder/archive | Backup tab |
| `restore`  | Backup → disk/image | Restore tab |
| `inspect`  | Aggregate read-only view: PT + filesystems + CHD info | Inspect tab |
| `show`     | Focused read-only sub-queries (`partmap`, `chd-info`, `fs-info`) | Inspect tab subsets |
| `ls`       | List files in a directory inside a filesystem | Browse view |
| `get`      | Extract file/dir from a filesystem | Browse view |
| `put`      | Copy host file into a filesystem (with `--boot` for HFS boot blocks) | Browse view |
| `mkdir`    | Create directory inside a filesystem | Browse view |
| `rm`       | Delete file/dir from a filesystem | Browse view |
| `fsck`     | Check filesystem; `--repair` / `--checkonly` modes | Inspect tab Check / Repair |
| `new`      | Format a blank filesystem image | (no GUI yet) |
| `resize`   | Change partition / filesystem sizes | Resize popup |
| `expand`   | HFS expand (clone to new block size) | Expand HFS dialog |
| `shrink`   | Re-encode CHD dropping trailing zeros | (SGI today) |
| `convert`  | Re-encode between disk-image formats | Bulk Convert dialog |
| `optical`  | CD/DVD subcommands (rip / convert / browse) | Optical tab |
| `write`    | Image → physical device | Physical Disk Export |
| `batch`    | Apply a JSON script of FS operations to one image | (no GUI — scripting only) |
| `batch-template` | Generate a batch JSON from a host dir / target spec | (no GUI) |

## Partition selection: `IMG@N`

Instead of `--partition N`, append `@N` to the image path:

```
rb-cli ls disk.hda@2 /System
rb-cli get disk.hda@2 /System/Finder ./Finder
rb-cli fsck disk.hda@2
```

  - `@N` is 1-based, matching how APM/MBR/GPT entries are displayed.
  - No `@` means: raw filesystem at byte 0 for filesystem verbs; whole
    disk for partition-table-aware verbs (`inspect`, `show partmap`,
    `backup`, etc.).
  - The character `@` is not allowed in any of our supported FS names
    and is uncommon in real-world disk-image filenames, so it parses
    cleanly.

### Shell-quoting check for `@`

`@` is safe-by-default in every shell that matters for us, but worth
recording so we can copy-paste the rules into `--help` later:

  - **bash / zsh / dash / sh** — `@` is only special inside parameter
    expansion (`${var@op}` after a `$`). As a literal argument
    character it's plain text; no quoting needed.
  - **fish** — fully literal, no special meaning.
  - **PowerShell** — `@` is special *only at the start of a token*:
    splat (`@var`), array literal (`@()`), hash literal (`@{}`),
    here-string (`@"..."@`). Mid-token (e.g. `disk.hda@2`) it's
    literal. Wrap in quotes (`"disk.hda@2"`) if a script ever begins
    an argument with `@`.
  - **cmd.exe** — `@` is special only at the start of a line (suppress
    echo); inside arguments it's literal.
  - **Globbing** — no shell treats `@` as a wildcard, so unquoted
    `disk.hda@2` won't accidentally expand.

If a real-world disk-image filename ever contains `@` (rare, but
possible), `--partition N` stays available as a fallback flag.

## HFS-specific niceties retained on the generic verbs

  - `put --type CODE --creator CODE` are HFS-only flags; ignored on
    other filesystems (with a warning) rather than rejected.
  - `put --zero N` replaces `put-zero` (pre-allocate N zero bytes).
  - `put --force` for overwrite.
  - `put --boot IMG BB_FILE` writes the 1024-byte boot-block region
    verbatim (replaces `api hfs put-boot`); no dedicated verb.

### `fsck` modes

Three mutually-exclusive modes covering check-only, prompted repair,
and unattended repair:

  - **Default** — scan, print the report, then prompt
    `Repair detected issues? [y/N]` if any repairable issues were
    found. Good for interactive use.
  - **`--checkonly`** — scan and print; never prompt, never repair.
    Exit code is non-zero if issues are found. Good for CI / cron.
  - **`--repair`** — scan and auto-repair without prompting. Implies
    write access. Errors out if the filesystem isn't repair-capable
    or the image isn't writable.

`--checkonly` and `--repair` are mutually exclusive.

**Prompt timeout.** Default mode prompts with a countdown timer
(`--prompt-timeout SECS`, default `30`) so the command stays fully
scriptable. If the timeout elapses with no input, the prompt resolves
to **No** and the command exits with `--checkonly` semantics
(non-zero exit if issues were found, nothing written). This means:

  - Interactive user hits Enter / `y` / `n` like normal.
  - Piped invocation (no TTY on stdin) — skip the prompt entirely and
    behave as `--checkonly`. No countdown, no wait.
  - Background / `nohup` / forgotten ssh session — countdown expires,
    safe exit. No hung process.
  - Need unattended repair? Pass `--repair`. Need to disable the
    timeout for slow human review? `--prompt-timeout 0` waits
    indefinitely (only meaningful on a TTY).

## Example command shapes

```
# Focused queries — replaces `api apm info`
rb-cli show partmap  disk.hda
rb-cli show chd-info disk.chd
rb-cli show fs-info  disk.hda@2

# Aggregate Inspect-tab view
rb-cli inspect disk.hda

# Format a blank floppy — replaces `api hfs new`
rb-cli new mac.dsk --fs hfs --size 800K --name MyDisk

# Browse / edit — replaces `api hfs {ls,put,get,rm,put-boot}`
rb-cli ls    mac.dsk /
rb-cli put   disk.hda@2 ./Finder /System/Finder --type FNDR --creator MACS
rb-cli put   disk.hda@2 --boot ./bootblocks.bin
rb-cli get   disk.hda@2 /System/Finder ./Finder
rb-cli mkdir disk.hda@2 /System/Extensions
rb-cli rm    disk.hda@2 /Trash/old.bin

# Filesystem check / repair — replaces `api hfs validate`
rb-cli fsck disk.hda@2                 # scan + prompt
rb-cli fsck disk.hda@2 --checkonly     # scan only (CI-safe)
rb-cli fsck disk.hda@2 --repair        # auto-repair

# SGI re-encode — replaces `api sgi shrink`
rb-cli shrink in.chd out.chd

# Real GUI features no CLI ever exposed
rb-cli backup  /dev/disk3 ./backups/mydisk --format chd --checksum sha256
rb-cli restore ./backups/mydisk /dev/disk3 --size minimum
rb-cli expand  disk.hda@2 expanded.hda --size 2G --block-size 4096
rb-cli resize  disk.hda  --plan plan.json   # or interactive
rb-cli convert in.vhd out.chd
rb-cli optical rip /dev/disk5 ./disc.chd --format chd --eject
rb-cli write   image.hda /dev/disk6 --yes
```

## Designing for verb churn

This grammar is a first draft. We will get feedback once real users
script against it, and verbs / flags will move around. The code
should make that cheap.

Rules to keep moves cheap:

  1. **One handler function per verb, named after the verb.** No
     business logic inside the `clap` enum or `match` arms. Renaming
     `fsck` → `check` is then a single change in the enum + a
     function rename; the body never moves.

  2. **Handler signatures take plain Rust types, not clap structs.**
     `fn fsck(image: &Path, partition: Option<u32>, mode: FsckMode,
     prompt_timeout: Duration) -> Result<()>`. The clap layer is
     just a translator — it parses argv into those types and calls
     the handler. Means we can wire two different verb spellings
     (e.g. `api hfs validate` and `fsck`) to the same handler with
     no copy-paste.

  3. **Shared helpers for cross-cutting concerns.** Image+partition
     resolution (the `IMG@N` parser), size-string parsing
     (`800K`/`5M`/`1G`), TTY-aware prompts, progress bars — each
     gets one module so verbs can swap them out uniformly.

  4. **Keep `src/cli.rs` thin.** Today it's 815 lines; the bulk is
     HFS-specific glue. As we generalize verbs to dispatch through
     `open_filesystem` / `open_editable_filesystem`, that file
     shrinks and the per-FS code stays where it belongs (in `fs/*`).

  5. **Aliases are cheap.** clap supports `#[command(alias = "...")]`
     and `#[arg(alias = "...")]`. Use them for renames so old verb
     spellings keep working through a deprecation cycle. The whole
     `api …` namespace ships as aliases pointing at the new
     handlers.

  6. **Snapshot tests on `--help` output.** Catch accidental grammar
     drift; intentional changes update the snapshot. Cheap insurance
     against breaking scripts.

## Config file (`~/.config/rb-cli/rbcli.conf`)

Out of scope for Phase A; sketched here so we don't paint ourselves
into a corner.

  - **Format:** simple INI (sections + `key = value`), not TOML.
    Sections map to verbs (`[backup]`, `[fsck]`, `[optical]`) plus a
    `[defaults]` section for cross-cutting options.
  - **Location:** XDG-style. `$XDG_CONFIG_HOME/rb-cli/rbcli.conf` or
    `~/.config/rb-cli/rbcli.conf` on Linux/macOS;
    `%APPDATA%\rb-cli\rbcli.conf` on Windows. A `--config PATH`
    override always wins.
  - **Precedence:** explicit CLI flag > config file > built-in
    default. The config supplies *defaults* for flags; it never
    forces behavior that the user didn't ask for.
  - **Scope deliberately limited:** the config holds preferences
    (default compression, default CHD codec, preferred checksum,
    update-check on/off), not the per-invocation arguments
    (source/target paths, partition indices). Those always come
    from the command line.
  - **Bootstrap command:** a future `rb-cli config init` writes a
    commented-out template so users can see every available key
    without having to read source.

Detailed scoping deferred until the verb grammar settles — we don't
want to commit to key names that get renamed in a week.

## Interactive terminal (`rb-cli terminal`)

A separate feature, launched via a top-level verb of its own. The
non-terminal verbs (`backup`, `restore`, `inspect`, `ls`, …) remain
one-shot and scriptable; `terminal` opens a REPL for multi-operation
sessions.

Why it pays off:

  - Open a disk image once, run `inspect` → `ls` → `cd /System` →
    `get …` → `put …` without paying APM/HFS parse cost each call.
  - Tab-completion of Mac paths, partition indices, and verbs.
  - Edit operations stay in memory and flush on `commit` / `:w`,
    matching the GUI's staged-edits model. `:q!` discards.

Sketch of the session:

```
$ rb-cli terminal disk.hda
disk.hda > show partmap
  1  Apple_partition_map  Apple                64
  2  Apple_HFS            MacOS           4194240
disk.hda > use 2                       # select partition
disk.hda@2 > ls /System
  ...
disk.hda@2 > cd /System
disk.hda@2:/System > get Finder ~/Finder
disk.hda@2:/System > edit on           # enter edit mode
disk.hda@2:/System (edit) > put ~/NewFile.bin NewFile --type BINA
disk.hda@2:/System (edit) > commit     # flushes sync_metadata
disk.hda@2:/System > quit
```

Implementation notes (deferred to its own phase):

  - Built on `rustyline` (or `reedline`) for line editing and history.
  - Inner verbs reuse the same handler functions as the one-shot CLI
    — REPL is a thin loop that parses tokens and calls them with
    state preserved (open `File`, current FS handle, current path).
  - Persistent history in `~/.cache/rb-cli/history`.
  - Honors `--config` and reads the same `rbcli.conf` defaults.
  - Ships as Phase F (after the one-shot verbs are stable enough to
    rely on as building blocks).



  1. **Phase A — add flat verbs alongside `api`.** Keep `api` working
     as a deprecated alias; new verbs share the same handler code, so
     there's no duplication. Drop `api` in a future release.
     - `api hfs new`      → `new --fs hfs`
     - `api hfs info`     → `show fs-info` (and rolled into `inspect`)
     - `api hfs ls`       → `ls`
     - `api hfs put`      → `put`
     - `api hfs put-zero` → `put --zero N`
     - `api hfs get`      → `get`
     - `api hfs rm`       → `rm`
     - `api hfs put-boot` → `put --boot`
     - `api hfs validate` → `fsck` (default mode = scan + prompt)
     - `api apm info`     → `show partmap` (and rolled into `inspect`)
     - `api sgi shrink`   → `shrink`

  2. **Phase B — generalize the FS verbs.** `ls/put/get/rm/mkdir/fsck`
     auto-detect filesystem inside `IMG@N`. Wire FAT, HFS+, NTFS,
     exFAT, AFFS, PFS3, SFS, EFS via the existing
     `open_filesystem` / `open_editable_filesystem` dispatch.

  3. **Phase C — add the big-ticket verbs.** `backup`, `restore`,
     `inspect` (full), `write`, `convert`. These reuse existing GUI
     orchestration modules; the CLI is a thin headless front-end.

  4. **Phase D — the resize/expand cluster.** `resize`, `expand`,
     `shrink` for other filesystems. Lower priority — interactive
     review is genuinely useful here.

  5. **Phase E — optical.** `optical rip / convert / browse / extract`.
     Often easiest with the GUI but worth scripting for bulk rips.

  6. **Phase F — interactive `terminal` REPL.** Built on top of the
     stable one-shot verbs. See the "Interactive terminal" section
     above.

  7. **Phase G — config file.** `rbcli.conf` plus `rb-cli config init`.
     Wait until the flag set has settled across at least one release
     so we're not renaming config keys every commit.

## Decisions locked in (2026-05-18)

  1. **Keep `api` as a deprecated alias** through the introduction of
     the flat verbs; drop later.
  2. **`IMG@N` is the partition selector.** Safe across bash, zsh,
     fish, PowerShell, cmd. `--partition N` stays as a fallback.
  3. **Boot blocks are written via `put --boot`.** No dedicated verb.
  4. **`fsck` has three modes**: default (scan + prompt),
     `--checkonly` (scan only), `--repair` (auto-repair). Non-TTY
     stdin downgrades default to `--checkonly`.
  5. **`show partmap` vs. `ls` are strictly separate.** `show partmap`
     lists partitions inside a disk image. `ls` lists files inside a
     filesystem. `inspect` is the GUI-Inspect-tab aggregator.

## Resolved follow-ups (2026-05-18)

### `inspect` is whole-disk only

`inspect IMG@N` is **not** supported. `inspect` always operates on the
whole disk image: partition table summary, CHD codec/hunk if relevant,
and a one-line-per-partition rollup with detected filesystem.

For per-partition detail, use the focused query:

```
rb-cli show fs-info disk.hda@2
```

The user-suggested alternative `inspect part IMG N` is reserved as a
possible future alias if `show fs-info` proves too verbose; today it
would just duplicate `show fs-info` and is left out.

### Blank-volume creators — scope

Inventory of what exists in `src/fs/` today:

| Filesystem | Blank creator | Status |
|---|---|---|
| HFS         | `create_blank_hfs`, `create_blank_hfs_sized` (`src/fs/hfs.rs`)     | exists |
| HFS+        | `create_blank_hfsplus` (`src/fs/hfsplus.rs`)                       | exists |
| PFS3        | `create_blank_pfs3` (`src/fs/pfs3.rs`)                             | exists |
| SFS         | `create_blank_sfs` (`src/fs/sfs.rs`)                               | exists |
| FAT12/16/32 | none                                                               | **missing** |
| NTFS        | none                                                               | missing (low priority — rarely needed for vintage workflows) |
| exFAT       | none                                                               | missing (low priority) |
| AFFS        | none                                                               | **missing** |
| EFS         | none                                                               | **missing** |
| ext         | none                                                               | skip (use real `mkfs.ext*` outside the tool) |
| btrfs       | none                                                               | skip |
| ProDOS      | none                                                               | TBD |
| ISO9660     | n/a — read-only                                                     | skip |

Priority for new blank creators, in order:

  1. **FAT12/16/32** — the obvious gap. Needed for DOS floppies, retro
     SD/CF cards, FAT EFI system partitions. Goes hand-in-hand with
     `new --fs fat --size 1.44M`.
  2. **EFS** — IRIX workflows are a current focus and `efs_resize.md`
     work just shipped. A blank creator complements the grow/shrink
     pipeline.
  3. **AFFS** — completes the Amiga trio (PFS3 and SFS already have
     creators).
  4. NTFS / exFAT — defer until someone asks.

These don't block CLI Phase A; they slot into Phase B (filesystem
generalization). `new --fs hfs|hfsplus|pfs3|sfs` ships in Phase A,
others gated on the creator landing.

### `backup` flag surface — non-interactive only

The CLI is strictly non-interactive: every knob the GUI Backup tab
exposes maps to a flag, and the user is expected to run an inspect
pass first to see what's there.

Suggested workflow:

```
# 1. Survey the source
rb-cli inspect /dev/disk3

# 2. Specify per-partition behavior from the inspect output
rb-cli backup /dev/disk3 ./backups/mydisk \
    --format chd \
    --checksum sha256 \
    --partitions 1,2,4 \
    --defrag 2 \
    --sector-by-sector 4 \
    --split-size 4G \
    --chd-codec lzma,zlib,huff,flac \
    --chd-hunk 4096 \
    --name "MyDisk-2026-05"
```

Rules of thumb:

  - Per-partition options take partition indices (1-based, matching
    the `inspect` output): `--defrag 2,3` toggles defrag on partitions
    2 and 3.
  - Bare flags without indices apply to all selected partitions.
  - No prompts, no confirmations beyond a top-level `--yes` for
    destructive targets (writing to a physical device).
  - Anything not specified takes the GUI default.

We are not introducing a config file in Phase A. If the flag list
grows past comfortable, a `--config file.toml` escape hatch can be
added later — but the flags must always work standalone for one-shot
shell invocations.

## Resolved follow-ups (2026-05-19)

### Exit codes

Small, git-style set. No bitmask convention — `fsck` follows the same
table as every other verb.

| Code | Meaning |
|---|---|
| 0   | Success |
| 1   | Generic operation failure (I/O, parse error, `fsck --checkonly` found issues, partial backup kept on Ctrl-C) |
| 2   | Usage / syntax error (bad flag, unknown verb) — clap default |
| 3   | Not found (image, partition index out of range, path inside FS missing) |
| 4   | Permission denied / needs elevation |
| 5   | User declined a prompt, or prompt timed out to No |
| 130 | SIGINT (Ctrl-C), shell convention 128 + signal number |

Logging verbs / progress lines may include the exit-code-relevant
summary on stderr (e.g. `fsck: 3 issues found, 0 repaired (exit 1)`)
so wrapper scripts don't have to re-parse output.

### Structured output (`--format`)

Modeled on `kubectl -o …` and `gh --json`. Single flag, queries only.

  - `--format text` (default) — human-readable tabular / paragraph form.
  - `--format json` — pretty-printed JSON object. Always includes a
    top-level `"schema_version": 1` for future-proofing.
  - `--format yaml` — same structure as JSON, serialized to YAML.
  - `--format csv` — flat tabular outputs only (`ls`, `show partmap`).
    Errors out with usage-code 2 on nested-result verbs (`inspect`,
    `show chd-info`, `show fs-info`).
  - `--format tsv` — same scope as CSV.

Applies to: `inspect`, `show partmap`, `show chd-info`, `show fs-info`,
`ls`, `fsck` (report form). Long-running verbs (`backup`, `restore`,
`convert`, `optical rip`, `expand`, `resize`, `write`, `batch`) stay
plain-text + a final summary line. A future `--json-events` for
streaming progress can be added if anyone asks; not in scope now.

Be thorough: every queryable surface emits the same field set across
all four structured formats. JSON is the canonical shape; YAML mirrors
it; CSV/TSV is the flat projection (one row per partition / per
directory entry / per fsck issue).

Schema stability: declared stable from v1.0 onward. Pre-1.0 the
`schema_version` field still exists but breaking changes are allowed
between minor releases with a CHANGELOG note.

### `get` / `put` semantics

**Recursion.** Implicit when source is a directory — no `-r` flag.

**Trailing-slash rule.** A trailing `/` on the destination means "into
this directory" (create it if needed). No trailing slash means "to this
exact path" (the source is renamed to the destination's basename).
The `--help` text spells this out with examples:

```
# Extract Finder to ./Finder (file)
rb-cli get disk.hda@2 /System/Finder ./Finder

# Extract Finder into ./extracted/ as ./extracted/Finder
rb-cli get disk.hda@2 /System/Finder ./extracted/

# Recursive: extract /System into ./out/ as ./out/System/...
rb-cli get disk.hda@2 /System ./out/

# Recursive: rename root to ./mycopy/
rb-cli get disk.hda@2 /System ./mycopy
```

The host filesystem's existing convention (rsync, cp -R) is the model.

**Conflict handling.**

  - Default: error and exit with code 1, leaving anything already
    written in place.
  - `--force` — overwrite existing entries.
  - `--skip-existing` — leave existing entries untouched, copy the
    rest. Good for resumable bulk puts.
  - `--force` and `--skip-existing` are mutually exclusive.

**Hidden / system files.** Many filesystems have them: HFS Finder
invisible flag, FAT hidden+system attributes, AFFS H/S protection bits,
NTFS hidden attribute. Extract all of them when traversing a directory
(treat them like any other entry) and preserve the host-side attribute
when possible. On hosts/host-filesystems that can't represent the
attribute (e.g. FAT host extracting an HFS invisible flag), emit a
single warning line per affected entry; the extraction itself
succeeds.

**Dates.** A `--preserve-dates` flag copies created/modified/backup
dates from the FS to host (and host → FS on `put`). **Default off** —
fresh writes get current date/time. Rationale: most retro workflows
*want* the new timestamp; date-preservation is opt-in for
archival-quality copies.

**Resource forks (HFS / HFS+).** On extract, write a sidecar
`<filename>.rsrc` next to the data fork when the resource fork is
non-empty. Flags:

  - `--no-rsrc` — skip the sidecar (data fork only).
  - `--applesingle` — bundle data + rsrc + Finder info into a single
    AppleSingle container.
  - `--native-rsrc` (darwin only) — write to `<filename>/..namedfork/rsrc`
    so the macOS host preserves it natively.

On `put`, if a `<src>.rsrc` exists next to the source it's written to
the resource fork automatically; `--rsrc PATH` overrides.

**Symlinks.** Skip with a warning on both extract and ingest.
Document in the verb help: "Symlinks inside filesystem images are not
supported by rb-cli; use the GUI browse view or another tool."

**Per-file metadata flags.** Already covered above — `--type CODE`,
`--creator CODE` for HFS / HFS+. ProDOS uses a 1-byte file_type and a
2-byte aux_type; same flag spellings (`--type`, `--aux`) work there.
On filesystems where the flag is meaningless, a warning is emitted and
the file is still written.

### File globbing — to scope

Globbing is a major feature in its own right and deserves its own
design doc before coding. Open questions to settle:

  1. **Where does it apply?** `get`, `rm`, `ls`, and the batch
     generator are the obvious targets. Does `put` accept host-side
     globs (shell does this for us) or need its own (for cross-shell
     consistency)?
  2. **Syntax.** Plain shell-style (`*.bin`, `**/*`) or full
     regex-capable? POSIX glob is the safe default.
  3. **Anchoring.** Are patterns anchored to the FS root (`/System/*`)
     or path-relative (`Finder*` matches everywhere under cwd)? My
     instinct: explicit `/`-anchored matches root; otherwise relative.
  4. **Case sensitivity.** HFS is case-insensitive (HFSX optionally
     sensitive); FAT is case-insensitive on lookup but case-preserving
     on store. Default: match the filesystem's native rules. Flag
     `--ignore-case` / `--case-sensitive` to override.
  5. **`--include` / `--exclude` flags** on the batch generator
     (definitely needed) — multiple-allowed, applied in order, last
     match wins (rsync rules)?
  6. **Glob expansion in batch JSON?** Probably not — globs are
     resolved at generator time so the JSON is a deterministic list.
  7. **Special HFS / ProDOS character ranges?** HFS allows `/` in
     filenames (it's just displayed as `:` on classic Mac); how does
     the glob escape that?

Park this for a follow-up doc; it is **not** a Phase A blocker. The
batch generator can ship with a minimal `--include`/`--exclude` for
host-side patterns and grow from there.

### Batch scripts (`rb-cli batch`)

Single-target JSON script applied as one transaction-ish operation.
Matches the GUI's staged-edits model: collect everything, run preflight
checks, then apply.

**Top-level shape:**

```json
{
  "schema": "rb-cli-batch/1",
  "target": "disk.hda@2",
  "default_options": {
    "force": false,
    "creator": "MACS"
  },
  "operations": [
    { "op": "mkdir", "path": "/System/Extensions" },
    {
      "op": "put",
      "src": "./build/Finder",
      "dst": "/System/Finder",
      "type": "FNDR",
      "creator": "MACS",
      "rsrc": "./build/Finder.rsrc",
      "locked": true
    },
    {
      "op": "put",
      "src": "./build/extensions/",
      "dst": "/System/Extensions/"
    },
    { "op": "rm", "path": "/Trash/old.bin" },
    { "op": "bless", "path": "/System Folder" }
  ]
}
```

  - **One target per script.** Loop in shell to hit multiple images.
  - **`format` op allowed at the top of `operations`** when the target
    doesn't exist yet — see "Whole-volume creation" below.
  - **No mid-script `target` override.**
  - **No variable substitution / templating.** Pipe through `jq` if you
    want it.
  - **JSON only.** YAML isn't a goal here because users won't be
    hand-writing these — the generator emits them.

**Preflight (matches GUI):** before any write, walk the operation list
and check:

  - All `src` paths exist on the host.
  - All `dst` parents exist on the FS (or will be created earlier in
    the list — preflight maintains a virtual view of the staged tree).
  - No duplicate creations under the same parent.
  - Projected free-space ≥ sum of file sizes (with FS overhead estimate).
  - Type/creator/aux strings are valid 4-char (or ProDOS byte) values.
  - Format-op preflight: target path is writable and (for `--force`)
    overwriting is allowed.

If preflight fails the script exits with code 1 and writes **nothing**.

**Refactor note.** The GUI today does these preflight-style checks
across `browse_view.rs` (free-space projection, duplicate-name check,
type/creator validation) and the staged-edits queue. For the CLI to
share them, we should lift the preflight logic into a model module
(e.g. `src/model/batch_preflight.rs`) that both `browse_view` and the
new CLI `batch` handler call. This is a real refactor but pays off
twice. Plan the lift as part of Phase D (or whichever phase lands
batch); don't duplicate the logic.

**Apply semantics.**

  - Open one `EditableFilesystem`, replay all ops in order, single
    final `sync_metadata()` at the end. Same as `apply_staged_edits()`
    in the GUI.
  - Default: **stop on first error.** Whatever ran before the error
    stays applied (one sync_metadata covers it); the report names the
    failed op.
  - `--continue-on-error` runs remaining ops after a failure and
    summarizes at the end.
  - Not transactional — no rollback on partial success. Same trade as
    the GUI today.

**Invocation:**

```
rb-cli batch script.json
rb-cli batch script.json --dry-run            # preflight + plan, no write
rb-cli batch script.json --target disk.hda@2  # override script's target
rb-cli batch script.json --continue-on-error
```

### Batch generator (`rb-cli batch-template`)

Scans a host directory (or an empty-volume spec) and emits a batch
JSON script the user then edits + applies. The killer feature for
"build a vintage volume from a host folder" workflows.

**Modes:**

```
# Mirror a host dir into a fresh HFS HDD
rb-cli batch-template ./build/ \
    --format hfs \
    --size 80M --block-size 4096 \
    --name "Boot Disk" \
    --include '*.bin' --exclude '*.tmp' \
    > script.json

# Floppy
rb-cli batch-template ./floppy-contents/ \
    --format hfs --size 800K --name "Disk 1" \
    > floppy.json

# Add to an existing image (no format op emitted)
rb-cli batch-template ./additions/ \
    --target disk.hda@2 --dst /System/ \
    > additions.json
```

Hard-disk and floppy creation share the same generator — the only
difference is the `--size` argument and (optionally) the geometry.
Output script begins with a `{ "op": "format", "fs": "...", "size":
"...", "block_size": "...", "name": "..." }` op when `--format` is
specified.

Files in the host tree become `put` ops; subdirectories become
`mkdir` ops first, then their contents. Type/creator inference for
HFS: by extension table (`.app` → APPL/MACS, `.txt` → TEXT/ttxt,
etc.) or `--type-table file.json` to override. Resource forks are
auto-detected from `.rsrc` sidecars or AppleSingle wrappers.

`--include` / `--exclude` apply to the host walk (rsync-style ordering;
last match wins). Globbing details are deferred to the file-globbing
doc above.

The generated JSON is fully self-contained — paths are absolute or
relative-to-script-dir — so the user can review, tweak, check it into
git, and re-run.

### Whole-volume creation via `format` op

Top of the `operations` array (and only there) may contain a single
`format` op. When present, the target image is created from scratch
before any subsequent ops run.

```json
{
  "schema": "rb-cli-batch/1",
  "target": "./out.hda",
  "operations": [
    {
      "op": "format",
      "fs": "hfs",
      "size": "80M",
      "block_size": 4096,
      "name": "Boot Disk",
      "partition_table": "apm"
    },
    { "op": "mkdir", "path": "/System Folder" },
    { "op": "put", "src": "./System", "dst": "/System Folder/" },
    { "op": "bless", "path": "/System Folder" }
  ]
}
```

Supported `fs` values for the format op follow the blank-creator
priority list (HFS, HFS+, PFS3, SFS, FAT in Phase B; EFS / AFFS as
they land). `partition_table` is optional; if omitted the volume is
written raw (no PT). If present (`apm` / `mbr` / `gpt`) the generator
emits the appropriate wrapper, and subsequent path ops implicitly
target partition 1 unless the script's `target` says otherwise.

### Ctrl-C / SIGINT behavior

Per-verb behavior on first Ctrl-C:

  - **`backup`** — set cancel flag, finish current sector write, prompt
    `Keep partial backup at ./backups/mydisk? [Y/n]`. Yes ⇒ write
    `metadata.json` with `"status": "partial"` and per-partition
    `bytes_written`, exit 130. No ⇒ rm-rf the backup folder, exit 130.
  - **`restore`** to a physical device — set cancel flag, stop writes,
    prompt `Disk is in an inconsistent state. Continue anyway? [y/N]`
    style — but inverted: this prompt is just a "what happened"
    notification; the disk is whatever it is. Exit 130 either way.
    User may have hit Ctrl-C because the target is bad.
  - **`restore`** to a file — like `backup`: prompt to keep / discard
    the partial output.
  - **`convert`** — partial output is usually useless; default discard
    with a `--keep-partial` flag to override.
  - **`optical rip`** — prompt to keep partial rip. Discs are
    sometimes intermittently readable and "what we got" has value.
    Also expose `--skip-bad-sectors` (zero-fill unreadable sectors and
    keep going) as a separate flag so the user can pre-decide.
  - **`batch`** — current sub-op finishes if possible, no further ops,
    final `sync_metadata` runs (so already-applied ops are durable),
    exit 130.
  - **All other verbs** — fast-exit, no prompt.

A **second Ctrl-C** during the prompt = immediate hard abort, no
discard, no metadata write. Exit 130.

**Non-TTY:** keep things simple — same as TTY behavior, but the prompt
defaults to "Keep" after a short timeout (1 second) and the choice is
logged on stderr. Rationale: cron / ssh-without-tty users would rather
keep partial data than lose it.

**Resume support** is deferred to its own phase — partial outputs are
written with enough state (`bytes_written` per partition) that a future
`--resume` can read it, but no resume code ships yet. This phase can
also feed the GUI (a "Resume previous backup" entry on the Backup tab).
