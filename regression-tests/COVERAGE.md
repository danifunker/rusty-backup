# Coverage Matrix

The enumerated test surface, derived from the code and the maintained README
tables on 2026-08-01 — not from memory. Re-derive this whenever a phase in
`PLAN.md` closes; drift here is itself a bug.

Sources of truth:

- Filesystems / container formats / optical FS / partition schemes: the
  maintained tables in `README.md` § Compatibility.
- Partition schemes, authoritative: the `PartitionTable` enum in
  `src/partition/mod.rs`.
- CLI verbs: the `###` headings in `docs/cli-reference.md`.
- Picker extensions: `DISK_IMAGE_EXTS` in `src/model/file_types.rs`.

## Headline numbers

| Axis                     | Count | Source |
|--------------------------|------:|--------|
| Filesystem families      |    42 | README § Filesystems |
| Container / image formats|    47 | README § Image / backup formats |
| Optical filesystems      |    10 | README § Optical disc filesystems |
| Partition schemes        |     9 | `PartitionTable` enum |
| CLI verb / subverb entries |  119 | `docs/cli-reference.md` |
| Picker extensions        |    69 | `DISK_IMAGE_EXTS` (65 unique, case-folded) |

A naive cross-product is meaningless (nobody converts a Xerox Alto pack to a
BasiliskII HFV). The matrix is therefore **per-axis with targeted
intersections**, described below.

---

## Findings already surfaced

Recorded here as they are found, before the harness even runs.

### R-001 — README partition-table list is missing two schemes

`src/partition/mod.rs` defines nine `PartitionTable` variants: `Mbr`, `Gpt`,
`Apm`, `Rdb`, `Sgi`, `Sun`, `Ahdi`, `X68k`, `None`. The README
§ Partition tables table lists only MBR, GPT, APM, RDB, SGI, Sun and
"None (superfloppy)" — **Atari AHDI and Sharp X68000 are absent** despite
both being fully implemented (`src/partition/atari.rs`,
`src/partition/x68k.rs`) and both having dedicated CLI builders
(`rb-cli new hd x68k`).

Exactly the drift class CLAUDE.md's pre-commit documentation-sync section
warns about. Fix in a normal commit, not via the regression suite.

### R-003 — `output.rs` claims `ls` supports `--format`; it does not

The module documentation in `src/cli/output.rs` lists `ls` among the verbs
that "can emit their results in one of five formats", and its CSV/TSV scope
note says those formats "only apply to *flat* tabular results — `ls`, `show
partmap`, `show devices`, `fsck` issue lists".

`rb-cli ls --help` exposes no `--format` flag at all, and `--format` is not a
global option either — both `rb-cli ls img --format json` and
`rb-cli --format json ls img` exit 2 with "unexpected argument". Verified
present on `inspect`, `fsck`, `du` and `locate`.

Either the flag was dropped from `ls` and the doc is stale, or structured
`ls` output is a genuine gap. `ls` is one of the most script-facing verbs in
the CLI, so this matters.

### R-004 — CSV/TSV rejection exits 1, documented as 2

`src/cli/output.rs` states that nested-result verbs "error out with
[`crate::cli::exit::USAGE_ERROR`]" — that is, **exit 2**. Observed:

```
rb-cli inspect vol.img --format csv   ->  exit 1
error: inspect returns nested data; --format csv only supports flat tabular
results. Use --format json or --format yaml instead.
```

The message is right; the exit code is not. `require_non_flat`
(`src/cli/output.rs:161`) uses `anyhow::bail!`, which propagates as a generic
failure. The existing unit test `require_non_flat_rejects_csv_for_nested`
only asserts `is_err()`, so it cannot catch this.

A script branching on the documented exit-code contract cannot distinguish
"you asked for the wrong format" from any other failure.

### R-005 — no error envelope is emitted under `--format json`

`src/cli/output.rs` documents that on failure the envelope still comes back,
with `status.error: true`, `status.code` carrying the exit code,
`status.message` a short description, and `result` null.

Observed: a failing verb under `--format json` writes **nothing** to stdout
and a plain-text error to stderr.

```
rb-cli inspect ./missing.img --format json
  -> exit 1, stdout empty, stderr: "error: open ./missing.img: ..."
```

A JSON consumer therefore gets no parseable output on the error path, which
is precisely the case the envelope exists to serve. Either the error
envelope was never wired up, or it regressed.

### R-006 — `new volume prodos` cannot succeed with default arguments

The default volume name for `rb-cli new volume` is `rusty-backup`. ProDOS
forbids `-` in volume names, so the default is invalid for that filesystem
and the verb always fails:

```
rb-cli new volume prodos --size 2M vp.img
  -> error: invalid data: filename contains '-' — ProDOS allows only letters
     (A-Z), digits (0-9), and '.'; rename the file
```

`--name TESTVOL` succeeds and produces a valid volume, so the builder itself
is fine — only the default is unusable. The message compounds it by saying
"rename the file" when the offending string is the volume name, not the
filename.

Every other `new volume` filesystem (hfs, hfsplus, hfv, fat, ntfs, hpfs,
ext, ext3, ext4, affs, efs, minix2, minix3) accepts the default.

### R-007 — a freshly formatted NTFS volume fails its own fsck

```
rb-cli new volume ntfs --size 2M v.img
rb-cli fsck v.img --checkonly
  ERROR [BitmapLeaked] 16 cluster(s) marked allocated in $Bitmap but referenced by nothing
  ERROR [BackupBootMismatch] backup boot sector (last sector) does not match the VBR
  -> exit 1
```

Either the formatter or the fsck is wrong, but they cannot both be right, and
`format -> fsck` on a volume nothing has touched must be clean.

`BackupBootMismatch` is the more serious of the two: real NTFS keeps a copy of
the VBR in the volume's last sector, and Windows checks it. A volume whose
backup boot sector does not mirror the VBR is not one `chkdsk` will be happy
with, which undercuts the point of writing NTFS at all.

### R-008 — the AFFS formatter emits only one bitmap block

One AFFS bitmap block covers `(512 - 4) / 4 * 32 = 4064` bits, addressing
blocks 2 through 4065. `src/fs/affs.rs` writes exactly one, with the comment
"the bitmap covers a full 4064-bit page even if the volume is smaller" — which
holds only while the volume *is* smaller. Two failures follow.

**R-008a — uncovered tail blocks above 4066 blocks.** A 2 MB volume is 4096
blocks; blocks 4066..4095 fall outside the single bitmap page and fsck flags
them:

```
ERROR [AffsBitmapMismatch] 30 block(s) marked allocated but unreachable
WARN  [AffsOrphanBlock]    30 block(s) unreachable from root: [4066, 4067, ...]
```

Confirmed as a boundary, not a coincidence — at `--size 2033K` (exactly 4066
blocks: 2 boot + 4064 mapped) fsck is **clean**; every larger size fails.

**R-008b — panic above ~3 MB.** AFFS puts the root block at the volume
midpoint. Once that index passes the single bitmap page, the "mark in-use"
loop indexes past the 512-byte block. Unlike the loop immediately below it,
this one has no `word_idx >= 128` guard:

```
rb-cli new volume affs --size 4M v.img
  thread 'main' panicked at src/fs/affs.rs: range end index 516 out of range
  for slice of length 512
  -> exit 101, no file created
```

The arithmetic matches exactly:

| size | blocks | root  | bit  | word_idx | slice access | observed message |
|------|-------:|------:|-----:|---------:|--------------|------------------|
| 4 MB |  8192  | 4096  | 4094 |     128  | `bm[512..516]`   | "range end index 516" |
| 8 MB | 16384  | 8192  | 8190 |     256  | `bm[1024..1028]` | "range start index 1024" |

Working sizes: up to 3 MB. Broken: 4 MB and above. A user asking for an
ordinary 4 MB Amiga volume gets a Rust panic and no file — the worst failure
mode in the suite so far, since exit 101 is not in the exit-code contract at
all and the message is a stack trace rather than a diagnostic.

Both halves have the same fix: allocate as many bitmap blocks as the volume
needs, chain them through the root block's `bm_pages` slots, and bound the
in-use marking loop.

### R-009 — superfloppy detection is missing JFS, UFS1/UFS2 and ReiserFS

**FIXED 2026-08-07**, together with R-017 (SFS). `fs.read.{jfs,reiserfs,ufs1,
ufs2}` and `fs.detect.sfs-bare-volume` went green; see `docs/Regression_Bugs.md`
for what changed. Report as filed follows.

`rb-cli inspect` cannot open a bare (partition-table-free) image of four
filesystems the engine has complete drivers for:

```
rb-cli inspect test_jfs.img
  -> error: detecting partition table: Invalid MBR: invalid boot signature:
     expected 0xAA55, got 0x0000
```

Same for `test_ufs1.img`, `test_ufs2.img`, `test_reiserfs_v3_6.img`.

Not a general superfloppy failure — the detector handles the neighbouring
cases fine. Swept over the committed fixtures:

| filesystem | bare-image `inspect` |
|------------|----------------------|
| btrfs, exFAT, APFS, XFS v4/v5 | works |
| ext, NTFS, HFS, HFS+, HPFS, Minix V2/V3, AFFS, ProDOS, EFS, FAT | works |
| **JFS, UFS1, UFS2, ReiserFS** | **fails** |

`detect_superfloppy` (`src/partition/mod.rs`) probes magics for SquashFS, the
Xerox Alto family, Pilot/Cedar, Dwarf, Lisa, NTFS, exFAT, XFS, ext and btrfs
— JFS (`JFS1` at 32768), UFS (`0x011954` at 8192 or 65536) and ReiserFS
(`ReIsEr2Fs`) are simply absent.

Why it went unnoticed: the `cargo test` suites for these four call
`open_filesystem` directly and never route through partition-table detection,
so the drivers are well covered while the path an actual user takes is not.
The fixtures were added by the `docs/need_fixtures.md` work — the drivers
landed, the detection wiring did not.

User impact: anyone who dumps a raw JFS/UFS/ReiserFS partition and points
rb-cli at it gets an MBR error, with no hint that the filesystem is in fact
fully supported.

### R-010 — `inspect` has no `--fs-type`, so signature-less images can't be inspected

`--fs-type` exists on `ls`, `fsck` and `du`, and `docs/cli-reference.md`
describes it as the mechanism for CP/M images "which have no on-disk
signature". `inspect` does not take it — its usage line is bare
`inspect <IMAGE>`:

```
rb-cli inspect ManicMiner.dsk --fs-type cpm:amstrad_data
  -> error: unexpected argument '--fs-type' found

rb-cli inspect ManicMiner.dsk
  -> error: detecting partition table: Invalid MBR: invalid boot signature:
     expected 0xAA55, got 0xE5E5
```

`rb-cli ls ManicMiner.dsk --fs-type cpm:amstrad_data` works and lists
`MANIC.BAS` / `MANIC.BIN`, so the engine is fine — `inspect` simply cannot
reach it. Every CP/M disk (nine DPB presets, the largest sub-axis in the
suite) is therefore un-inspectable, along with any other format needing a
forced dispatch.

Lower severity than R-009 since a working path exists, but `inspect` is the
natural first command a user reaches for.

### R-011 — G64 decoding fails on copy-protected / patched GCR dumps

Of three real-world G64 files tested during fixture harvesting, one opened
and two failed:

| file | result |
|------|--------|
| `C'est la Vie.g64` (278 KB) | opens |
| `Protector II (must be write protected).g64` (191 KB) | fails |
| `American Express (Magic Disk 64 1989-09 Side 2) [patched].g64` (278 KB) | fails |

Both failures are copy-protected or patched dumps with non-standard GCR;
both report the generic `Invalid MBR: invalid boot signature` rather than
anything GCR-specific.

Needs a decision before it can be graded: preserving copy-protected disks is
the *reason* G64 exists as a format (as opposed to D64), so partial support
may be a real limitation rather than an acceptable scope boundary. The
sector-image `.d64` path and the `.g71` fixture are unaffected.

`C'est la Vie.g64` is now the corpus G64 fixture; the two failing files are
recorded here as candidate known-bad fixtures once the behaviour is settled.

### R-012 — `optical info` rejects pure CD-DA discs: "No data track found"

A disc with only AUDIO tracks and no data track cannot be opened:

```
rb-cli optical info Audio-only.cue
  Container:   unknown
  Filesystems: (none recognized)
  warning: unrecognized disc image: No data track found
```

Reproduced exactly, and the rule is clean — presence of a data track is the
only thing that matters:

| cue | tracks | data tracks | result |
|-----|-------:|------------:|--------|
| `Akumajou Dracula MIDI Collection` | 21 | 1 | `bincue` |
| `usbode-audio-sampler` | 2 | 1 | `bincue` |
| `mixedmode-both` (MODE1 + AUDIO) | 2 | 1 | `bincue` |
| `FF_CD` | 5 | **0** | **rejected** |
| `Audio-only` | 1 | **0** | **rejected** |
| synthetic 2-track audio (1.8 MB) | 2 | **0** | **rejected** |

Minimal repro, no fixture needed — two 400-sector silent audio tracks:

```bash
head -c $((800*2352)) /dev/zero > cdda.bin
printf 'FILE "cdda.bin" BINARY\r\n  TRACK 01 AUDIO\r\n    INDEX 01 00:00:00\r\n  TRACK 02 AUDIO\r\n    INDEX 01 00:05:25\r\n' > cdda.cue
rb-cli optical info cdda.cue
```

Why it looks like a real gap rather than a scope decision: `src/optical/cd_audio.rs`
exists specifically to read audio tracks, `optical rip` handles CD-DA, and the
README lists CD-DA support — yet the disc a user is most likely to point at
(an audio CD) is the one shape `optical info` refuses. Mixed-mode discs work
fine, so this is narrowly about discs with *no* data track.

Consequence for the corpus: pure-audio material is **not** a fixture gap. We
hold `Audio-only.bin/.cue` (35 MB, 1 track) and `FF_CD.bin/.cue` (130 MB,
5 tracks) already. They cannot be admitted as fixtures until this is decided,
because a fixture that no verb can open proves nothing.

### R-013 — UFS directories are reported as files, with a garbage size

Reading the root of an installed Solaris disk (Sun label, UFS slice):

```
rb-cli ls disk-0.qcow2@1 /
  DIR            0             lost+found
  FILE  14989422569311248440   usr
  FILE           0             export
  FILE           0             var
  FILE           9             bin
  FILE           0             etc
  ...
```

Two defects in one listing:

1. **Every directory except `lost+found` is typed `FILE`.** `usr`, `var`,
   `etc`, `bin`, `dev`, `kernel`, `lib`, `platform`, `export` are all
   directories on any Solaris root.
2. **`usr` reports a size of 14,989,422,569,311,248,440 bytes** — about
   1.5e19, far past any plausible value and consistent with an unrelated
   field being read as the size, or a sign/endianness slip.

`lost+found` coming out right is the interesting detail: it is the first
entry in the directory, which points at the walk going wrong *after* the
first record rather than at a wholesale layout misread.

Our UFS fixtures to date (`test_ufs1.img`, `test_ufs2.img`) are 16 MB images
built by NetBSD `makefs` and are read via `open_filesystem` directly, so
neither the Solaris on-disk variant nor this listing path was ever exercised.
This is the first time real vendor UFS has been read, and it found a bug on
the first directory listed — a good argument for the real-world fixtures
generally.

Fixture: `part.sun.solaris-disk.multipart` (annex).

### R-002 — `src/fs/README.md` capability table is stale

It still lists ext2/3/4 as "No (planned)" for browsing, compaction and
resize, and covers only six partition type bytes. The engine implements ext
fully (`src/fs/ext.rs`, `ext_format.rs`, `ext_fsck.rs`, `ext_csum.rs`) plus
about forty other filesystems. The table has been overtaken; either regenerate
it from `fs/mod.rs` dispatch or delete it and point at the README.

---

## Axis 1 — Filesystems (42)

Each family needs, where the engine claims the capability: **browse**,
**read**, **edit**, **shrink/expand**, **fsck**. The README table already
records which of the five apply per family; the harness reads that
intent from the case manifests, and a case asserting a capability the engine
does not claim is a manifest bug.

FAT12 · FAT16 · FAT32 · exFAT · NTFS · HPFS · ext2/3/4 · SquashFS 4.0 ·
HFS · HFS+/HFSX · MacPlus MFS · APFS · btrfs · JFS2 · ReiserFS v3 ·
Minix V1/V2/V3 · UFS/FFS (UFS1, UFS2) · ProDOS · Apple DOS 3.3 ·
UCSD p-System · Apple Lisa FS · CBM DOS (1541/1571/1581/8050/8250) ·
Atari DOS 2 (2.0S, 2.5) · RS-DOS · OS-9 / NitrOS-9 RBF · DragonDOS ·
Acorn DFS · Acorn ADFS / FileCore · TI-99 · TR-DOS · Oric Jasmin ·
Human68k (FAT12, FAT16) · CP/M 2.2 / 3 / Plus · QDOS (QXL.WIN) ·
AFFS (OFS, FFS) · PFS3 / PDS3 / muFS · SFS · SGI EFS · SGI XFS v4/v5 ·
Alto BFS / TFS · Pilot / Cedar · Carve (raw recovery)

**Sub-variants that need their own cases, not just one per family:**

- FAT: 12/16/32 are separate on-disk shapes; plus superfloppy vs partitioned.
- CP/M: every DPB in `src/fs/cpm_diskdefs.rs` is effectively a distinct
  geometry. This is the single largest sub-axis in the suite.
- CBM DOS: five drive models, plus GCR (`.g64`/`.g71`) vs sector (`.d64`).
- ext: ext2, ext3 and ext4 differ enough (journal, extents, metadata csum)
  to need three.
- UFS1 vs UFS2, and the two superblock offsets NetBSD vs FreeBSD produce.
- HFS+ vs HFSX vs the HFS-wrapper case; journalled vs not.
- AFFS: OFS vs FFS, plus international and dir-cache variants.
- XFS v4 vs v5.

## Axis 2 — Container / image formats (47)

Read, write and convert. Formats the engine only reads get read + convert-out
cases; formats it writes additionally get a round-trip identity case.

Raw · Fixed VHD · Dynamic VHD · QCOW2 · VMDK · Zstd stream · Gzip stream ·
LZ4 stream · cb-dos container · CHD · AppImage · Norton Ghost · WinImage ·
ZIP (raw disk) · BasiliskII HFV · Apple 2MG · Apple II DSK · Disk Copy 4.2 ·
Apple DMG · Self-mounting / NDIF · Apple sparse image · WOZ · MOOF · DART ·
Amiga ADF/HDF · gzip-wrapped ADZ/HDZ · Atari MSA · CPCEMU DSK/EDSK ·
Commodore disk · Commodore GCR · Atari disk · CoCo disk · Acorn DFS disk ·
ZX Spectrum TR-DOS · TI-99/4A disk · Sharp D88 · X68000 XDF · X68000 HDD ·
PC-98 HDM · DiskExplorer DIM · Xerox Alto pack · Salto disk · ContrAlto2 ·
Diablo Trident pack · Xerox Pilot/Cedar volume · Dwarf 6085 disk ·
Raw physical disk

Special cases that need dedicated assertions:

- **Encrypted containers**: Ghost password (`gho_crypto.rs`), DMG
  (`dmg_crypto.rs`), IMZ (`imz.rs`), APFS (`apfs_crypto.rs`). Each needs a
  correct-password case and a wrong-password case that fails cleanly.
- **CHD**: single-file layout only. A case must assert we never emit
  per-partition CHDs, and that `chdman info` opens the result (tier 6).
- **HFV**: classic HFS only, 2047 MB ceiling. Needs a boundary case at
  2047 MB and a rejection case above it.

## Axis 3 — Optical filesystems (10)

ISO 9660 (+ Joliet, Rock Ridge) · High Sierra · UDF · HFS / HFS+ ·
SGI EFS · UFS/FFS · VMS ODS-2 / Files-11 · GameCube / Wii · CD-i ·
3DO Opera

Plus the optical verb surface: `optical boot extract|replace`, `browse`,
`convert`, `drives`, `du`, `extract`, `info`, `new sgi-efs`, `rip`.
CD-DA and mixed-mode discs are their own sub-axis.

## Axis 4 — Partition schemes (9)

MBR · GPT · APM · RDB · SGI volume header · Sun disk label · Atari AHDI ·
Sharp X68000 · None (superfloppy)

Per scheme: parse via `inspect`, export, and the full `partmap` editor
surface (`add`, `apply`, `delete`, `move`, `resize`, `set-bootable`,
`set-type`). Alignment detection (DosTraditional, Modern1MB, Custom, None)
is asserted on the schemes where it applies.

**`show partmap` is APM-only** (`docs/cli-reference.md`: "APM-only today"), so
it is *not* the per-scheme assertion — `inspect` is. On any non-APM image
`show partmap` fails with `parsing APM: Invalid APM: bad DDR signature`,
including on a plain superfloppy. That is documented behaviour rather than a
bug, but the message names APM as though the image were malformed instead of
saying the verb does not support this scheme, which is worth a one-line fix
whenever someone is next in that file.

## Axis 5 — CLI verbs (119 entries)

Every `###` heading in `docs/cli-reference.md` gets at least:

1. a **help** case (`--help` exits 0 and mentions the documented flags),
2. a **happy path** case,
3. a **usage-error** case asserting exit code 2,
4. a **not-found** case asserting exit code 3 where a path argument exists.

Top-level verbs: `api` (apm/hfs/sgi) · `archive` · `backup` · `batch` ·
`batch-template` · `bless` · `cbk` · `chmeta` · `chmod` · `chown` ·
`completions` · `config` · `convert` · `cp` · `du` · `expand` · `floppy` ·
`fsck` · `get` · `get-binhex` · `grow` · `import` · `inspect` ·
`install-completions` · `locate` · `ls` · `mac-scsi-bless` ·
`make-bootable` · `menu` · `mkdir` · `new` · `optical` · `partmap` · `put` ·
`put-binhex` · `put-macbinary` · `reformat` · `repack` · `resize` ·
`restore` · `rm` · `serve` · `setrsrc` · `setvolname` · `show` · `shrink` ·
`squashfs` · `tar` · `terminal` · `tui` · `untar` · `update` · `write` ·
`xattr`

Excluded from automated coverage, by design: `tui`, `terminal`, `menu`
(interactive), `serve` (covered by a dedicated daemon case, not the verb
matrix), `update` (network side effects), `install-completions` /
`completions` (get help-only cases).

## Axis 6 — Output formats

`--format text|json|yaml|csv|tsv` per query verb, including the documented
behaviour that CSV/TSV on a nested-result verb exits with `USAGE_ERROR` (2).
The JSON envelope (`schema_version`, `status.error`, `status.code`,
`status.message`, `result`) is validated on every structured invocation the
harness makes, so envelope regressions surface everywhere at once.

## Axis 7 — Platforms

Windows 10 · Linux · macOS (modern), then `rb-cli-vintage`
(macOS 10.7 / Windows 7) and `rb-cli-ppc`. Per-platform-only surface: raw
device paths (`\\.\PhysicalDriveX`, `/dev/sdX`, `/dev/diskX`), elevation,
volume locking/dismounting, and file-association registration.

---

## Assertion vocabulary

The operators a case manifest may use. Keep this list small and closed; a
case needing something outside it is a signal the harness needs a new
first-class operator, not a shell escape.

| Operator | Meaning |
|----------|---------|
| `exit` | Process exit code equals an expected value from `src/cli/exit.rs`. |
| `envelope` | Output parses as the JSON envelope and `status.error` matches. |
| `json_path` | A JSONPath-ish selector into `result` equals / matches / exists. |
| `stdout_contains` / `stdout_matches` | Text-mode output assertions. |
| `stderr_empty` | Nothing on stderr for a successful run. |
| `file_exists` / `file_size` | Produced artifact checks. |
| `file_sha256` | Exact artifact identity, for deterministic outputs. |
| `files_identical` | Two produced files are byte-identical (round-trips). |
| `image_roundtrip` | Convert A->B->A reproduces the original bytes. |
| `fsck_clean` | A follow-up `rb-cli fsck` reports no issues. |
| `oracle` | An external tool accepts the artifact (tier 6). |
| `timeout` | Case must complete within a wall-clock budget. |

## Deliberate exclusions

Recorded so a future run does not re-litigate them:

- Interactive surfaces (`tui`, `terminal`, `menu`, GUI dialogs) are out of
  scope for the CLI harness by construction.
- `update` is excluded to avoid network dependence and release-feed flakiness.
- Anything requiring a fixture that cannot be redistributed stays a
  `skip-fixture` on hosts without a local copy; the catalog records the
  redistribution status per fixture.
