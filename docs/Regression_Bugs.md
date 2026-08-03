# Regression Findings (R-001 … R-013)

Defects and documentation drift turned up while building the regression suite
(`regression-tests/`), 2026-08-01/02. **None of these have been fixed** — the
suite work was deliberately kept separate from bug fixing, and nothing outside
`regression-tests/` was modified while finding them.

Every entry was reproduced against `target/release/rb-cli` on Windows. Where a
finding depends on a fixture, the fixture is named.

## Summary

| ID | Severity | Area | Finding |
|----|----------|------|---------|
| [R-008b](#r-008b) | **High** | `src/fs/affs.rs` | `new volume affs --size 4M` panics; no file produced, exit 101 |
| [R-007](#r-007) | **High** | `src/fs/ntfs_format.rs` | Freshly formatted NTFS fails its own fsck, incl. `BackupBootMismatch` |
| [R-009](#r-009) | **High** | `src/partition/mod.rs` | Bare JFS / UFS1 / UFS2 / ReiserFS images cannot be opened at all |
| [R-013](#r-013) | **High** | `src/fs/ufs.rs` | Solaris UFS directories reported as files, one with a garbage size |
| [R-005](#r-005) | Medium | `src/cli/output.rs` | No error envelope emitted under `--format json` |
| [R-008a](#r-008a) | Medium | `src/fs/affs.rs` | AFFS volumes above 4066 blocks have uncovered tail blocks |
| [R-012](#r-012) | Medium | `src/optical/` | `optical info` rejects any disc with no data track (pure CD-DA) |
| [R-003](#r-003) | Medium | `src/cli/output.rs` | Docs claim `ls` supports `--format`; it does not |
| [R-010](#r-010) | Medium | `src/cli/verbs/inspect.rs` | `inspect` has no `--fs-type`, so CP/M images cannot be inspected |
| [R-006](#r-006) | Medium | `src/cli/verbs/new.rs` | `new volume prodos` always fails with default arguments |
| [R-004](#r-004) | Low | `src/cli/output.rs` | CSV/TSV rejection exits 1, documented as 2 |
| [R-011](#r-011) | Unknown | `src/rbformats/` | G64 decoding fails on copy-protected / patched dumps |
| [R-001](#r-001) | Doc | `README.md` | Partition-table list missing AHDI and X68000 |
| [R-002](#r-002) | Doc | `src/fs/README.md` | Capability table stale — ext listed as "planned" |

---

## High

### R-008b — `new volume affs` panics at 4 MB and above {#r-008b}

AFFS puts the root block at the volume midpoint. Once that index passes the
single bitmap page the "mark in-use" loop indexes past the 512-byte block.
Unlike the loop immediately below it, this one has no `word_idx >= 128` guard.

```
rb-cli new volume affs --size 4M v.img
  thread 'main' panicked at src/fs/affs.rs: range end index 516 out of range
  for slice of length 512
  -> exit 101, no file created
```

The arithmetic matches exactly:

| size | blocks | root | bit | word_idx | slice access | message |
|------|-------:|-----:|----:|---------:|--------------|---------|
| 4 MB | 8192 | 4096 | 4094 | 128 | `bm[512..516]` | "range end index 516" |
| 8 MB | 16384 | 8192 | 8190 | 256 | `bm[1024..1028]` | "range start index 1024" |

Works up to 3 MB; broken at 4 MB and above. A user asking for an ordinary
4 MB Amiga volume gets a stack trace and no file. Exit 101 is not in the
exit-code contract (`src/cli/exit.rs`) at all.

### R-007 — a freshly formatted NTFS volume fails its own fsck {#r-007}

```
rb-cli new volume ntfs --size 2M v.img
rb-cli fsck v.img --checkonly
  ERROR [BitmapLeaked] 16 cluster(s) marked allocated in $Bitmap but referenced by nothing
  ERROR [BackupBootMismatch] backup boot sector (last sector) does not match the VBR
  -> exit 1
```

Either the formatter or the fsck is wrong; they cannot both be right, and
`format -> fsck` on an untouched volume must be clean.

`BackupBootMismatch` is the more serious half: real NTFS keeps a copy of the
VBR in the volume's last sector and Windows checks it. A volume whose backup
boot sector does not mirror the VBR is not one `chkdsk` will accept, which
undercuts the point of writing NTFS.

### R-009 — bare JFS / UFS / ReiserFS images cannot be opened {#r-009}

```
rb-cli inspect test_jfs.img
  -> error: detecting partition table: Invalid MBR: invalid boot signature:
     expected 0xAA55, got 0x0000
```

Same for `test_ufs1.img`, `test_ufs2.img`, `test_reiserfs_v3_6.img`.

Not a general superfloppy failure — the neighbouring cases work:

| filesystem | bare-image `inspect` |
|------------|----------------------|
| btrfs, exFAT, APFS, XFS v4/v5 | works |
| ext, NTFS, HFS, HFS+, HPFS, Minix V2/V3, AFFS, ProDOS, EFS, FAT | works |
| **JFS, UFS1, UFS2, ReiserFS** | **fails** |

`detect_superfloppy` (`src/partition/mod.rs`) probes magics for SquashFS, the
Xerox Alto family, Pilot/Cedar, Dwarf, Lisa, NTFS, exFAT, XFS, ext and btrfs.
JFS (`JFS1` at 32768), UFS (`0x011954` at 8192 or 65536) and ReiserFS
(`ReIsEr2Fs`) are simply absent.

Why it went unnoticed: the `cargo test` suites for these four call
`open_filesystem` directly and never route through partition-table detection,
so the drivers are well covered while the path a user actually takes is not.

User impact: anyone who dumps a raw JFS/UFS/ReiserFS partition and points
rb-cli at it gets an MBR error, with no hint the filesystem is supported.

### R-013 — UFS directories reported as files, with a garbage size {#r-013}

Reading the root of an installed Solaris disk (Sun label, UFS slice):

```
rb-cli ls disk-0.qcow2@1 /
  DIR            0             lost+found
  FILE  14989422569311248440   usr
  FILE           0             export
  FILE           0             var
  FILE           9             bin
  FILE           0             etc
```

Two defects in one listing:

1. Every directory except `lost+found` is typed `FILE`. `usr`, `var`, `etc`,
   `bin`, `dev`, `kernel`, `lib`, `platform`, `export` are all directories.
2. `usr` reports 14,989,422,569,311,248,440 bytes — about 1.5e19, consistent
   with an unrelated field read as the size, or a sign/endianness slip.

`lost+found` being correct is the useful detail: it is the first entry, which
points at the walk going wrong *after* the first record rather than a
wholesale layout misread.

Existing UFS fixtures are NetBSD `makefs` images read via `open_filesystem`
directly, so neither the Solaris on-disk variant nor this listing path was
ever exercised. Fixture: `part.sun.solaris-disk.multipart` (annex).

---

## Medium

### R-005 — no error envelope under `--format json` {#r-005}

`src/cli/output.rs` documents that on failure the envelope still returns, with
`status.error: true`, `status.code` carrying the exit code, `status.message` a
short description and `result` null.

Observed: a failing verb under `--format json` writes **nothing** to stdout
and a plain-text error to stderr.

```
rb-cli inspect ./missing.img --format json
  -> exit 1, stdout empty, stderr: "error: open ./missing.img: ..."
```

A JSON consumer gets no parseable output on exactly the path the envelope
exists to serve.

### R-008a — AFFS tail blocks above 4066 are uncovered {#r-008a}

One AFFS bitmap block covers `(512 - 4) / 4 * 32 = 4064` bits, addressing
blocks 2..4065. `src/fs/affs.rs` writes exactly one, commented "the bitmap
covers a full 4064-bit page even if the volume is smaller" — true only while
the volume *is* smaller.

A 2 MB volume is 4096 blocks, so 4066..4095 fall outside the page:

```
ERROR [AffsBitmapMismatch] 30 block(s) marked allocated but unreachable
WARN  [AffsOrphanBlock]    30 block(s) unreachable from root: [4066, 4067, ...]
```

Confirmed as a boundary: at `--size 2033K` (exactly 4066 blocks) fsck is
clean; every larger size fails.

Same root cause as R-008b. Both need the bitmap to span as many blocks as the
volume requires, chained through the root block's `bm_pages` slots, with the
marking loop bounded.

### R-012 — `optical info` rejects discs with no data track {#r-012}

```
rb-cli optical info Audio-only.cue
  Container:   unknown
  Filesystems: (none recognized)
  warning: unrecognized disc image: No data track found
```

The rule is exactly "has a data track", nothing subtler:

| cue | tracks | data tracks | result |
|-----|-------:|------------:|--------|
| Akumajou Dracula MIDI Collection | 21 | 1 | `bincue` |
| usbode-audio-sampler | 2 | 1 | `bincue` |
| mixedmode-both (MODE1 + AUDIO) | 2 | 1 | `bincue` |
| FF_CD | 5 | **0** | **rejected** |
| Audio-only | 1 | **0** | **rejected** |
| synthetic 2-track audio | 2 | **0** | **rejected** |

Minimal repro, no fixture required:

```bash
head -c $((800*2352)) /dev/zero > cdda.bin
printf 'FILE "cdda.bin" BINARY\r\n  TRACK 01 AUDIO\r\n    INDEX 01 00:00:00\r\n  TRACK 02 AUDIO\r\n    INDEX 01 00:05:25\r\n' > cdda.cue
rb-cli optical info cdda.cue
```

`src/optical/cd_audio.rs` exists specifically to read audio tracks and
`optical rip` handles CD-DA, so the disc a user is most likely to point at is
the one shape `optical info` refuses. Mixed-mode is unaffected.

### R-003 — docs claim `ls` supports `--format`; it does not {#r-003}

`src/cli/output.rs` lists `ls` among the verbs that "can emit their results in
one of five formats", and its CSV/TSV note says those formats apply to "`ls`,
`show partmap`, `show devices`, `fsck` issue lists".

`rb-cli ls --help` exposes no `--format`, and it is not global either — both
`rb-cli ls img --format json` and `rb-cli --format json ls img` exit 2 with
"unexpected argument". Verified present on `inspect`, `fsck`, `du`, `locate`.

Either the flag was dropped and the doc is stale, or structured `ls` output is
a real gap. `ls` is among the most script-facing verbs in the CLI.

### R-010 — `inspect` has no `--fs-type` {#r-010}

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
`MANIC.BAS` / `MANIC.BIN`, so the engine is fine — `inspect` cannot reach it.
Every CP/M disk (nine DPB presets) is therefore un-inspectable.

### R-006 — `new volume prodos` fails with default arguments {#r-006}

The default volume name is `rusty-backup`. ProDOS forbids `-`, so the default
is invalid for that filesystem and the verb always fails:

```
rb-cli new volume prodos --size 2M vp.img
  -> error: invalid data: filename contains '-' — ProDOS allows only letters
     (A-Z), digits (0-9), and '.'; rename the file
```

`--name TESTVOL` succeeds, so the builder is fine — only the default is
unusable. The message compounds it by saying "rename the file" when the
offending string is the volume name.

Every other `new volume` filesystem accepts the default.

---

## Low / undecided

### R-004 — CSV/TSV rejection exits 1, documented as 2 {#r-004}

`src/cli/output.rs` states nested-result verbs "error out with
`crate::cli::exit::USAGE_ERROR`" — exit 2. Observed exit 1.

```
rb-cli inspect vol.img --format csv   ->  exit 1
error: inspect returns nested data; --format csv only supports flat tabular
results. Use --format json or --format yaml instead.
```

The message is right, the code is not. `require_non_flat`
(`src/cli/output.rs:161`) uses `anyhow::bail!`, which propagates as a generic
failure. The unit test `require_non_flat_rejects_csv_for_nested` only asserts
`is_err()`, so it cannot catch this.

### R-011 — G64 fails on copy-protected / patched dumps {#r-011}

Of three real G64 files, one opened and two failed:

| file | result |
|------|--------|
| `C'est la Vie.g64` (278 KB) | opens |
| `Protector II (must be write protected).g64` (191 KB) | fails |
| `American Express (Magic Disk 64 1989-09 Side 2) [patched].g64` (278 KB) | fails |

Both failures are copy-protected or patched dumps with non-standard GCR, and
both report the generic `Invalid MBR: invalid boot signature` rather than
anything GCR-specific.

**Needs a decision before it can be graded.** Preserving copy-protected disks
is the reason G64 exists rather than D64, so partial support may be a real
limitation rather than an acceptable boundary. `.d64` and `.g71` unaffected.

---

## Documentation drift

### R-001 — README partition-table list missing two schemes {#r-001}

`src/partition/mod.rs` defines nine `PartitionTable` variants: `Mbr`, `Gpt`,
`Apm`, `Rdb`, `Sgi`, `Sun`, `Ahdi`, `X68k`, `None`. README § Partition tables
lists only MBR, GPT, APM, RDB, SGI, Sun and "None (superfloppy)" — **Atari
AHDI and Sharp X68000 are absent**, despite both being fully implemented
(`src/partition/atari.rs`, `src/partition/x68k.rs`) and both having CLI
builders (`rb-cli new hd x68k`).

Exactly the drift class CLAUDE.md's pre-commit documentation-sync section
warns about.

### R-002 — `src/fs/README.md` capability table is stale {#r-002}

Still lists ext2/3/4 as "No (planned)" for browsing, compaction and resize,
and covers only six partition type bytes. The engine implements ext fully
(`src/fs/ext.rs`, `ext_format.rs`, `ext_fsck.rs`, `ext_csum.rs`) plus about
forty other filesystems. Either regenerate from `fs/mod.rs` dispatch or delete
it and point at the README.

---

## Suggested order

1. **R-008b** — a panic with no file produced is the worst failure mode here,
   and R-008a shares its fix.
2. **R-009** — smallest change of the high-severity set; unlocks four
   filesystems' worth of tier-2 coverage.
3. **R-007** — `BackupBootMismatch` means our NTFS is not what Windows
   expects.
4. **R-013** — wrong entry types and an absurd size are user-visible
   immediately.
5. **R-005**, **R-004**, **R-003** — the CLI contract group; cheap, and the
   regression harness depends on that contract being true.
6. **R-006** — a one-line default change.
7. **R-001**, **R-002** — fold into the next docs commit.
8. **R-011** — decide scope first.
