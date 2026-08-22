# Command coverage audit

**The live numbers are computed, not written here:**

```
rb-regress query verbs
```

It parses `rb-cli --help` for the verb list and every `args` array under
`cases/`, then prints what is invoked, how often, and what is not. The binary
is the only authority on which verbs exist, so asking it is the only way this
does not drift — and it drifted twice before the query existed. It also
reports anything invoked that is *not* a verb, which catches a typo in a case
that would otherwise just be an exit-2 failure someone has to read.

As of 2026-08-08 it reports **45 of 54**. The narrative below is kept for the
before/after, which is the part a number cannot carry; treat every count in it
as a snapshot of when it was written.

## After

| | before | after |
|---|---:|---:|
| cases | 87 | 170 |
| verbs invoked | 11 | 41 |
| backup -> restore -> compare | none | 14 cases, 8 filesystems, 4 formats |
| edit surface (put/get/rm/mkdir/cp/tar/xattr/chmod/...) | none | **18/18 advertised filesystems** |
| resize (grow/resize/repack/expand/shrink) | 1 | 9 cases |
| catalogued fixtures used | 26/84 | 41/84 |

Six findings came out of the first run: R-021 (`resize --size` is a silent
no-op), R-022 (HPFS is the only filesystem that fails a sector-by-sector
round-trip), R-023 (`repack` loses every file), R-024 (one `put` makes an AFFS
volume fail its own fsck), R-025 (`squashfs put` cannot replace the image on
Windows), R-026 (`show partmap` cannot read an SGI disk `inspect` reads fine).

### Edit coverage is now complete against what README advertises

README lists eighteen filesystems with edit mode. The first tier-3 pass
covered nine; a follow-up pass covered the other nine — exFAT, PFS3, SFS,
Apple DOS 3.3, MacPlus MFS, UFS, CP/M, Human68k and XFS.

Four of those have **no builder at all**, so their cases edit a
`{fixture_copy}` rather than a fresh volume: exFAT (formats.toml calls a
reference fixture mandatory), PFS3, SFS and UFS. The copy is not a
convenience — an edit case mutates the volume, and a reference fixture a test
has written to has stopped being a reference.

**PFS3 has no catalogued fixture either.** The only PFS3 volumes on hand are
inside AmigaVision.hdf (9.1 GB, on the MiSTer) and 2.1-HD.hdf. Its case is
written and will report `skip-fixture` until one is catalogued — the skip is
the reminder, which is better than the case not existing.

**SFS is the case the FS-UAE oracle work was aiming at.** rb-cli cannot write
an SFS volume from scratch, so editing the reference volume and checking the
result is the only way to exercise the SFS editor at all.

### Fixture usage was the bigger miss

58 of the 84 catalogued fixtures were referenced by nothing. That was never
measured because the audit above counted *commands*, and a suite can invoke
every verb while still testing them all against volumes it made itself.

Two things fell out of checking:

* **`fs.pfs3.rdb-cd32saves.hd` existed all along.** An earlier draft claimed no
  PFS3 fixture was catalogued. That claim came from reading the case files
  rather than the catalogue — the wrong direction to check in, and the same
  shortcut that produced the "no L: directory anywhere" mistake during the
  FS-UAE work.
* **Two real AFFS volumes were sitting unused** (`fs.affs.workbench13.hd`,
  `fs.affs.ffs-intl-cd32.hd`). The reasoning at the time was R-020 — our AFFS
  formatter emitted volumes no Amiga would mount, and our own fsck agreed with
  the formatter rather than with reality, so an AFFS editor tested only against
  our own output proved very little. `cases/tier3/edit-real-volumes.toml` now
  runs the same put/get/fsck round-trip against third-party volumes for AFFS,
  NTFS, ext2/4, FAT16/32, HFS+, HFS, HFV, ProDOS, CP/M, Human68k, Apple DOS
  and EFS.

  R-020 was fixed and closed on 2026-08-14 (Kickstart 3.1 mounts our volumes
  Read/Write), so the premise no longer holds — but the conclusion does, and
  more strongly for having been tested: third-party volumes are what caught
  R-030 and R-013, and testing an editor only against its own writer is the
  circularity that let R-020 stand for a week. `workbench13` earned its keep
  twice more since, as the R-038 control and as the FS-UAE oracle's boot
  volume.

43 remain unused, mostly optical discs and the exotic end (Alto, Lisa, Xerox
D0, 3DO, GameCube, CD-i). Those are read-path fixtures whose cases belong in
tier 2, not here.

### Still not invoked, and why

Each of these is a decision, not an oversight:

| verb | why not |
|------|---------|
| `menu`, `terminal`, `tui`, `edit` | interactive; no scriptable surface |
| `completions`, `install-completions` | shell integration, not disk behaviour |
| `update` | reaches the network — would make the suite non-deterministic and fail offline |
| `serve` | a daemon; needs a client and a lifecycle the harness cannot drive yet |
| `write` | physical media; needs `--device-allowlist` (does not exist) and a dedicated throwaway device |

Two entries that used to be on that list were closed on 2026-08-08:

| verb | how |
|------|-----|
| `batch` | `batch-template --out` writes the script to a file, so the two chain inside one case. Without `--out` the template goes to stdout and a case cannot redirect it. |
| `mac-scsi-bless` | needs an APM disk, not a real Mac: `expand` writes one. `show partmap` is the read-back — an `Apple_Driver43` entry and `drivers=1` in the DDR. |

Of what remains, only `write` and `serve` are gaps worth closing. The rest are
outside what a headless harness can meaningfully assert.

---

# The original audit — 2026-08-07

What fraction of rb-cli the suite actually exercises, measured rather than
estimated. Generated by cross-referencing `rb-cli --help` against every `args`
array in `cases/**/*.toml` and `data/produce.toml`.

> **Superseded 2026-08-17.** The measurement below is from 2026-08-07, when the
> suite was 87 cases across tiers 0-2. It is now 295 across tiers 0-3 and 5, and
> the specific complaint — that everything clustered on read/create paths — was
> the reason tier 3 (edit, resize, subcommand sweep) and tier 5 (backup/restore
> round-trips) were written. `serve` and `write` are the verbs still genuinely
> uncovered: `serve` needs a daemon lifecycle the case schema cannot drive (its
> integration tests live in `tests/remote_filesystem.rs` instead), and `write`
> needs a hardware allowlist that does not exist. The rest of this section is
> kept as the measurement that motivated the work.

**Headline (2026-08-07): 11 of 54 top-level verbs are invoked. 43 are never run
by anything.**

That is a worse number than the case count suggests, because the 87 cases
cluster heavily: `inspect` (43), `new` (39) and `fsck` (23) account for most of
them, and they are all *read* or *create* paths.

## Exercised

| verb | invocations | what is actually covered |
|------|------------:|--------------------------|
| `inspect` | 43 | broad — the best-covered verb |
| `new` | 39 | broad across filesystems; the suite's strongest area |
| `fsck` | 23 | mostly `--checkonly` straight after `new` |
| `ls` | 9 | browse, several filesystems |
| `backup` | 7 | **one flag combination only** — see below |
| `convert` | 7 | container round-trips via produce recipes |
| `optical` | 6 | `info` on a few disc shapes |
| `restore` | 1 | **`restore --help`. Nothing functional.** |
| `expand` | 1 | one path |
| `get` | 1 | single file extract |
| `partmap` | 1 | one path |

## Never invoked by any case or recipe

```
archive  batch  batch-template  bless  cbk  chmeta  chmod  chown
completions  config  cp  du  edit  floppy  get-binhex  grow  import
install-completions  locate  mac-scsi-bless  make-bootable  menu  mkdir
put  put-binhex  put-macbinary  reformat  repack  resize  rm  serve
setrsrc  setvolname  show  shrink  squashfs  tar  terminal  tui  untar
update  write  xattr
```

## The gaps that matter most

**1. Backup/restore has no round-trip.** This is the product's headline
feature and its name. All six functional `backup` calls are
`--format raw --sector-by-sector`, and all six exist to demonstrate F-008
(container sources being rejected). Never tested: zstd / CHD / VHD / gzip / lz4
output, checksum verification (CRC32 and SHA256), split backups, and
`cbk` incremental. `restore` is only ever asked for its help text, so **no test
has ever restored anything**, let alone compared the result to the source.

**2. The entire edit surface is untested.** README advertises edit mode on
about twenty filesystems. Not one of `put`, `rm`, `mkdir`, `cp`, `edit`,
`chmod`, `chown`, `xattr`, `setvolname`, `setrsrc`, `chmeta`, `tar`, `untar`
is invoked anywhere. Every write path beyond "format a blank volume" is
unverified — including the AFFS and SFS editors, and the SquashFS rebuild
that R-014 was about.

**3. Resize is untested.** `resize`, `shrink` and `grow` are never invoked;
`expand` runs once. Partition resizing with alignment preservation is a stated
design goal in CLAUDE.md and has no coverage at all.

**4. Physical media is untested and cannot currently be tested.** `write` is
never invoked. `--allow-hardware` exists as a flag but gates nothing, because
no hardware cases are written (RUNBOOK § Known limitations already says this).
`HARDWARE.md` describes a `--device-allowlist` that does not exist.

**5. Remote is untested.** `serve` — the remote/agent surface — is never
invoked. Neither is the ssh transport the planner assumes for reaching
linuxbox, mac, the G5 and the MiSTer; `scripts/regress-all.sh` drives those
hosts from the outside instead.

**6. The optical and floppy container surfaces are barely touched.** `optical`
has 9 subcommands and only `info` is used; `floppy convert` is never run
despite five container formats (`d88`, `xdf`, `hdm`, `dim`) declaring builders
in `formats.toml`.

## Why this happened, structurally

The suite was built to tiers 0-2 and those tiers are, by design, about
*reading* and *creating*:

- tier 0 — the CLI contract (exit codes, envelopes)
- tier 1 — rb-cli builds a volume and reads it back
- tier 2 — rb-cli reads third-party reference fixtures

Tiers 3-7 were specified in `PLAN.md` and never written, and those are exactly
where round-trips, edits, resizes and hardware live. So the gap is not an
oversight in the case files; it is the part of the plan that was never built.

`formats.toml` knows 121 formats and `produce.toml` has recipes for 35, which
is the same shape of gap measured a different way.

## Suggested order

1. **backup -> restore -> compare round-trip**, one filesystem, one format.
   Nothing else in this list is worth more, and it is the shape every other
   round-trip test will copy.
2. **Edit round-trips**: `put` a file, `ls` it back, `fsck` clean. Cheap, and
   it covers a surface currently at zero across twenty filesystems.
3. **Resize**: shrink and grow, then fsck, then confirm contents survive.
4. **Backup format matrix**: each output format, with checksum verification.
5. **Physical media**: needs `--device-allowlist` built first, and a dedicated
   throwaway device.
6. **`serve` / remote**: needs a decision on whether it is in scope at all.
