# Fixture Policy and Catalog

## The rule

**No fixture file, and no path to a fixture file, is ever committed to this
repository.** Case manifests reference fixtures by *logical ID* only. The
mapping from ID to a real file lives on the NAS.

This keeps the repo small, keeps redistribution-restricted material out of
git history, and lets different hosts hold different subsets of the corpus
without the manifests changing.

Note this is a different policy from `tests/fixtures/`, which *does* commit
small synthetic `.zst` fixtures for `cargo test`. Those stay where they are.
The regression corpus is the large, real-world, non-redistributable set.

## Locations

| What | Where |
|------|-------|
| Corpus | `\\NAS\share\rb-fixtures\fixtures\` |
| Catalog | `\\NAS\share\rb-fixtures\fixture-map.tsv` |
| Catalog (machine form) | `\\NAS\share\rb-fixtures\fixture-map.json` |
| Gap list | `\\NAS\share\rb-fixtures\missing-fixtures.md` |
| Run reports | `\\NAS\share\rb-fixtures\runs\` |
| Inventory scans | `\\NAS\share\rb-fixtures\scans\` |

The runner locates the corpus through, in order: `--fixture-root`, the
`RB_FIXTURE_ROOT` environment variable, then a gitignored
`regression-tests/local.toml`. It never hardcodes a path.

## Logical ID scheme

Lowercase, dot-separated, stable forever. Renaming an ID is a breaking change
to every manifest that references it.

```
<axis>.<family>.<variant>.<shape>
```

- `axis` — one of `fs`, `fmt`, `optical`, `part`, `hw`
- `family` — the filesystem / container / scheme, matching `COVERAGE.md`
- `variant` — the meaningful sub-variant, or `base`
- `shape` — `floppy`, `hd`, `cd`, `superfloppy`, `multipart`

Examples:

```
fs.fat16.dos622.hd
fs.fat12.base.floppy
fs.cpm.amstrad-data.floppy
fs.affs.ffs-intl.hd
fs.hfsplus.journalled.hd
fs.xfs.v5.hd
fmt.chd.single-file.hd
fmt.gho.password.hd
fmt.woz.base.floppy
optical.iso9660.rockridge.cd
part.ahdi.xgm-chain.multipart
part.x68k.scsi.multipart
```

## Minimum corpus

The inventory scan turned up **6,400 candidate files totalling ~2 TB** across
the four sources. Almost none of that should become a fixture. The corpus has
to be copyable to a laptop, a Linux box, a Mac and eventually a PowerPC
machine, so the governing question is not "what could we test with" but
"what is the smallest set that still proves the matrix".

### The provenance rule — read this first

**No fixture may be produced by rusty-backup.** A fixture exists to be an
independent opinion about what the bytes should look like; one we generated
ourselves is not independent, and a bug on both the write and read side
cancels out silently. See `README.md` § "Why tier 1 is not coverage".

Acceptable provenance, best first:

1. **Real-world vendor media** — install CDs, shipped game disks, images
   pulled off working hardware. Proves compatibility with what users
   actually own, quirks included.
2. **The canonical third-party formatter** — `mkfs.fat`, `mkfs.jfs`,
   `mkfs.reiserfs`, `makefs`/`newfs`, `mkfs.cpm`, Apple Disk Utility,
   `chdman`. These *are* the reference implementations, so an image one
   produces is a legitimate oracle.
3. **Never `rb-cli`.** Not `new`, not `reformat`, not `convert`.

Audited 2026-08-02: every generator under `scripts/` uses a third-party tool
(`mkfs.fat`, `mkfs.cpm`, `mkfs.jfs`, `mkfs.reiserfs`, `makefs`, `newfs`).
The committed corpus is clean. Keep it that way — a new generator that
shells out to `rb-cli` is a bug in the test suite, not a shortcut.

Record the producing tool and its version in the catalogue `origin` column,
so a future reader can tell a vendor image from a `mkfs` image at a glance.

### What rusty-backup's own builders are still for

`rb-cli new` / `reformat` / `convert` remain useful, just not as fixtures:

- **Tier 1 smoke** — build, read back, catch panics and round-trip
  regressions. Never the sole coverage for a format.
- **Tier 6 subjects** — build an image, then hand it to `fsck.ext4`,
  `chdman info`, `qemu-img check`, `unsquashfs -s`, or mount it on the real
  OS. That is a genuine test of the write path, because the verdict comes
  from somebody else.

The distinction throughout: *rusty-backup output is a thing under test, never
a source of truth.*

### Resolution order

Every case resolves its input through the first of these that can supply it.

**1. A reference fixture already in `tests/fixtures/` — 0 extra bytes.**

**2. A reference fixture on the NAS corpus.**

**3. Generate one with the canonical third-party formatter**, and add the
script to `scripts/` so it is reproducible.

For context, here is what our own builders can construct. This is **not** a
list of things that need no fixture — it is the list of write paths that need
a tier-6 oracle behind them:

| Builder | Reach |
|---------|-------|
| `new volume` | hfs, hfsplus, hfv, fat, ntfs, hpfs, ext, ext3, ext4, affs, prodos, efs, minix2, minix3 |
| `new floppy` | fat, hfs, atari, apple-dos, cpm (9 DPB presets), os9, ucsd, trdos, ti99, oric, mfs, adfs, minix |
| `new hd` | x68k (Human68k + X68000 table), sgi-efs (SGI volume header + EFS) |
| `optical new` | sgi-efs CD |
| `squashfs create` | SquashFS, from any host directory |
| `reformat` | same filesystem set, applied in place |
| `convert --format` | chd, chd-dvd, chd-cd, bincue, vhd, vhd-dynamic, qcow2, vmdk-flat, vmdk-sparse, raw, twomg, woz, moof, dc42 |
| `floppy convert` | xdf, hdm, dim, d88 |
| `backup --format` | zstd, gzip, lz4, raw, VHD, single-file CHD |

That is roughly **26 filesystem families and 18 container formats we can
write**, and 13 of the 14 `new volume` filesystems format at 2 MB
(ext3/ext4 need 8 MB), so building them during a run is cheap.

**Read this as a workload, not a saving.** An earlier revision of this
document counted these as "covered with zero stored bytes". That was wrong:
being able to write a format is not evidence of writing it *correctly*.
Every one of those 26 + 18 now needs

- a **reference fixture** so the read path is checked against somebody
  else's bytes, and
- a **tier-6 oracle** so the write path is checked by somebody else's tool.

So the corpus requirement went up, not down. The compensation is that the
oracle side is mostly free — `fsck.ext4`, `fsck_msdos`, `chdman info`,
`qemu-img check`, `unsquashfs -s`, `cpmtools` and mounting on the host cover
a large share of that list without hosting a single extra byte.

### Step 1 in detail — what the repo already carries

`tests/fixtures/` holds **68 files totalling 4.0 MB**, all zstd-compressed and
already cloned onto every machine that has the source. Most are under 5 KB
(`test_fat16.img.zst` is 842 bytes; `test_cbm_1541.d64.zst` is 196).

`fixture-map.repo.tsv` maps logical IDs onto these via a `repo:` relpath
prefix. Those rows resolve with **no NAS, no network and no configured
fixture root** — a bare clone can run them. 45 IDs are mapped today,
totalling 3.3 MB, and they cover the filesystems with no builder: exFAT,
APFS, btrfs, JFS, ReiserFS, UFS1/2, XFS v4/v5, CBM DOS across five drive
models, RS-DOS, OS-9, DragonDOS, Acorn DFS, CP/M variants, Human68k, Ghost
(including the password path), MSA and AHDI.

### Step 2 in detail — what the NAS corpus still has to supply

What remains after the repo fixtures is genuinely short:

| Still needed | Why | Candidate source |
|--------------|-----|------------------|
| Apple Lisa FS | no builder, no committed fixture | — |
| PFS3, SFS | builders exist inside the engine for tests but are not exposed on the CLI | promote the internal `create_blank_pfs3` / `create_blank_sfs` to `new volume` and this row disappears |
| Alto BFS/TFS, Pilot/Cedar, Dwarf | no builder, no committed fixture | NAS `Software/PARC-Stuff.zip` |
| QDOS QXL.WIN | only a microdrive anchor is committed | — |
| DMG, NDIF/self-mounting, sparseimage, DART | read-only container formats | NAS `Software` (114 `.dmg`), `Old Mac Stuff` |
| IMZ (encrypted), ZIP-disk, WinImage, cb-dos/cbk | read-only container formats | — |
| Amiga ADF / HDF / ADZ / HDZ | real-world Amiga media | **ConsoleGames: 155 `.adf`, 11 `.hdf`** |
| Optical: UDF, High Sierra, VMS ODS-2, GameCube/Wii, CD-i, 3DO | no builder | ConsoleGames `.cdi` / `.iso`, but **cut to size first** |
| Floppy preservation: `.scp`, `.imd`, `.hfe` | flux/sector formats | ConsoleGames: 12 `.scp`, 15 `.imd` |
| A handful of real-world "messy" images | fragmentation, vendor quirks, known-bad | any source |

Scanning found **28,196 candidates / ~6.7 TB** across the three reachable
sources, so material is never the constraint — selection is. ConsoleGames
alone holds 2,525 GiB of `.chd` and 1,932 GiB of `.iso` that have no fixture
value whatsoever. Take the `.adf`/`.hdf`/`.scp`/`.imd` gap-fillers, minimise
the optical picks aggressively, and leave the rest where it is.

Perhaps **25–40 fixtures**. At the sizes the committed corpus already
demonstrates, that is tens of megabytes, not gigabytes.

### Budget

**Hard ceiling on the core corpus: 250 MB.** If a fixture would push past it,
minimise something first. Optical fixtures are the main risk — a real ISO can
be 700 MB on its own, so cut them to the smallest disc that still carries the
structure under test rather than storing a whole game or install disc.

Track the total in the catalogue; the run report prints it.

### The large-fixture annex

Some formats have no small specimen. A UDF DVD-Video is 483 MB and cannot be
truncated — UDF puts an anchor near the *end* of the disc, so cutting it
destroys the structure under test. Refusing the fixture would mean refusing
UDF coverage entirely.

So: fixtures that cannot be brought under roughly 50 MB live in

```
\\NAS\share\rb-fixtures\fixtures-large\
```

and are catalogued with a `../fixtures-large/` relpath.

**The annex is part of the standard corpus, not an opt-in extra.** Coverage
is never disabled just because a fixture is big — 250 MB is a target to aim
at, not a rule that silently drops formats. Sync the annex like everything
else.

The escape hatch exists for genuinely constrained hosts only: because an
unresolvable fixture already degrades to `skip-fixture` rather than a
failure, a machine that cannot hold the annex still completes a run, and the
report says exactly which cases were skipped and why. That is a
last-resort behaviour, not the default posture.

Some formats simply have no small specimen and never will:

| Fixture | Size | Why it cannot shrink |
|---------|-----:|----------------------|
| `part.sun.solaris-disk.multipart` | 1.57 GB | An installed Solaris system; the Sun label + UFS slices only exist on a real installed disk. |
| `optical.ufs.nextstep33.cd` | 356 MB | NeXTSTEP install CD — the only common UFS-formatted optical media. |
| `optical.high-sierra.bookshelf.cd` | 318 MB | Microsoft Bookshelf. High Sierra discs are 1986-89 only; there is no small one to find. |
| `optical.udf.dvd-video.cd` | 483 MB | UDF anchors sit near the end of the disc, so truncation destroys the structure under test. |

Anything added to the annex must carry a note like the above explaining why
it could not be shrunk. The annex is a minimisation backlog, not a dumping
ground — but a big fixture always beats no coverage.

### Rules for admitting a new fixture

A fixture earns its place only if it answers yes to all four:

1. Can neither be synthesized nor served from `tests/fixtures/`.
2. Is the smallest image that still proves the case.
3. Proves something no existing fixture already proves.
4. Its provenance and redistribution status are recorded.

And the standing preference: **shrinking the corpus beats growing it.** Every
new `rb-cli new` builder that lands should retire fixtures — when a builder
appears for a filesystem, move its cases from tier 2 to tier 1 and delete the
fixture rows. Note the reverse pressure too: R-009 (bare JFS/UFS/ReiserFS
images fail detection) means those four currently *need* their fixtures
exercised through a path that works, so fixing detection also improves
coverage.

### Storage form

Store zstd-compressed wherever the container does not already compress. The
runner decompresses into a per-run fixture cache in process (no external
`zstd` binary needed) and reuses the expansion across every case that names
the same fixture.

### Populated-system fixtures — a distinct class

The canonical five-file payload below is deliberately minimal, which makes it
good for proving a driver reads *something* and useless for proving the edit
path is safe. A freshly formatted volume with five files has no
fragmentation, no deep hierarchy, no near-full allocation bitmap, no
long-name edge cases, and nothing valuable to corrupt.

So tier 3 (mutation) needs a second class: **real installed systems with a
lot of real files on them.** These are for `put` / `rm` / `mkdir` / `chmod` /
resize against a volume that looks like something a user actually owns, with
the assertion that *nothing else changed*.

What makes a good one:

- A real OS install, not a synthesized volume.
- Hundreds to thousands of files, several directories deep.
- Filesystem-native metadata in real use — Mac type/creator codes and
  resource forks, Amiga protection bits and comments, Unix permissions.
- Meaningfully full, so allocation has to work rather than always
  appending into free space.
- Ideally something whose breakage is obvious — a blessed System Folder, a
  bootable partition.

Admitted so far:

| Fixture | Size | Why it is a good edit target |
|---------|-----:|------------------------------|
| `fs.hfs.populated-macii.hd` | 20 MB | Real Mac II disk: APM + HFS, blessed `System 7.1.2` folder, type/creator codes on every file (`APPL`/`SITD`/`BTFL`), Desktop DB/DF, nested folders, a 1.5 MB StuffIt archive. Small enough for the core corpus. |
| `fs.hfv.populated-macos81.hd` | 300 MB | Full Mac OS 8.1 install as a flat HFV. Annex. |

Available on the NAS but **not admitted** — too large to copy, fetch on
demand if a case needs them: Win98 original disk (3.1 GB CHD), IRIX 5.3
(727 MB CHD) and IRIX 6.5 (5.4 GB), Ken's Dell Optiplex (2.3 GB),
Amiga 128 GB (21 GB), plus several G3/G4/G5 Mac clones under
`VintageSystemBackups/`.

**Still wanted in this class**, one per major editable filesystem: a
populated **FAT32** (Win98-era), **ext**, **NTFS**, **AFFS** and **EFS**
volume, each small enough to admit. The Win98 and IRIX images above are the
right *content* at the wrong *size* — shrinking one of each is a better use
of effort than sourcing new material.

The safety rule still applies: mutation cases always run on a scratch copy
(`{fixture_copy}`), never on the corpus file.

### Canonical payload

Reuse the payload the existing `tests/fixtures/` generators already use, so
assertions stay uniform across filesystems:

- `/hello.txt` — short ASCII
- `/subdir/nested.txt` — proves directory descent
- `/link.txt -> hello.txt` — symlink, where the filesystem has them
- `/tiny.txt` — 10 bytes, exercises tail-packing / inline-data paths
- `/large.bin` — 24 KiB deterministic, forces indirect / extent block walks

Filesystems with native metadata add the relevant extras: Mac type/creator
and a resource fork, Amiga protection bits and a file comment, Unix owner and
permission bits.

## Catalog format

`fixture-map.tsv`, tab-separated, one row per fixture, header row present:

| Column | Meaning |
|--------|---------|
| `id` | Logical ID. Primary key. |
| `relpath` | Path relative to the corpus root. |
| `bytes` | Size on disk of the stored (possibly compressed) file. |
| `sha256` | Of the stored file, for integrity checking. |
| `inner_sha256` | Of the decompressed image, where stored compressed. |
| `origin` | Where it came from — the scan source, a generator script, or a URL. |
| `redistributable` | `yes` / `no` / `unknown`. Governs whether it may leave the NAS. |
| `minimised` | `no`, or a note describing what was cut. |
| `notes` | Anything a future reader needs: quirks, why this image, known bad. |

`fixture-map.json` carries the same rows for the runner to consume; the TSV
is the human-editable form and the JSON is generated from it.

## Sources being harvested

| Source | Reachable | Notes |
|--------|:---------:|-------|
| `\\NAS\share` | yes | Richest source. `VintageSystemBackups`, `Old DOS and Windows Stuff`, `Old Mac Stuff`, `SGI-IRIX`, `BlueSCSI Images`, `Operating Systems`, `Recovery Disks`, `winworld-pc`, `SampleFixtures`. |
| `\\NAS\games` | yes | `Amiga`, `Emulators`, `ROMs`, `HardwareGames`, `Backups`. |
| `C:\Temp` | yes | Working scratch with a lot of real material already (`.hdf`, `.iso`, `.cue/.bin`, ClassicMacHDDs, BlueSCSI-v2). |
| MiSTer `/media/fat/games` | host up | `mister.local`. SSH refuses public-key auth; needs a key installed or the documented password channel. Blocked on that. |

## Missing fixtures

A case whose fixture ID is absent from the catalog resolves to
`skip-fixture` and is written to `missing-fixtures.md` in the run bundle with
the case ID, the fixture ID, and what the case would have proven. That file
is the shopping list.

This is expected to be long at first and to shrink over time. It is never a
run failure.

## Relationship to `docs/need_fixtures.md`

`docs/need_fixtures.md` tracks fixtures needed to unblock *engine* work and
is scoped to `tests/fixtures/`. This document covers the regression corpus.
When a fixture would serve both, generate it small enough to commit and put
it in `tests/fixtures/`; the regression suite can reference committed
fixtures too, by ID, with the catalog pointing into the repo.
