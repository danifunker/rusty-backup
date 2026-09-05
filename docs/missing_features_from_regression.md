# Missing Features Found During Regression Work

Capability gaps turned up while building the regression suite
(`regression-tests/`). These are **not defects** — nothing here is behaving
incorrectly. Each is something the engine does not do yet, noticed because
the suite tried to do it.

Bugs live in [`Regression_Bugs.md`](Regression_Bugs.md). The distinction:
a bug means the code disagrees with its own documentation or with reality;
a missing feature means the code never claimed to do it and now there is a
concrete reason to.

| # | Feature | Area | Blocks |
|---|---------|------|--------|
| ~~F-001~~ | ~~`optical extract` cannot extract a single path~~ — **SHIPPED** 2026-08-09 | `src/cli/verbs/optical.rs` | — |
| ~~F-002~~ | ~~CloneCD not supported~~ — **retracted, it is supported** | — | — |
| ~~F-003~~ | ~~PFS3 / SFS builders exist but are not on the CLI~~ — **SHIPPED** 2026-08-15 | `src/cli/verbs/new.rs` | — |
| ~~F-005~~ | ~~Optical extract is CLI-only; the GUI cannot pull one file~~ — **ALREADY SHIPPED**, entry was stale | `src/optical/browse_view.rs` | — |
| [F-006](#f-006) | IRIX support-disk building / browsing is thin | `src/cli/verbs/new_sgi_cdrom.rs` | bootable IRIX discs — **scoped**; step 1 (header validated by IRIX) done, steps 2-3 open |
| ~~F-007~~ | ~~No optical fixture with nested directories~~ — **SHIPPED** 2026-08-17 | `regression-tests/` | — |
| ~~F-008~~ | ~~`backup` reads only flat-layout sources~~ — **SHIPPED** 2026-08-15 | `src/cli/verbs/backup.rs` | — |
| ~~F-009~~ | ~~SFS editor writes single-leaf extent b-trees only~~ — **SHIPPED** 2026-08-17 | `src/fs/sfs.rs` | — |
| ~~F-004~~ | ~~`show partmap` is APM-only~~ — **SHIPPED** 2026-08-08, same gap as R-026 | `src/cli/verbs/show.rs` | — |
| [F-010](#f-010) | A file-aware Ghost image exposes only its FAT record stream; an NTFS partition behind it is unreachable | `src/rbformats/gho.rs` | D13's Windows check (a real multi-partition `.GHO` with long names) |
| ~~[F-011](#f-011)~~ | **SHIPPED 2026-09-05.** ~~The classic HFS writer allocates a fork as one contiguous run; a fragmented volume reports disk full with room to spare~~ | `src/fs/hfs.rs` | H3's classic-HFS check: a resource fork spilled into the extents-overflow file by rb-cli, clean under `fsck_hfs -n` and Disk First Aid before and after the delete |

---

## F-001 — `optical extract` is whole-disc only {#f-001}

**SHIPPED 2026-08-09.** `--path` takes a file or a folder, `--recursive`
decides whether a named folder descends, `--tar` archives instead of writing
loose files, and `--preserve-permissions` applies the disc's POSIX mode.
Nine cases in `cases/tier3/optical-extract.toml`, the single-file one anchored
on a sha256 so a truncated read fails on content rather than on exit code.

The Windows-illegal-names observation below is largely addressed too: a
per-path extract sidesteps it for the common case, and `--tar` sidesteps it
entirely, since a tar entry stores a name NTFS would refuse.

```
Usage: rb-cli optical extract [OPTIONS] --to <TO> <SOURCE>
```

There is no way to pull one file out of a disc image. `--to` takes a
destination folder and everything goes into it.

`get` does not fill the gap either — it rejects optical images and redirects
to `optical browse` / `optical extract`:

```
rb-cli get disc.iso "/some/file.img" ./out
  -> rb-cli optical browse disc.iso        # list the file tree
     rb-cli optical extract disc.iso --to DIR   # pull files out
```

**Why it came up.** Harvesting one 64 KiB NDIF image out of
`Apple-Legacy-Feb_2000.iso` meant extracting all 649 MB of it — 2,167
entries — to reach a single file. Fixture harvesting from optical media is a
recurring task for this project, and it is currently all-or-nothing.

**What would help:** a `--path <IN_IMAGE_PATH>` filter, or a glob, matching
the ergonomics `get` already has for filesystem images.

Related observation, worth handling at the same time: that extraction
reported **59 errors** on Windows for entries whose names Windows rejects
(`/OpenFolderListDF` and similar). Not wrong — those names are legal on HFS
and not on NTFS — but it means a full-disc extract cannot be assumed complete
on Windows, and a per-path extract would sidestep the issue entirely for the
common case.

## F-002 — RETRACTED: CloneCD is supported {#f-002}

**This entry was wrong and is withdrawn.** CloneCD reads correctly today:

```
rb-cli optical info BOOKSHELF.ccd
  Container:   clonecd
  Filesystems: high_sierra
  ISO 9660:
    Volume id:   BOOKSHELF
    Space size:  162838 blocks x 2048 B

rb-cli optical du BOOKSHELF.ccd
  148.7 MiB   1056 files   17 dirs   /
```

**How I got it wrong.** I ran `optical info` on the `.img` and saw
`Container: unknown`. That is correct behaviour — a CloneCD `.img` is raw
2352-byte sectors with no header, so there is nothing to identify it by. The
`.ccd` control file is the entry point, exactly as `.cue` is for a bin/cue
pair. Testing the payload instead of the descriptor produced a confident
wrong conclusion, and I deleted the fixture on the strength of it.

Two things worth keeping from the episode:

**A real, smaller limitation does exist.** The container reports:

> `warning: raw-sector metadata (preparer id, El Torito) unavailable for the
> clonecd container`

So CloneCD is read at the filesystem level but the raw-sector layer is not
plumbed through, which is where El Torito boot records and the preparer ID
live. That is a genuine partial — much narrower than "unsupported" — and the
`.sub` subchannel data is likewise carried but not interpreted. Neither
blocks reading a disc.

**The fixture is now held**, in the annex as
`optical.clonecd.bookshelf.cd/` (`.ccd` + `.img` + `.sub` + `.cue`, 399 MB).
It is a good one: CloneCD container, High Sierra filesystem, real subchannel
data, and its `.cue` is the R-015 repro. Verified internally consistent —
`.img` at 2352 B/sector, `.sub` at 96 B/sector and the sibling `.ISO` at
2048 B/sector all describe exactly **162,840 sectors**, and that `.ISO` is
byte-identical to the one already held as
`optical.high-sierra.bookshelf.cd`.

## F-003 — PFS3 and SFS can be created, but only from inside the engine {#f-003}

**SHIPPED 2026-08-15.** `new volume pfs3` and `new volume sfs` exist, and both
round-trip: format, `put`, `ls`, `get`, and for SFS `fsck` too.

**Exposing the builder immediately found a second gap, which is the point this
entry made.** A bare PFS3 image was not detected at all — it fell through to
the MBR parse and reported `invalid boot signature`, the exact shape of R-017
(the same omission for SFS). Nothing had ever opened one, because nothing
could make one outside a unit test.

Fixing it needed the distinction between two four-byte tags that are easy to
conflate. The **boot block** magic at offset 0 is `PFS` / `PDS` /
`muAF` / …, where the final byte is a *format version*, not a character. The
**DosType** the filesystem dispatcher matches on is `PFS`, and a bare volume
has no RDB to carry it. So `looks_like_pfs3` validates the boot magic *and* the
root block at sector 2, then `detect_superfloppy` reports the DosType — and the
hint has to be added to the routing allow-list in `PartitionTable::detect`,
which is what actually made it dispatch.

Guarded by `detect_superfloppy_bare_pfs3_routes_by_dostype` and a negative
`..._rejects_bare_magic` twin, matching the SFS pair beside them.

The caveat this entry always carried still stands: a volume rusty-backup builds
is not a reference fixture for rusty-backup. What this buys is that the write
paths are now reachable by an external oracle — and since 2026-08-14 that
oracle exists (`oracles/fsuae/affs_mount.py`), so pointing it at a PFS3 or SFS
volume is a real next step rather than a hypothetical one. It needs the PFS3 /
SFS handlers staged into the guest's `L:`; Kickstart has neither in ROM.

---

### The original report

`create_blank_pfs3` and `create_blank_sfs` already exist in `src/fs/` and are
used by unit tests to build volumes. Neither is reachable from `rb-cli`:
`new volume` offers hfs, hfsplus, hfv, fat, ntfs, hpfs, ext, ext3, ext4,
affs, prodos, efs, minix2, minix3 — no pfs3, no sfs.

**Why it came up.** Both formats sit in the regression matrix with
`we_write = true` and no reference fixture. Exposing the existing builders
would close two fixture gaps for what looks like a small change, and would
make the write paths reachable for testing at all.

Caveat worth stating: a volume rusty-backup builds is **not** a valid
reference fixture for rusty-backup — see `regression-tests/README.md`
§ "Why tier 1 is not coverage". Exposing these builders makes the write path
*testable against an external oracle* (a booted AmigaOS with the PFS3/SFS
handlers), which is the actual win. It does not by itself prove anything.

## F-004 — `show partmap` only understands APM {#f-004}

**SHIPPED 2026-08-08.** It detects the partition table first; APM keeps its
full DDR and driver-descriptor rendering, every other table gets the generic
partition list. Tracked twice, here and as
[R-026](Regression_Bugs.md#r-026) — the defect / missing-feature split is not
always obvious from a symptom, and this was genuinely both: the verb never
claimed to read other schemes, and its error blamed the image for it.

`docs/cli-reference.md` is honest about this — "Print the partition table of a
disk image (APM-only today)" — so it is a scoped feature rather than a defect.
On anything else it fails:

```
rb-cli show partmap x68k.img
  -> error: parsing APM: Invalid APM: bad DDR signature: 0x6002

rb-cli show partmap superfloppy.img
  -> error: parsing APM: Invalid APM: bad DDR signature: 0xEB3C
```

**Why it came up.** The regression matrix wants a per-scheme structured dump
for all nine `PartitionTable` variants; `inspect` covers the display side but
`show partmap` is the machine-readable one, and it reaches only one scheme.

Two things would help, in order of cost:

1. **The error message.** It names APM as though the *image* were a malformed
   APM disk, rather than saying the verb does not support this scheme. A
   one-line fix, independent of any feature work.
2. **The feature.** Extend to MBR, GPT, RDB, SGI, Sun, AHDI and X68K, which
   are all already parsed by the engine — `inspect` prints them today, so the
   data is there and only the structured emitter is missing.

---

## F-005 — the GUI cannot extract a single file from a disc {#f-005}

**ALREADY SHIPPED — this entry was stale, and closing it needed no code.**
Checked 2026-08-17 against the four things it asked for; all four were already
there, landed in July and never reflected here.

| what the entry asked for | where it is |
|---|---|
| an extract action on a selected entry **and on a folder** | checkbox column + `render_selection_bar` (called at `browse_view.rs:392`); `marked_export_entries` drops anything under an already-marked directory so a folder's recursive walk is not archived twice |
| a destination chooser, folder vs archive | `rfd::FileDialog` — `save_file` for single-file formats, `pick_folder` otherwise, branched on `ExportFormat::is_single_file` |
| the `--filesystem` selector, "without which the GUI can only ever reach one side of a hybrid Mac/PC disc" | `selected_fs` + its ComboBox; `b8cfe77` is literally "filesystem selector for hybrid Mac/PC discs (browse/extract + GUI)" |
| a README Inspect-tab bullet | present, and it names the Optical disc browser as one of four surfaces on the shared `export_selection` engine |

The commits, none of which updated this file:

```
61413fe  2026-07-14  export-selection engine for all output formats
b8cfe77  2026-07-16  filesystem selector for hybrid Mac/PC discs (browse/extract + GUI)
400f54f  2026-07-21  multi-select export to .mar in the CD browser
aff45d0  2026-07-21  unify multi-select export as a format pulldown + all archive types
```

It also overshot the ask. The entry wanted the CLI's `--tar`; the GUI offers ten
formats — loose files, gzip/zstd per file, BinHex, tar, tar.gz, tar.zst, Zip,
StuffIt and Mac Archive.

**One real difference from the wording, left alone deliberately.** The entry
says "an extract action on the *selected* browse entry", and selection here
means ticking a checkbox rather than single-clicking a row. The capability is
complete either way, and adding a second redundant path to the same engine is
not obviously an improvement. Worth doing only if the checkbox proves awkward
in use.

**Same shape as R-038**: a document confidently describing a state that had
changed underneath it. Two of the four remaining features in this file turned
out to be stale on inspection this week, which is an argument for checking the
code before believing the list.

---

### The original report

`optical extract` grew `--path`, `--recursive`, `--tar`,
`--preserve-permissions`, `--filesystem` and `--filesystem-index` (F-001).
None of it is reachable from the GUI, which can browse an optical disc but
offers no way to pull anything out of it.

**Why it is small.** The capability is already there:
`src/optical/browse_view.rs` calls `fs.read_file(entry)` in three places, so
the GUI can already read one file out of a disc — it just never offers to save
one. This is wiring, not new engine work.

**What it needs:**

- an extract action on the selected browse entry, and on a selected folder
- a destination chooser, with the folder-vs-archive choice `--tar` introduced
- the `--filesystem` / `--filesystem-index` selector surfaced, **without which
  the GUI can only ever reach one side of a hybrid Mac/PC disc** — the reason
  this is listed rather than left implicit
- CLAUDE.md's pre-commit doc sync applies: a new dialog wants a README
  Inspect-tab bullet

## F-006 — IRIX support-disk building and browsing is thin {#f-006}

**Scoped 2026-08-15, and step 1 is done.** The ask is a *bootable* IRIX
CD-ROM, 5.3 and 6.5, for building all-in-one installation discs.

### Step 1 — the existing header is now validated against IRIX (DONE)

`new hd sgi-efs` printed "real IRIX fx/prtvtoc validation is unverified without
hardware/emulator". It is verified. IRIX 6.5.22's own `fx` 6.5 opens the drive
and prints our partitions, bootinfo and geometry without complaint, and
`prtvtoc` agrees field for field:

```
  0: efs        5040 + 126000         2 + 62
  8: volhdr        0 + 5040           0 + 2
 10: volume        0 + 131040         0 + 64
 root partition = 0     swap partition = 0    bootfile = /unix
----- directory entries-----
```

So **partitioning is solved** — a user does not run `fx` by hand — and the
empty `directory entries` section is precisely the gap. That directory is what
ARCS reads to find `sash` / `ide`; we write 15 blank slots. Method and traps
are in `oracles/iris/README.md`.

Two limits found while proving it: nothing yet tests `fx` *writing* a label or
the kernel mounting from it, and our label declares 130 cylinders where the
drive reports 131 (the builder rounds up to whole cylinders and then the size
is clamped). `fx` did not object, but a bootable disk should match the drive
exactly, so the rounding wants revisiting first.

### Step 2 — populate the volume directory (NOT STARTED)

The boot files are SGI-copyrighted and cannot ship with rusty-backup, so this
is "assemble a bootable disc *from* the user's own distribution media", not
"synthesize one". That shapes the CLI: it needs `--sash` / `--miniroot` inputs
or a `--from-disc` that harvests them.

### Step 3 — the CD path (NOT STARTED)

`optical new sgi-efs` already writes the CD-shaped volume header — EFS in slot
7, the 1 head x 32 sectors/track geometry verified against real 5.3 and 6.5
discs. It needs the directory entries and the miniroot.

**Scope 6.5 first.** 6.5 boots under iris and is verifiable end to end; the 5.3
image hangs at "The system is coming up" with `Find Error: 10` and never
reaches a login, so 5.3 cannot be proven here yet. The verification story is
otherwise unusually good for this feature — "did it boot" has an unambiguous
answer, the same oracle shape that closed R-039 and R-020.

---

### The original report


`optical new sgi-efs` builds an SGI volume header with EFS in slot 7, and
takes `--from-dir`, `--expand-archives` and `--flatten-folders`. What it does
not do is treat the disc as a *bootable* IRIX support disc.

**Needs scope before any work starts.** "Extending the IRIX support disks"
could reasonably mean any of:

1. **Volume-header executables as first-class entries.** An SGI volume header
   carries standalone programs (`sash`, `ide`, `/unix`) outside the EFS
   filesystem. `inspect` reports the header; nothing lists, extracts or
   replaces those entries.
2. **Building a disc that actually boots.** Writing the right volume-header
   entries and boot fields for real hardware or a MiSTer core.
3. **Richer `--from-dir` ergonomics** for laying out an inst-ready tardist
   tree.

(1) is self-contained and testable from a fixture. (2) cannot be verified
without hardware — every SGI oracle is `skip-manual`, so it would ship
unproven. That used to be Amiga's position too; it no longer is, and the way
out is worth copying. [R-020](Regression_Bugs.md#r-020) was closed by writing
one emulator harness (`oracles/fsuae/affs_mount.py`, host directory as the
verdict channel), and `iris` does the same job for IRIX — so the SGI oracle is
a harness someone has not written yet rather than a thing that cannot be done.
(3) is ergonomics on an existing path.

## F-007 — no optical fixture has nested directories {#f-007}

**SHIPPED 2026-08-17.** The fixture was there all along — this was a case gap,
not a corpus gap. `optical.hfs.opentransport.cd` carries a real tree:
`/Install 1` holds files *and* a subdirectory (`Open Transport Files`), so the
flag finally has something to descend into.

Three cases in `cases/tier3/optical-extract.toml`:

| case | asserts |
|---|---|
| `path-dir-stops-at-one-level` | `--path "/Install 1"` alone -> 10 entries |
| `path-dir-recursive-descends` | the same path `--recursive` -> 30 entries |
| `nested-dir-addressable-directly` | `/Install 1/Open Transport Files` -> 20 entries |

The counts are exact rather than "recursive found more", which would still pass
if recursion degraded from a subtree to one extra file. Against a sha256-pinned
fixture an exact count says what each mode actually covers, and a change either
way earns a failure.

The third case is the one that makes the pair meaningful: it proves the nested
directory is independently addressable, so the recursive result is a real
subtree rather than a flattening artifact.

**One trap, recorded because it cost a run:** `extract` prints its count to
**stderr**, keeping stdout clean for data. A `stdout_contains` assertion
matches nothing and fails with no visible difference between expected and
actual — the counts were right the first time, the stream was not.

---

### The original report

`--path DIR --recursive` is implemented and unverified. The discs in the
corpus are flat or single-file at the root:
`optical.iso9660.joliet.cd` holds one file with no directories at all, which
is why it makes such a clean single-file test and such a poor recursion test.

**What would help:** a case against `optical.hfs.opentransport.cd` or the
CloneCD Bookshelf set, both of which have real trees, asserting that
`--recursive` descends and that its absence stops at one level. The fixtures
are already catalogued — only the case is missing. It belongs in
`cases/tier3/optical-extract.toml` beside the nine that exist.

## F-008 — `backup` reads only flat-layout sources {#f-008}

**SHIPPED 2026-08-15.** `backup` now decodes a container source before the
engine sees it, so all four formats in the table below work. The four cases
went XPASS on the first run after the change and their `known-failures.toml`
entries are gone — which leaves that file with **no entries at all**.

The fix is the one this entry predicted: route through the decoding that
already existed. It is done by mirroring the *remote* arm of
`run_backup_from`, which had already solved the identical problem — the byte
source is not a `File`, but two engine paths (single-file CHD output and the
defrag-clone) are typed on one. So a container is decoded once to a scratch
file in the destination directory, under a delete-guard, and the ordinary
local pipeline runs on that.

Two things that had to be right beyond "it exits 0":

* **Block devices must not be probed.** Container detection is gated on
  `path.is_file()` first. Probing `\.\PhysicalDrive0` would open it without
  the elevation prompt the real path performs, and report a length of zero.
* **The recorded size must be the virtual one.** `metadata.source_size_bytes`
  is what `restore` reads back to size its target, so writing a QCOW2's
  compressed file length there would mis-size every restore from that backup.
  Decoding to a temp makes `total_size()` the virtual size for free; all six
  sources now record 67108864 for the same 64 MB disk.

Verified beyond exit codes: the partition images produced from CHD, dynamic
VHD, QCOW2 and sparse VMDK are **byte-identical** (sha256) to those from the
raw baseline, and `--format chd` — the default, and the path that needs a
concrete `File` — works from a QCOW2 source.

**Not yet verified on a real-world container.** `fs.hpfs.os2-warp45.hd`, the
monolithicSparse VMDK named below, is not in the local corpus and needs a
sync; only synthetic containers have been exercised.

**A follow-up worth taking:** the decode is unconditional for containers, so
`--format raw|zstd|gzip|lz4|vhd` pays a full disk copy it does not need. Those
paths consume `Box<dyn ReadSeek>` already, so a `SourceFactory::Container`
variant could stream them, leaving the temp only for CHD output and
shrink-to-minimum. Filed as a note here rather than a new F-entry because the
capability is present either way.

---

### The original report

Filed as defect [R-016](Regression_Bugs.md#r-016) until 2026-08-09.
**Reclassified**: `backup` has never claimed to decode containers, so this is
a capability the engine lacks, not code disagreeing with itself. The four
cases keep their assertions — they describe the behaviour we want — and now
cite this entry.

`backup --help` documents SOURCE as "an image file or a block-device path",
and `inspect` reads every container we write. `backup` reads only the ones
whose data begins at offset 0:

| source | `inspect` | `backup` |
|--------|-----------|----------|
| raw `.img` | `Partition table: MBR` | **ok** |
| fixed VHD | `Partition table: MBR` | **ok** |
| **CHD** | `Partition table: MBR` | **fails** |
| **dynamic VHD** | `Partition table: MBR` | **fails** |
| **QCOW2** | `Partition table: MBR` | **fails** |
| **VMDK sparse** | `Partition table: MBR` | **fails** |

Fixed VHD only passes because it *is* raw data with a trailing footer. Every
container with a non-flat internal layout fails, in one of two ways:

```
rb-cli backup o-chd/disk.chd ./out --format raw --sector-by-sector
  -> error: backup failed: cannot read first sector: failed to fill whole buffer

rb-cli backup o-qcow2/disk.qcow2 ./out --format raw --sector-by-sector
  -> error: backup failed: failed to detect partition table:
     Invalid MBR: invalid boot signature: expected 0xAA55, got 0x...
```

Same root cause — the container is not decoded, so raw file bytes are read as
though they were the disk. Which message appears depends on whether the read
runs off the end of a small file or lands on header bytes that resemble a bad
MBR. `--sector-by-sector` does not help; the failure precedes all partition
logic.

**Why it matters.** These are four of our own output formats, CHD being the
default for `convert`. A user can convert a disk to any of them and then find
they cannot back it up — archive to QCOW2, later try to make a working copy,
and the tool refuses. `inspect` reading them fine makes the gap look arbitrary
from outside, which is what made it read as a defect for so long.

**What would help.** `inspect` already opens all four, so the decoding exists;
`backup` takes a different route to the bytes. Routing `backup`'s source open
through the same container-aware path `inspect` uses is the whole feature.
`backup.container.inspect-reads-what-backup-cannot` is green and pins that
asymmetry, so it is the case to read first.

Reproduces on a 64 MB synthetic image; no fixture required. **It also
reproduces on a real one** as of 2026-08-09: `fs.hpfs.os2-warp45.hd` is a
monolithicSparse VMDK holding an OS/2 Warp 4.52 install, and `backup` refuses
it with the same `invalid boot signature: expected 0xAA55, got 0x0000` while
`inspect` reads all 4722 files. Useful when implementing this — the gap is not
an artifact of how the synthetic containers were built.

**Two traps when verifying this**, both of which caught the original reporter:

1. `backup` prints `rb-cli backup: SRC -> DEST` *before* doing any work, so a
   grep for `->` reports success on a run that then fails. **Check the exit
   code**, not the output.
2. `--format raw` writes `partition-N.img` files, so a `find` over several
   directories can pick up an unrelated `.img` and attribute the wrong result
   to the wrong container. Use explicit paths per case.

## F-009 — the SFS editor writes single-leaf extent b-trees only {#f-009}

**SHIPPED 2026-08-17.** The extent B-tree now descends, splits a full node in
half, promotes the new sibling's first key, and grows a level when the split
reaches the root; emptied nodes are unlinked and their blocks freed, and a root
left with one child collapses into it. `edit.sfs.put-get` flipped from
asserting the refusal to asserting the round-trip, exactly as its own note
predicted.

Three things the estimate below did not anticipate, all found by reading the
reference volume before writing any code:

- **Half the job did not exist.** Separators are only `<=` their subtree's
  minimum, not equal to it — five interior entries in the reference volume sit
  below their child's first key, left behind by deletes. So there are no parent
  updates to maintain on removal, and "node splitting, root promotion and
  parent updates" reduced to the first two.
- **A prerequisite was broken.** `alloc_admin_block` could not have worked on
  any real volume: the AdminSpaceContainer layout was four bytes off in reader,
  writer and allocator alike. Split needs blocks, so this had to land first —
  [R-042](Regression_Bugs.md#r-042).
- **A second ceiling sat behind the first.** Object nodes live in a different
  structure — a fixed-fan-out index, not a B-tree — and its allocator assumed a
  single leaf too. It refused every write on a real volume the moment the
  extent tree stopped refusing first. Now walked; *growing* it remains
  unimplemented, which is what limits a real volume to ~926 new files rather
  than unlimited. That is the honest remainder of this entry and is tracked in
  CLAUDE.md rather than reopened here.

**The emulator proof is still outstanding.** `oracles/fsuae/sfs_mount.py` is
written and separates its outcomes correctly, but no SFS volume mounts under it
yet: the guest reports `not enough memory available` when the handler starts.

An earlier version of this paragraph blamed the FS-UAE environment, on the
evidence that the AFFS oracle had also stopped answering. That was a bad
control — it was handed the bootable Workbench fixture as its artifact, so the
guest had two bootable volumes and never ran the probe. Against an ordinary
AFFS volume it mounts exactly as it did when it closed R-020. The environment
is fine; the SFS mount is its own problem, and the remaining lead is WinUAE,
whose `hardfile2` filesystem field FS-UAE strips. See that script's status note
for the four routes already eliminated.

---

### Original report

Filed as defect [R-032](Regression_Bugs.md#r-032) until 2026-08-10.
**Reclassified**: the driver has always documented this ceiling — CLAUDE.md
says "SFS (`src/fs/sfs.rs`) — read + edit (single-leaf btree only)" — so hitting
it is the documented limit, not code disagreeing with itself. That is the same
line R-016 was moved across.

SFS keeps its free-space extents in a b-tree. `extent_btree_insert` and
`extent_btree_remove` handle a tree that is a single leaf node; a volume large
enough to need interior nodes — which is any volume of real size, including the
Workbench reference disk — cannot be written.

```
rb-cli put fs.sfs.workbench-dh0.hd.img payload.bin /payload.bin
  -> error: create_file: unsupported: SFS extent b-tree has interior nodes;
     this editor writes single-leaf trees only ...
  -> exit 4
```

**Two things were fixed while reclassifying it**, because how it refused was
wrong even if the refusal was right:

- It raised a **`Parse`** error, which says the *disk* is malformed. The disk is
  fine and reads perfectly. It is now `Unsupported` — the volume is readable and
  this build will not write it — which is precisely what
  `exit.rs` reserves `PERMISSION_DENIED` for.
- `put` mapped every `create_file` failure to the catch-all 1. It now routes
  through `write_open_error`, the R-034 machinery, so the refusal exits **4**.
  The write-*open* path had done this since R-034; `create_file` can refuse for
  the same reason and did not.

**What implementing it needs.** Node splitting, root promotion and parent
updates against the on-disk `BNDC` format.

The validation half is no longer the hard part. This entry used to say writes
to a real Amiga filesystem could not be confirmed from here; that stopped being
true on 2026-08-14, when `oracles/fsuae/affs_mount.py` drove FS-UAE to a
verdict and closed [R-020](Regression_Bugs.md#r-020). The same harness serves
SFS — mount the volume under test as DH1: and have the guest read it — with
one addition: Kickstart has no SFS handler in ROM, so the guest needs the SFS
handler staged into `L:` (it is at `rb-fixtures/oracle-assets/amiga`,
extracted from the SFS reference fixture's own `L:`). So the code is the
work now, not the proof.

## F-010 — a file-aware Ghost image exposes only its FAT record stream {#f-010}

Found 2026-09-02 while looking for a Ghost image to verify D13 (eaa6f3d,
unique 8.3 aliases for prefix-sharing long names) against Windows.
`C:\Temp\JoeBackup\JoeBa.GHO` is a complete 12-span, 24 GB file-aware
backup of a Dell machine. `rb-cli inspect` reports one partition: the
39 MB Dell utility FAT volume, 70 files, all 8.3 names.

`GhoReader::open` dispatches a file-aware image on whether a FAT record
stream is present: with one, the whole image becomes that single FAT
partition; without one, it tries the NTFS file-aware path (GHPR metadata
plus packed cluster runs). An image holding both, which is what every Dell
or OEM machine of the era produces, never reaches its NTFS partition, so
the Windows volume with the long names cannot be reconstructed, browsed or
restored from it.

What it would take: walk the record stream per partition instead of once,
hand each partition to the FAT or NTFS reconstruction according to its own
metadata, and emit a multi-partition disk image with the table the header
describes. The single-partition paths already exist; the missing piece is
the loop and the disk-level layout. Until then D13 stays covered by its
unit test only.

## F-011 — the classic HFS writer allocates a fork as one contiguous run {#f-011}

**SHIPPED 2026-09-05.** `allocate_extents` in `src/fs/hfs.rs` still asks for
one run first (a contiguous fork when the volume has room for it, the way
Mac OS allocates) and otherwise gathers the free runs first-fit in address
order; the first three go into the catalog record's inline extents and the
rest into the extents-overflow B-tree, three per record, through the same
`btree_insert_full` the HFS+ driver uses (`BTreeKeyFormat::CLASSIC_EXTENTS`).
`create_file` and `write_resource_fork` (so `put`, `import`, `setrsrc`,
Commander) all take that path; the read and delete sides already handled
overflow records. The H3 classic-HFS check now builds: a 1 MB resource fork
over 126 one-block holes spills five overflow records, `fsck_hfs -n` and Disk
First Aid 7.2 (System 7.1 in Snow) both pass before and after `rb-cli rm`
(`docs/Regression_Bugs.md`, macOS verification table; unit test
`create_file_spreads_a_fork_over_free_runs`).

Found 2026-09-05 by the audit's H3 check on macOS. The check fills an
8 MiB classic HFS volume with 64 KiB files, deletes every other one and
adds a file whose 1 MB resource fork must spread over the holes, so that
its extents spill into the extents-overflow file and the delete that
follows can be judged by `fsck_hfs`. `rb-cli put` refused:

```
create_file: disk full: cannot find 196 contiguous free blocks
```

with 4 MiB free in 63 holes of 128 blocks. `allocate_blocks` in
`src/fs/hfs.rs` asks the volume bitmap for one run of the fork's whole
length; the three inline extents and the extents-overflow B-tree that
the *reader* and the *delete* path handle are never produced by the
writer. HFS+ (`src/fs/hfsplus.rs`) allocates extent by extent and does
spill, which is how H3 was verified there.

What it would take: allocate a fork as a list of runs (first fit over
`bitmap_collect_clear_runs_be`), fill the three inline extents, and
insert the rest as overflow records through the same
`btree_insert_full` path the HFS+ driver uses with
`BTreeKeyFormat::CLASSIC_EXTENTS`. Until then a fragmented classic HFS
volume fills up early, and H3 on classic HFS stays covered by the unit
test `delete_frees_overflow_extents_and_their_records`, whose overflow
record is written by hand.
