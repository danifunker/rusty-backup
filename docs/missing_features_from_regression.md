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
| [F-003](#f-003) | PFS3 / SFS builders exist but are not on the CLI | `src/cli/verbs/new.rs` | two Amiga fixture gaps |
| [F-005](#f-005) | Optical extract is CLI-only; the GUI cannot pull one file | `src/optical/browse_view.rs` | GUI parity with `optical extract` |
| [F-006](#f-006) | IRIX support-disk building / browsing is thin | `src/cli/verbs/new_sgi_cdrom.rs` | bootable IRIX disc work — **needs scope** |
| [F-007](#f-007) | No optical fixture with nested directories | `regression-tests/` | verifying `--path DIR --recursive` |
| ~~F-008~~ | ~~`backup` reads only flat-layout sources~~ — **SHIPPED** 2026-08-15 | `src/cli/verbs/backup.rs` | — |
| [F-009](#f-009) | SFS editor writes single-leaf extent b-trees only | `src/fs/sfs.rs` | editing any real-sized SFS volume |
| ~~F-004~~ | ~~`show partmap` is APM-only~~ — **SHIPPED** 2026-08-08, same gap as R-026 | `src/cli/verbs/show.rs` | — |

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
