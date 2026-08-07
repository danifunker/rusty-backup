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
| [F-001](#f-001) | `optical extract` cannot extract a single path | `src/cli/verbs/optical.rs` | fixture harvesting from ISOs |
| ~~F-002~~ | ~~CloneCD not supported~~ — **retracted, it is supported** | — | — |
| [F-003](#f-003) | PFS3 / SFS builders exist but are not on the CLI | `src/cli/verbs/new.rs` | two Amiga fixture gaps |
| [F-004](#f-004) | `show partmap` is APM-only | `src/cli/verbs/show.rs` | scripted partition inspection |

---

## F-001 — `optical extract` is whole-disc only {#f-001}

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
