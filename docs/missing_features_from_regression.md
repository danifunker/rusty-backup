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
| [F-002](#f-002) | CloneCD (`.ccd` / `.img` / `.sub`) not supported | `src/rbformats/` | a whole rip format users have |
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

## F-002 — CloneCD sets are not readable {#f-002}

A CloneCD rip is a set of sibling files:

| File | Contents |
|------|----------|
| `.ccd` | the disc's table of contents / control file |
| `.img` | raw 2352-byte sectors |
| `.sub` | subchannel data |

We read none of them. `optical info` on the `.img` reports
`Container: unknown`, which is correct — there is no support to speak of.

**Why it matters.** CloneCD was a mainstream ripping tool, so these sets are
common in the wild, and the `.sub` subchannel data is exactly what makes them
worth having: it preserves protection and pregap information that a plain
`.iso` discards. For a project whose stated purpose is preserving vintage
media, "we cannot read the format that preserves the most" is a real gap.

The `.ccd` control file is plain text and the `.img` is raw MODE1/2352, so
basic read support is not a large piece of work. Subchannel interpretation is
the harder half and could come later.

**Fixture status — needs re-sourcing.** A `BOOKSHELF.ccd/.cue/.img/.sub` set
(383 MB + 15 MB subchannel) was dropped into the fixture inbox on
2026-08-07 and **deleted during triage** on the reasoning that CloneCD is not
a supported format — which was precisely backwards, since an unsupported
format is the one worth keeping a specimen of. It is not recoverable from the
share. The same disc survives in ISO form as
`optical.high-sierra.bookshelf.cd`, but the ISO is not a substitute: it has
no subchannel data and no `.ccd`, so it cannot exercise this feature at all.

Re-source the CloneCD set before starting F-002.

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
