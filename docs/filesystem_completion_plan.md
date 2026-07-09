# Filesystem Completion Plan

**Goal:** for every filesystem where it is meaningful, reach the full quartet —
**Browse · Create-blank · Edit · Check/Repair (fsck)**. This document turns the
[coverage audit](filesystem_coverage_audit.md) into a work-list, ordered so the
highest-value capability lands first.

Backup/restore already works on *every* filesystem at the raw-sector level. This
plan is only about the filesystem-*aware* quartet layered on top.

## What "where possible" excludes

Four filesystems already have the complete quartet: **HFS, HFS+, SGI EFS,
AmigaDOS/AFFS**. They are the template. But not everything can or should get all
four:

- **Read-only by nature** — Apple Lisa FS (files reconstructed from sector tags;
  no writable catalog), Carve (raw recovery view), Pilot/Cedar *client* files
  (no on-disk names). Browse is the ceiling.
- **Fixed-geometry floppies** — CBM DOS, Atari DOS, Acorn DFS, RS-DOS,
  DragonDOS, OS-9 floppies. "Shrink/grow" is meaningless at a fixed 140K–800K
  geometry, so the quartet for them is **Create + Edit + fsck** (shrink N/A).
- **Deprecated / risky upstream** — ReiserFS write (unmaintained, being removed
  from Linux), Reiser4. Read is enough.
- **Effort-vs-value underwater** — btrfs / APFS / ZFS write. Their on-disk
  structures are large, checksummed, and CoW; a correct writer is months of work
  and the archival demand is read-first. Targeted as **read-only** for now.

Everything else is in scope. Effort key: **S** ≈ days · **M** ≈ 1–3 weeks ·
**L** ≈ 3–6 weeks.

---

## Part 1 — Complete the filesystems we already support

The engine (read + edit) already exists for these; we are filling the missing
cells. This is the cheapest, highest-value work — no new parser, just the
missing capability on a driver that already round-trips.

### 1a. Current quartet gaps (supported, editable filesystems)

| Filesystem | Create | Edit | Check/Repair | Missing → target |
|---|:---:|:---:|:---:|---|
| **HFS / HFS+ / EFS / AFFS** | Yes | Yes | Yes | — **complete** (the template) |
| FAT12/16/32 | Yes | Yes | **Yes** | — **fsck done** (FAT chains: loops, bad/lost/cross-linked clusters, size-vs-chain; FAT-mirror consistency; repair is FAT-only, byte-verified vs `fsck_msdos`) |
| exFAT | Yes | Yes | **Yes** | — **fsck done** (allocation-bitmap reconciliation vs the directory tree; boot checksum + backup + VolumeDirty; repair rebuilds the bitmap + resyncs boot, byte-verified vs `fsck_exfat`) |
| NTFS | Yes | Yes | **Yes** | — **fsck done** (`$Bitmap` reconciliation vs the MFT walk, `$MFTMirr` + backup-boot sync, VolumeDirty; repair rewrites `$Bitmap`, `$MFTMirr`, backup boot, and clears the dirty flag; oracle-verified against Windows `chkdsk`) |
| ext2/3/4 | **Yes** | Yes | **Yes** | — **create done** (`rb-cli new --fs ext` formats a plain rev-1 ext2 — 128-byte inodes, no extents/csum — and `--fs ext3` adds an empty jbd2 journal (inode 8, 4 KiB blocks); reuses the `ext_fsck` bitmap builders; byte-verified `e2fsck`-clean across 1 MiB–160 MiB, single- and multi-group. ext4 (extents + metadata_csum) intentionally out of scope). fsck done: block+inode bitmap + free-count reconciliation vs computed allocation, byte-verified vs `e2fsck`; repair withheld on metadata_csum. |
| UFS / FFS | No | Yes | **repair** | **create** |
| XFS (v4/v5) | No | Yes (v4) | **repair** | **create + v5 edit** (shrink = known limitation) |
| MFS | No | Yes | No | **create + fsck** |
| PFS3 | Yes | Yes | No | **fsck** |
| SFS | Yes | Yes (single-leaf) | No | **fsck + multi-leaf edit** |
| ProDOS | No | Yes | validate | **create + check/repair** |
| Apple DOS 3.3 | No | Yes | No | **create + fsck** |
| CBM DOS | Yes | Yes | **Yes** | — **fsck done** (VALIDATE; byte-verified vs `c1541 validate`) |
| Atari DOS 2 | test-only | Yes | No | **create + fsck** |
| RS-DOS | Yes | Yes | No | **fsck** |
| OS-9 / RBF | No | Yes | No | **create + fsck** |
| DragonDOS | Yes | Yes | No | **fsck** |
| Acorn DFS | Yes | Yes | No | **fsck** |
| Acorn ADFS | No | Partial | No | **finish edit + create + fsck** |
| Human68k | Yes | Yes | No | **fsck** |
| CP/M | No | Yes | No | **create + fsck** |
| QDOS (QXL.WIN) | No | Yes | No | **create + fsck** |
| Alto BFS / TFS | Yes | Yes | No | **fsck** |

### 1b. Add fsck (check + repair) — the biggest single lever

Only 7 of 26 editable filesystems have an interactive fsck today. The trait
already supports it (`Filesystem::fsck()` + `repair()`); the pattern is
established (see `hfs_fsck/`, `efs_fsck.rs`, `affs_fsck.rs`, `ufs_fsck` in
`ufs.rs`). Work items, by tier:

| Target | Filesystems | Effort | Notes |
|---|---|---|---|
| **Most-used first** | ~~FAT~~ (done), ~~exFAT~~ (done), ~~NTFS~~ (done), ~~ext~~ (done) | M each | Promote the existing `validate` gate to a real check + repair. All four shipped. **FAT** (`fat_fsck.rs`): FAT-table walk over the directory tree flags chain loops, links into free/bad/reserved/out-of-range clusters, cross-links, size-vs-chain, lost cluster chains, an undersized FAT, and FAT-mirror divergence; repair is FAT-only, byte-verified against `fsck_msdos`. **exFAT** (`exfat_fsck.rs`): allocation-bitmap reconciliation against the traced directory tree (contiguous + FAT-chained), boot checksum + backup-region consistency + VolumeDirty; repair rebuilds the bitmap and resyncs the boot regions, byte-verified against `fsck_exfat`. **ext** (`ext_fsck.rs`): block + inode bitmap + free-count reconciliation against the computed allocation (metadata from group descriptors + inode-owned blocks incl. reserved inodes / journal / reserved-GDT), byte-verified against `e2fsck`; repair withheld on metadata_csum volumes. **NTFS** (`ntfs_fsck.rs`): `$Bitmap` reconciliation against the MFT walk (in-use MFT records' non-resident data runs), `$MFTMirr` sync vs the first 4 MFT records, backup-boot-sector vs VBR consistency, and VolumeDirty flag; repair rewrites `$Bitmap`, resyncs `$MFTMirr`, rewrites the backup boot sector, and clears VolumeDirty — oracle-verified against Windows `chkdsk`. Volumes carrying `$ATTRIBUTE_LIST`-spilled data are surfaced with a warning rather than mis-traced. **Tier complete.** |
| **JFS repair** | JFS | M | `fsck()` exists (check-only); add the `repair()` branch the code comments already scope. |
| **Amiga** | PFS3, SFS | M each | Mirror the AFFS Disk-Validator model (bitmap + directory-tree walk, set-bit-free convention). |
| **Retro long-tail** | ~~CBM~~ (done), Atari DOS, RS-DOS, OS-9, DragonDOS, DFS, ProDOS, CP/M, MFS, Human68k, Alto BFS, ADFS, QDOS | S each | Lightweight consistency checks: BAM/VTOC/granule/allocation-bitmap vs directory chains, orphan detection, free-count reconciliation. Small formats → small checkers. Repair where a replica or recomputable structure exists. **CBM shipped** as the template: recompute the BAM from the directory + file chains (the VALIDATE model), diff against the on-disk BAM, rewrite; byte-verified against `c1541 validate` fixtures for all five variants. |

### 1c. Add create-blank (`rb-cli new --fs …`)

Editable but can't yet format a blank volume. Formatters are well-specified and
independently testable (mount/inspect the output). Order by value:

| Tier | Filesystems | Effort |
|---|---|---|
| Unix/Linux | ~~**ext** (ext2)~~ **done** (`ext_format.rs`), **UFS** (newfs-lite) | M each |
| Apple / retro workhorses | **ProDOS**, **Apple DOS 3.3**, **CP/M** (per-DPB), **Atari DOS** (promote test-only), **OS-9** | S–M each |
| Others | **QDOS**, **ADFS** (E-format), **MFS** | S–M each |

### 1d. Finish the two detect-only scaffolds

| Scaffold | Work | Effort |
|---|---|---|
| **ANDOS** (BK0011M) | Implement browse/extract (boot-block + catalogue), then edit. Sparse public docs → gate on real-disc fixtures. | M |
| **QL Microdrive** (`.mdv`) | Implement the directory walk (read), then edit. Needs a real-hardware oracle. | M |

---

## Part 2 — New filesystems (from the gap analysis)

For each unsupported filesystem the target is the same quartet, built in order
**Browse → Edit → Create → fsck**. Sequenced by retro/MiSTer value (bands mirror
the [audit](filesystem_coverage_audit.md#6-gap-analysis--what-is-not-supported)).
Some are **read-first** where a correct writer is disproportionately hard or
risky — noted as such; "where possible" applies.

### High value

| Filesystem | Systems | Quartet feasibility | Effort to browse |
|---|---|---|---|
| **UCSD p-System** | Apple II/III, PC | Full quartet realistic (simple block FS) | M |
| **HPFS** | OS/2 | Browse + edit realistic; fsck M | M–L |
| **Minix FS** | Minix, early Linux | Full quartet easy (small, documented) | S–M |
| **TR-DOS** | ZX Spectrum | Full quartet (fixed-geometry floppy) | M |
| **TRSDOS / LDOS** | TRS-80 | Browse + edit; variants complicate | M |
| **TI-99 FS** | TI-99/4A | Full quartet (VIB/FDIR) | M |
| **Sedoric / Oric DOS** | Oric | Full quartet | M |
| **N88-BASIC** | NEC PC-8801 | Full quartet (shares `.d88`) | M |
| **SpartaDOS / MyDOS** | Atari 8-bit | Extend the Atari DOS driver | S–M |
| **APFS** | Modern macOS | **Read-first** — write is L and risky | L |
| **ZFS** | Solaris/BSD/Linux | **Read-first** — write out of scope | L |

### Medium value

Finish **ADFS write + double-sided `.dsd`**; commercial-Unix **VxFS / AdvFS /
QFS** (browse-first); **OpenVMS ODS-2 on a raw disk** (already read on optical);
**Apple SOS / DOS 3.2 / Pascal**; optical **UDF 2.50+ / El Torito**.

### Low value

Finish the Soviet/hobby scaffolds and niche cores (CSIDOS, MicroDOS,
Specialist-MX); **Reiser4 / NILFS / F2FS / bcachefs** browse only where they
collide with supported magics.

---

## Recommended sequencing

The order that buys the most capability per unit effort:

1. **fsck for FAT / exFAT / NTFS / ext** — promote the four most-used
   filesystems from `validate` to real check + repair. Biggest user impact.
   **All four done** (`src/fs/fat_fsck.rs`, `exfat_fsck.rs`, `ext_fsck.rs`,
   `ntfs_fsck.rs`) — **Step 1 complete**.
2. **create-blank for ext + UFS** — completes the two Unix workhorses (both are
   already read + edit; UFS already fscks/repairs). **ext2 + ext3 done**
   (`src/fs/ext_format.rs`, oracle-verified `e2fsck`-clean). **UFS deferred** to a
   follow-up gated on the independent NetBSD `makefs` + FreeBSD `fsck_ffs` oracle
   (neither runs on macOS) — target UFS1/LE/SB@8192, single cylinder group,
   lost+found = inode 3, reusing the existing `ufs.rs` write stack; author
   `scripts/ufs-oracle.sh` (modeled on `xfs-oracle.sh`) on the WSL/BSD box.
3. **Amiga PFS3 + SFS fsck** — brings the Amiga trio to parity with AFFS.
4. **Retro long-tail fsck + create** — small formats, small checkers/formatters
   (CBM, Atari, ProDOS, CP/M, RS-DOS, DragonDOS, DFS, OS-9, Human68k, Alto).
5. **JFS repair** — the one read-only FS whose repair is already scoped.
6. **New high-value filesystems** — Minix and UCSD p-System first (cheapest full
   quartet), then the MiSTer-core long-tail (TR-DOS, TI-99, Oric, N88-BASIC,
   TRSDOS), then HPFS.
7. **Read-only-to-edit for btrfs / JFS / APFS / ZFS** — last; large effort, and
   read is the realistic ceiling for the CoW/checksummed ones.

Milestones 1–3 alone take the editable set from 4 fully-complete filesystems to
roughly a dozen, covering every filesystem a typical user actually writes to.

## Tracking

As each item lands, move the affected row in the
[coverage audit](filesystem_coverage_audit.md) and the README Filesystems table,
and re-grade any [MiSTer core](full_MiSTer_support_status.md) that depended on
it — per the CLAUDE.md pre-commit documentation-sync checklist.

## Adjacent bugs found + fixed while doing this work

- **`create_blank_fat` mis-sized the FAT for FAT12-labelled volumes above ~2 MB
  — FIXED.** The formatter's type picker used raw size thresholds
  (`<= 32 MiB -> FAT12`) that ignored the 4084-cluster FAT12 ceiling, so e.g. an
  8 MiB volume was labelled FAT12 with 8160 clusters while its FAT was sized for
  12-bit entries (24 sectors). `FatFilesystem::open` then re-derived FAT16 from
  the cluster count and read 16-bit entries out of a FAT that only held ~6144 of
  them — the tail clusters were unaddressable. `fsck_msdos` rejected such images;
  our reader silently tolerated them (out-of-range entries read as free).
  `compute_fat_blank_layout_with_sector_size` now derives the FAT type from the
  cluster count (matching `open`) and sizes the FAT precisely for it — an 8 MiB
  volume is a proper FAT16 (spf 32, not 24). Guarded by
  `fat_fsck::tests::create_blank_fat_is_geometrically_valid_across_sizes` and the
  `fsck_msdos` oracle across 2 MiB–128 MiB. The new FAT fsck also *detects* the
  undersized condition (`FatTooSmallForClusters`) for images formatted by older
  builds or other broken tools.
