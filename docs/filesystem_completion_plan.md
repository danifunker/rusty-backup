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
| ext2/3/4 | **Yes** | **Yes** | **Yes** | — **complete**. Create: `rb-cli new --fs ext\|ext3\|ext4` — plain ext2, ext3 (+jbd2 journal), and ext4 (extents + `metadata_csum` + journal, 256-byte inodes), all `e2fsck`-clean. fsck: block+inode bitmap + free-count reconciliation, **plus full ext4 `metadata_csum` crc32c** verify + repair (superblock, descriptors, bitmaps). Edit: create/delete/rename keep `metadata_csum` ext4 `e2fsck`-clean (inode + directory-tail checksums; handles the uninit_bg optimization on real Linux images). All byte-verified against `e2fsck`. |
| UFS / FFS | No | Yes | **repair** | **create** |
| XFS (v4/v5) | No | Yes (v4) | **repair** | **create + v5 edit** (shrink = known limitation) |
| MFS | No | Yes | No | **create + fsck** |
| PFS3 | Yes | Yes | **Yes** | — **fsck done** (`pfs3_fsck.rs`: directory-tree + anode-chain walk reconciles both the data and reserved allocation bitmaps + free counters; rebuild is withheld when structural damage would make the walk incomplete) |
| SFS | Yes | Yes (single-leaf) | **Yes** | **multi-leaf edit** (fsck done — `sfs_fsck.rs`: metadata-block checksums + AdminSpaceContainer chain + object-tree walk reconcile the single block bitmap; repair is bitmap-only, safe at any btree depth) |
| ProDOS | **Yes** | Yes | **Yes** | — **complete**. Create: `rb-cli new --fs prodos` (boot + 4-block volume directory + bitmap; 8 KiB–32 MiB). fsck: volume-bitmap reconciliation against the directory-tree + file-index walk (seedling/sapling/tree + subdirs), byte-verified self-consistent; repair rebuilds the bitmap, withheld on cross-links / past-end blocks / directory-chain cycles. |
| Apple DOS 3.3 | **Yes** | Yes | **Yes** | — **complete**. Create: `rb-cli new --fs apple-dos` (140 KB non-bootable data disk, DOS-order). fsck: VTOC free-map reconciliation against the catalog + file T/S-list chains; repair rewrites the free map (leaked DOS-image sectors on tracks 0-2 are a benign warning, never reclaimed). |
| CBM DOS | Yes | Yes | **Yes** | — **fsck done** (VALIDATE; byte-verified vs `c1541 validate`) |
| Atari DOS 2 | **Yes** | Yes | **Yes** | — **complete**. Create: `rb-cli new --fs atari` (single density, promoted from the test-only formatter). fsck: VTOC-bitmap + free-count reconciliation against the directory's linked-sector chains; repair rewrites the bitmap + count (SD only — the DOS 2.5 VTOC2 region is surfaced as an unchecked warning). |
| RS-DOS | Yes | Yes | **Yes** | — **fsck done** (granule-table VALIDATE) |
| OS-9 / RBF | **Yes** | Yes | **Yes** | — **complete**. Create: `rb-cli new --fs os9` (35-track CoCo floppy: ident sector + bitmap + empty root). fsck: cluster-bitmap reconciliation against a directory-tree walk from the root FD; repair marks referenced-but-free clusters allocated, leaves reserved clusters (boot / reserved track) as a benign warning, and withholds on structural damage. |
| DragonDOS | Yes | Yes | **Yes** | — **fsck done** (sector-bitmap VALIDATE) |
| Acorn DFS | Yes | Yes | **Yes** | — **fsck done** (contiguous-file consistency) |
| Acorn ADFS | No | Partial | No | **finish edit + create + fsck** |
| Human68k | Yes | Yes | **Yes** | — **fsck done** (FAT VALIDATE + mirror sync) |
| CP/M | **Yes** | Yes | **Yes** | — **complete**. Create: `rb-cli new --fs cpm --cpm-preset <name>` (per-DPB 0xE5-fill). fsck: directory self-consistency (no on-disk free map) — cross-links, out-of-range pointers, invalid entries (CP/M 3 label/timestamp SFCBs recognized); repair reclaims invalid entries, cross-links surfaced read-only. |
| QDOS (QXL.WIN) | No | Yes | No | **create + fsck** |
| Alto BFS / TFS | Yes | Yes | **Yes (CLI)** | — **fsck done** (label/bitmap VALIDATE); `rb-cli fsck` has its own Alto branch (container open path); `--repair` in place for `.pdi` |

### 1b. Add fsck (check + repair) — the biggest single lever

Only 7 of 26 editable filesystems have an interactive fsck today. The trait
already supports it (`Filesystem::fsck()` + `repair()`); the pattern is
established (see `hfs_fsck/`, `efs_fsck.rs`, `affs_fsck.rs`, `ufs_fsck` in
`ufs.rs`). Work items, by tier:

| Target | Filesystems | Effort | Notes |
|---|---|---|---|
| **Most-used first** | ~~FAT~~ (done), ~~exFAT~~ (done), ~~NTFS~~ (done), ~~ext~~ (done) | M each | Promote the existing `validate` gate to a real check + repair. All four shipped. **FAT** (`fat_fsck.rs`): FAT-table walk over the directory tree flags chain loops, links into free/bad/reserved/out-of-range clusters, cross-links, size-vs-chain, lost cluster chains, an undersized FAT, and FAT-mirror divergence; repair is FAT-only, byte-verified against `fsck_msdos`. **exFAT** (`exfat_fsck.rs`): allocation-bitmap reconciliation against the traced directory tree (contiguous + FAT-chained), boot checksum + backup-region consistency + VolumeDirty; repair rebuilds the bitmap and resyncs the boot regions, byte-verified against `fsck_exfat`. **ext** (`ext_fsck.rs`): block + inode bitmap + free-count reconciliation against the computed allocation (metadata from group descriptors + inode-owned blocks incl. reserved inodes / journal / reserved-GDT), byte-verified against `e2fsck`; **ext4 `metadata_csum` volumes are now fully repaired** — the crc32c on the superblock, descriptors, and bitmaps is verified and recomputed via `ext_csum` (only the legacy gdt_csum-only crc16 case stays withheld). **NTFS** (`ntfs_fsck.rs`): `$Bitmap` reconciliation against the MFT walk (in-use MFT records' non-resident data runs), `$MFTMirr` sync vs the first 4 MFT records, backup-boot-sector vs VBR consistency, and VolumeDirty flag; repair rewrites `$Bitmap`, resyncs `$MFTMirr`, rewrites the backup boot sector, and clears VolumeDirty — oracle-verified against Windows `chkdsk`. Volumes carrying `$ATTRIBUTE_LIST`-spilled data are surfaced with a warning rather than mis-traced. **Tier complete.** |
| ~~**JFS repair**~~ (done) | JFS | M | `repair()` shipped: orphaned (allocated-but-unreachable) fileset inodes are adopted into `/lost+found/ino_<inum>`, creating `/lost+found` when absent. Pulled forward the JFS edit primitives (inode allocator + inline dtree insert + dinode write-back) behind `EditableFilesystem`; general create/delete/rename stay `Unsupported`. Every structural write is oracle-verified against real `fsck.jfs` (`scripts/jfs-oracle.sh`). |
| **Amiga** | ~~PFS3~~ (done), ~~SFS~~ (done) | M each | Mirror the AFFS Disk-Validator model (bitmap + directory-tree walk, set-bit-free convention). **Both shipped.** PFS3 (`pfs3_fsck.rs`): both bitmaps + free counters reconciled against the tree/anode walk; no block checksums, so structural damage is surfaced read-only. SFS (`sfs_fsck.rs`): metadata-block checksums + AdminSpaceContainer chain + object-tree walk reconcile the single block bitmap; repair is bitmap-only (safe at any btree depth). Both withhold bitmap rewrite when the structural walk is incomplete. |
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
| ~~**UCSD p-System**~~ (done) | Apple II/III, PC | **Full quartet shipped** — browse + edit (contiguous first-fit) + create-blank (`rb-cli new --fs ucsd`) + fsck (directory self-consistency: bounds / overlaps / file-count, re-sort + count repair). No standard oracle builds here (needs Boost+libexplain+libfuse), so validated against an independent clean-room `scripts/ucsd-oracle.py`, per the RS-DOS clean-room precedent | M |
| **HPFS** | OS/2 | Browse + edit realistic; fsck M | M–L |
| ~~**Minix FS**~~ (done) | Minix, early Linux | **Full quartet shipped** — V1/V2/V3 browse + edit + create-blank (`rb-cli new --fs minix{,2,3}`, mkfs.minix-parity geometry) + fsck (bitmap/link-count reconciliation + orphan adoption into `/lost+found`), every write verified against Linux `fsck.minix` | S–M |
| ~~**TR-DOS**~~ (done) | ZX Spectrum | **Full quartet shipped** — browse + edit (create / delete / rename; append at the first-free high-water mark, tombstone deletes) + create-blank (`rb-cli new --fs trdos`) + fsck (contiguous-packing check + disk-info-counter reconciliation, repair withheld on structural damage). Raw `.trd`, geometry from the disk-info type byte. Validated against an independent clean-room oracle (`scripts/trdos-oracle.py`) | M |
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
   already read + edit; UFS already fscks/repairs). **ext done — ext2 + ext3 +
   ext4** (`src/fs/ext_format.rs` + `ext_csum.rs`, oracle-verified `e2fsck`-clean;
   ext4 also gained `metadata_csum` fsck-repair and checksum-aware editing).
   **UFS deferred** to a
   follow-up gated on the independent NetBSD `makefs` + FreeBSD `fsck_ffs` oracle
   (neither runs on macOS) — target UFS1/LE/SB@8192, single cylinder group,
   lost+found = inode 3, reusing the existing `ufs.rs` write stack; author
   `scripts/ufs-oracle.sh` (modeled on `xfs-oracle.sh`) on the WSL/BSD box.
3. ~~**Amiga PFS3 + SFS fsck**~~ — **DONE**. Brings the Amiga trio to parity with
   AFFS: `src/fs/pfs3_fsck.rs` + `src/fs/sfs_fsck.rs`, both self-consistency-verified
   (format → corrupt bitmap → detect → repair → clean) and gated into
   `is_checkable_type` so the CLI `fsck` verb and Inspect-tab Check button light up
   for the PFS3 (`PFS\3`/`PDS\3`/`muFS`) and SFS (`SFS\0`/`SFS\2`) DosTypes.
4. **Retro long-tail fsck + create** — small formats, small checkers/formatters
   (CBM, Atari, ProDOS, CP/M, RS-DOS, DragonDOS, DFS, OS-9, Human68k, Alto).
   **fsck sweep DONE for the five already-create+edit-complete formats:**
   DragonDOS (sector bitmap), RS-DOS (granule table), Acorn DFS
   (contiguous-file), Human68k (big-endian FAT + mirror), Alto BFS/TFS
   (label/bitmap) — all on the CBM VALIDATE template, self-consistency-verified.
   The first four wire through the block-reader factory (`rb-cli fsck` + the
   Inspect Check button light up automatically); Alto reaches `rb-cli fsck` via
   a dedicated Alto branch in the fsck verb (its packs open through the
   `open_pack` container path, not the factory), with `--repair` in place for
   `.pdi` inputs. **ProDOS create-blank + fsck now done too:**
   `rb-cli new --fs prodos` formats a bare volume (boot + 4-block volume
   directory + bitmap), and `prodos.rs` reconciles the volume bitmap against
   the directory-tree/file-index walk (block-factory-reachable, so `rb-cli
   fsck` + the Inspect Check/Repair button light up automatically). **Atari
   DOS create-blank + fsck now done too:** `rb-cli new --fs atari` formats a
   single-density disk, and `atari_dos.rs` reconciles the VTOC bitmap +
   free-count against the directory's linked-sector chains (block-factory
   reachable; the fsck check verb now peels floppy containers like `.atr` so
   it works on wrapped images too). **Apple DOS 3.3 create-blank + fsck now
   done too:** `rb-cli new --fs apple-dos` formats a 140 KB data disk, and
   `apple_dos.rs` reconciles the VTOC free map against the catalog + file
   T/S-list chains (the tracks-0-2 DOS image is a benign warning, never
   reclaimed). **CP/M create-blank + fsck now done too:** `rb-cli new --fs cpm
   --cpm-preset <name>` formats a per-DPB blank, and `cpm.rs` runs a directory
   self-consistency check (CP/M has no free map) — cross-links, out-of-range
   pointers, and invalid entries (CP/M 3 label / timestamp SFCBs recognized as
   valid). The fsck verb gained a `--fs-type` flag so a signatureless CP/M
   image is dispatchable. **OS-9 create-blank + fsck now done too:** `rb-cli
   new --fs os9` formats a 35-track CoCo floppy, and `os9.rs` walks the
   directory tree from the root FD to reconcile the cluster bitmap (reserved
   clusters — boot / last track — are a benign warning, never freed). **Step 4
   retro long-tail complete: all create + fsck gaps closed.**
5. ~~**JFS repair**~~ — **done**: orphan-inode adoption into `/lost+found`,
   oracle-verified against real `fsck.jfs`. Pulled forward the JFS inode
   allocator + inline dtree insert + dinode write-back behind
   `EditableFilesystem` (general create/delete/rename still `Unsupported`).
6. **New high-value filesystems** — ~~Minix~~ **done** (full quartet, V1/V2/V3,
   `fsck.minix`-verified), ~~UCSD p-System~~ **done** (full quartet,
   clean-room-oracle-verified), and ~~TR-DOS~~ **done** (full quartet, ZX
   Spectrum Beta Disk `.trd`, clean-room-oracle-verified); next the remaining
   MiSTer-core long-tail (TI-99, Oric, N88-BASIC, TRSDOS), then HPFS.
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
