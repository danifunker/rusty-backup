# Rusty Backup — Filesystem & Format Coverage Audit

A single-source, **code-grounded** map of every filesystem, container, and
partition table Rusty Backup can handle today, and — just as important — the
notable ones it **cannot** yet handle. Generated 2026-07-07 by reading the
`src/fs/mod.rs` dispatch hub, `src/partition/`, `src/rbformats/`,
`src/model/file_types.rs`, and every driver's trait impls directly (not the
prose docs, which had drifted — see [§7](#7-known-code-gaps--drift-fixed-in-this-sweep)).

Companion docs: [`full_MiSTer_support_status.md`](full_MiSTer_support_status.md)
grades each MiSTer computer core; [`../README.md`](../README.md) is the
user-facing feature list; [`filesystem_completion_plan.md`](filesystem_completion_plan.md)
turns the gaps in §6 into a sequenced work-list. This file is the exhaustive
engine-level view.

## How to read the support columns

| Column | Meaning |
|---|---|
| **Detect** | `Auto` = recognized from on-disk signature/geometry; `String` = caller must declare the format (no on-disk signature); `via X` = reached only through another path |
| **Browse** | Read + list + extract files (Inspect tab, `rb-cli ls` / `get`) |
| **Edit** | Create / delete / write files (Edit Mode, `rb-cli put` / `rm` / `mkdir`) — implements `EditableFilesystem` **and** is wired into the edit dispatch |
| **Create** | Format a blank volume (`rb-cli new --fs`) |
| **Shrink/Grow** | Compact to minimum for backup and/or re-expand on restore. `compaction` = backup-side size reduction only; `in-place` = true volume resize |
| **fsck** | `check+repair` = detects and fixes; `check` = detects/reports only; `validate` = restore-time integrity gate, no interactive check |

Everything below is a **disk image or physical disk**; backup/restore itself
works on *any* filesystem at the raw sector level — these columns describe the
filesystem-*aware* value-adds (browse, edit, shrink, fsck) on top of that.

---

## 1. Supported filesystems

### PC / DOS / Windows

| Filesystem | Detect | Browse | Edit | Create | Shrink/Grow | fsck | Systems / era |
|---|---|---|---|---|---|---|---|
| FAT12 / FAT16 / FAT32 | Auto | Yes | Yes | Yes | Yes (in-place + defrag) | Yes (check + repair) | DOS, Windows 3.x/9x, MSX, universal |
| exFAT | Auto | Yes | Yes | Yes | Yes (in-place + defrag clone) | Yes (check + repair) | Modern removable media, large SD/CF |
| NTFS | Auto | Yes | Yes | Yes | Yes (in-place + defrag clone) | validate | Windows NT / 2000 / XP+ |

### Classic Mac & Apple

| Filesystem | Detect | Browse | Edit | Create | Shrink/Grow | fsck | Systems / era |
|---|---|---|---|---|---|---|---|
| MFS (Macintosh File System) | Auto | Yes | Yes | No | No | No | Mac 128K / 512K / Plus, 400 KB SS floppies (1984–86) |
| HFS (Mac OS Standard) | Auto | Yes | Yes | Yes | Yes (block-size clone) | check+repair | Classic Mac OS (System 7–9), 68k / early PPC |
| HFS+ / HFSX | Auto | Yes | Yes | Yes | Yes (defrag clone) | check+repair | Mac OS 8.1 → macOS; embedded + wrapped variants |
| ProDOS | Auto / MBR 0xA8 / APM | Yes | Yes | No | Yes (in-place) | validate | Apple II / IIgs |
| Apple DOS 3.3 | Auto (140 KB only) | Yes | Yes | No | No | No | Apple II 5.25" (`.dsk` / `.do` / `.po`, sector-order auto) |
| Apple Lisa FS | Auto (tag-bearing DC42/DART) | Yes | No | No | No | No | Apple Lisa — reconstructed from 12-byte sector tags |

> **HFV** is the *write* side of classic HFS for BasiliskII / SheepShaver: a
> flat HFS volume with no APM wrapper, created via `rb-cli new --fs hfv`.
> Reads/edits/fscks flow through the HFS path; only creation is HFV-specific,
> capped at classic HFS ≤ 2047 MB.

### Linux / Unix / BSD

| Filesystem | Detect | Browse | Edit | Create | Shrink/Grow | fsck | Systems / era |
|---|---|---|---|---|---|---|---|
| ext2 / ext3 / ext4 | Auto | Yes | Yes | No | Yes (in-place) | Yes (check + repair; ext4 metadata_csum check-only) | Linux |
| btrfs | Auto | Yes | **No** (read-only) | No | Yes (volume resize) | validate | Modern Linux |
| XFS (v4 / v5) | Auto | Yes (v4 edit) | Yes (v4) | No | Grow only (disk-layout + `xfs_growfs`) | check+repair | SGI IRIX 6.x, Linux |
| JFS (JFS2) | Auto | Yes | **No** (read-only) | No | compaction | check | IBM OS/2 Warp Server, AIX 5+, Linux JFS2 |
| ReiserFS (v3.5 / v3.6) | Auto | Yes | **No** (read-only) | No | compaction | No | Linux, late-1990s → mid-2000s |
| UFS / FFS (UFS1 / UFS2) | Auto | Yes | Yes | No | compaction | check+repair | 4.2/4.4BSD, FreeBSD, SunOS / Solaris, NeXTSTEP |

> UFS is browse + edit (create / delete / rename) + fsck with repair (replica
> superblock, block bitmap, orphan → `lost+found`). Its edit path was wired
> into the dispatch as part of this sweep — see [§7](#7-known-code-gaps--drift-fixed-in-this-sweep).

### SGI

| Filesystem | Detect | Browse | Edit | Create | Shrink/Grow | fsck | Systems / era |
|---|---|---|---|---|---|---|---|
| SGI EFS | Auto | Yes | Yes | Yes | Yes (in-place + aggressive shrink) | check+repair | SGI IRIX < 6.0 |
| SGI XFS | *(see Linux/Unix row above — same driver)* ||||||

### Amiga

| Filesystem | Detect | Browse | Edit | Create | Shrink/Grow | fsck | Systems / era |
|---|---|---|---|---|---|---|---|
| AFFS (OFS / FFS, Intl, DirCache) | Auto `DOS\0`..`DOS\7` | Yes | Yes | Yes | Yes (in-place) | check+repair (Disk Validator) | Commodore Amiga |
| PFS3 / PDS3 / muFS | String | Yes | Yes | Yes | Yes (in-place + defrag clone) | No | Amiga Professional File System 3 |
| SFS (Smart File System) | String | Yes | Yes (single-leaf btree) | Yes | Yes (in-place) | No | Amiga `SFS\0` / `SFS\2` |

### 8-bit & home micros

| Filesystem | Detect | Browse | Edit | Create | Shrink/Grow | fsck | Systems / era |
|---|---|---|---|---|---|---|---|
| CBM DOS (1541 / 1571 / 1581 / 8050 / 8250) | Auto | Yes | Yes | Yes | — (fixed geometry) | Yes | Commodore C64 / 128 / VIC-20 / C16 / PET. fsck = VALIDATE (BAM reconciliation), rewrite byte-verified vs `c1541 validate` |
| Atari DOS 2 (2.0S / 2.5) | Auto | Yes | Yes | No | — (fixed geometry) | No | Atari 8-bit (400/800/XL/XE) |
| RS-DOS / Disk BASIC | Auto | Yes | Yes | Yes | — (fixed geometry) | No | Tandy CoCo 1 / 2 / 3 |
| OS-9 / NitrOS-9 RBF | Auto | Yes | Yes | No | — (fixed geometry) | No | Tandy CoCo, Dragon, 6809 systems |
| DragonDOS | Auto | Yes | Yes | Yes | — (fixed geometry) | No | Dragon 32 / 64 |
| Acorn DFS | Auto | Yes | Yes | Yes | — (fixed geometry) | No | BBC Micro / Master, Acorn Electron |
| Acorn ADFS / FileCore | Auto / String | Yes | Partial (create/delete, E-format) | No | No | No | Acorn Archimedes, BBC Master, RISC OS |
| CP/M (2.2 / 3 / Plus) | **String** | Yes | Yes | No | No | No | Amstrad CPC/PCW, Einstein, SV-328, Altair, MultiComp, ZX +3 (9 DPBs — see §2) |
| Human68k (FAT12 / FAT16) | Auto / String | Yes | Yes | Yes | Yes (HDD in-place + defrag repack) | No | Sharp X68000 (big-endian FAT dialect) |
| QDOS (QXL.WIN) | Auto / String | Yes | Yes | No | Yes (in-place resize) | No | Sinclair QL hard-disk container |

### Xerox research systems

| Filesystem | Detect | Browse | Edit | Create | Shrink/Grow | fsck | Systems / era |
|---|---|---|---|---|---|---|---|
| Alto BFS / TFS | Auto | Yes | Yes | Yes | Yes (resize) | No | Xerox Alto (Diablo 31/44), Trident T-80/T-300 |
| Pilot / Cedar | Auto | Yes | No (read-only in GUI) | (test infra) | No | No | Xerox D-machines (Dolphin / Dorado / Dandelion) |

### Detect-only scaffolds & fallback

| Filesystem | Detect | Browse | Notes |
|---|---|---|---|
| QDOS Microdrive (`.mdv`) | Auto | **Detect only** | Cartridge name surfaced; directory walk not implemented (`Unsupported`) |
| ANDOS | Auto / String | **Detect only** | Signature probe surfaces "ANDOS"; browse returns `Unsupported`. Soviet BK0011M / Elektronika BK |
| Carve (raw recovery) | Fallback | Yes (synthetic) | Last-resort for unmountable / NDOS images: whole-disk blob + carved text/JSON runs + Amiga bootblock |

**Totals:** ~30 filesystem drivers — **26 editable+wired**, **7 with an
interactive fsck** (AFFS, EFS, HFS, HFS+, JFS, UFS, XFS — all repair except
JFS, which is check-only), **16 create-blank**, **2 detect-only scaffolds**,
**1 recovery fallback**.

---

## 2. CP/M disk-parameter blocks (DPBs)

CP/M has no on-disk signature, so the format is declared explicitly
(`--fs cpm:<preset>`). Nine presets ship (`src/fs/cpm_diskdefs.rs`):

| Preset | Machine / format |
|---|---|
| `amstrad_data` | Amstrad CPC 6128 data format (no reserved tracks) |
| `amstrad_sys` | Amstrad CPC 6128 system format (2 boot tracks) |
| `amstrad_pcw` | Amstrad PCW 8256/8512 Format A (3" CF2, 180 KB SS) |
| `einstein` | Tatung Einstein 80T DS 10×512 |
| `svi328_cpm` | Spectravideo SV-328 CP/M (80 KB SS, 40-track) |
| `altair_8in` | MITS Altair 8800 8" SSSD (26×128, 77 track) |
| `altair_cf` | Altair-Z80 CF / IDE image (8 MB chunks) |
| `multicomp` | Grant Searle MultiComp SD (80×10×512 SS) |
| `zxplus3` | ZX Spectrum +3 / +3DOS (Amstrad-data compatible) |

The DPB registry is the extension point for **any** other CP/M machine
(Kaypro, Osborne, Epson, TRS-80 CP/M, …) — a new format is a table entry, not
a new driver.

---

## 3. Optical-disc filesystems

Read through the [`opticaldiscs`](https://github.com/danifunker/opticaldiscs-rs)
crate (v0.7, `optical` feature). **Browse + extract only** — no edit / resize /
fsck. Container formats opened: ISO 9660 (`.iso` / `.toast`), BIN/CUE, CHD-CD,
MDF/MDS, NRG, CCD (raw 2352-byte-sector images auto-detected inside a bare
`.iso`).

| Filesystem | Typical discs |
|---|---|
| ISO 9660 (+ Joliet, Rock Ridge) | PC / Unix / mixed data CDs & DVDs |
| High Sierra | Pre-ISO 9660 CD-ROMs (early Microsoft / IBM titles) |
| UDF (1.02–2.01) | DVDs, data discs |
| HFS / HFS+ | Classic Mac & Mac OS X CDs / DVDs, "Mac/PC" hybrids |
| SGI EFS | IRIX install / distribution CDs |
| UFS / FFS | Tru64 / Solaris CDs, NeXT / OpenStep / Rhapsody discs |
| VMS ODS-2 / Files-11 | OpenVMS (VAX / Alpha) discs |

Gap: UDF 2.50+ metadata-partition discs (Blu-ray) are **detected only**.

---

## 4. Container / image formats

**Whole-disk wrappers** (`src/rbformats/`):

| Format | Read | Write | Notes |
|---|:---:|:---:|---|
| Raw (`.img/.raw/.bin/.dd/.hda/.hdv/.hdf/.hds`) | Yes | Yes | Passthrough, sparse zero-skip |
| VHD fixed / dynamic | Yes | Yes | + VHD export |
| QCOW2 v2/v3 | Yes | Yes | + in-place edit; `qemu-img`-clean |
| VMDK flat / sparse | Yes | Yes | Allocate-on-write edit |
| MAME CHD (disk + CD-cooked) | Yes | Yes | In-process libchdman-rs; expand |
| 2MG | Yes | Yes | Apple II |
| DiskCopy 4.2 (+ Twiggy) | Yes | Yes | Twiggy read-only |
| WOZ 1 / 2 | Yes | Yes | Apple nibble/bitstream |
| DART | Yes | No | Compressed Apple/Lisa (LZH/RLE) |
| UDIF DMG / NDIF | Yes | No | Compressed Apple images |
| WinImage IMZ | Yes | No | ZIP-wrapped (incl. encrypted) |
| Norton Ghost (`.gho/.ghs`) | Yes | No | File-aware + password + span sets |
| cb-dos (`.cbk`) | Yes | Yes | Native single-file backup |
| gzip (`.gz/.adz/.hdz`), `.zip` disk | Yes | `.adz/.hdz` | Transparent decompress |

**Floppy-container decoders** (`src/rbformats/containers/`):

| Kind | Read | Write | Machines |
|---|:---:|:---:|---|
| MSA | Yes | Yes | Atari ST |
| D88 | Yes | Yes | X68000, PC-88/98, MSX, FM-7 |
| XDF / HDM / DIM | Yes | Yes | X68000 / PC-98 / DiskExplorer |
| ATR | Yes | Yes | Atari 8-bit |
| G64 / G71 | Yes | Yes (from d64) | Commodore GCR |
| EDSK / DSK (CPCEMU) | Yes | **No** | Amstrad CPC/PCW, Einstein, Oric |

Gaps here: **EDSK write**, and the flux/track container family (TD0, IMD, HFE,
86F, IPF, SCP, NIB) — the [fluxfox](https://github.com/dbalsom/fluxfox) crate
is the candidate infrastructure (see MiSTer status doc §"Prior art").

---

## 5. Partition tables

| Scheme | Parse | Edit | Build | Notes |
|---|:---:|:---:|:---:|---|
| MBR | Yes | Yes | Yes | Incl. extended/logical, CHS patch |
| GPT | Yes | Yes | Yes | Protective-MBR paired, CRC refresh |
| APM (Apple) | Yes | Yes | Yes | 68k / PowerPC Macs |
| RDB (Amiga) | Yes | Bootable-flag only | — | RDSK + PART chain; carries `DosType` |
| SGI Volume Header | Yes | Yes | Yes | IRIX; 16 slots + volume dir |
| AHDI (Atari ST/TT/Falcon) | Yes | **No** | Yes | Big-endian, XGM chains |
| X68000 / Sharp | Yes | **No** | Yes | 256/512/1024-B sectors |
| None (superfloppy) | Yes (~30 fs hints) | — | — | FS auto-detected at sector 0 |

Gaps: **AHDI and X68000 partition editing** (build works, in-place edit bails);
BSD disklabels and Solaris VTOC slices as first-class partition schemes.

---

## 6. Gap analysis — what is *not* supported

No canonical "every filesystem ever made" list exists, but cross-referencing
[Wikipedia's list of file systems](https://en.wikipedia.org/wiki/List_of_file_systems)
against the mission — **backup / restore / inspect of vintage-computer disk
images, with a MiSTer-FPGA lean** — the gaps sort into four bands.

### High value (retro, disk-image, plausible demand)

| Filesystem | Systems | Why it matters | Prior art |
|---|---|---|---|
| **UCSD p-System** | Apple II/III, IBM PC, many | Cross-platform Pascal environment; common on Apple II archival disks | a2kit has partial p-System |
| **HPFS** | OS/2 | The defining OS/2 filesystem; PC-preservation gap | TotalImage (C#, portable) |
| **Minix FS** | Minix, early Linux | Small, well-documented; the FS Linux booted on in 1991 | Linux kernel `fs/minix` |
| **TR-DOS** | ZX Spectrum (Beta Disk) | MiSTer **ZX-Spectrum** core; native Spectrum disk FS | — |
| **TRSDOS / LDOS / NEWDOS** | TRS-80 | MiSTer **TRS-80** core | — |
| **TI-99 FS (VIB / FDIR)** | TI-99/4A | MiSTer **TI-99_4A** core | — |
| **Sedoric / Oric DOS** | Oric | MiSTer **Oric** core | — |
| **N88-BASIC** | NEC PC-8801 | MiSTer **PC88** core | — |
| **SpartaDOS / MyDOS / DOS 3** | Atari 8-bit | Alternative Atari DOSes beyond the supported DOS 2 | — |
| **APFS** (read) | Modern macOS | Rising relevance for later Mac SSD images | Apple spec public |
| **ZFS** (read) | Solaris, FreeBSD, Linux | Workstation & NAS preservation | OpenZFS |

### Medium value (niche machine or workstation)

- **ADFS write completion + double-sided `.dsd`** (upgrade the existing Partial)
- **SAM DOS / MasterDOS** (SAM Coupé), **Sharp MZ FD**, **Coleco EOS**,
  **Atom DOS** — each a MiSTer core in the floppy long-tail
- **AIX JFS1** — the pre-JFS2 format; currently detected then rejected
- **Veritas VxFS**, **Tru64 AdvFS**, **Solaris QFS/ZFS** — commercial-Unix
  workstation filesystems (we cover SGI EFS/XFS; the rest are gaps)
- **OpenVMS ODS-2 on a raw disk** — we read ODS-2 on *optical* only
- **Apple SOS** (Apple III), **Apple DOS 3.2** (13-sector), **Apple Pascal**
- **CD-i / Video-CD structure**, **El Torito boot-catalog** emulation-image
  extraction, **UDF 2.50+ metadata partition** (optical extras)

### Low value (Soviet / hobby / research)

- Finish the **ANDOS** and **QL Microdrive** scaffolds; **CSIDOS**,
  **MicroDOS** (Vector-06C), **Specialist-MX**
- **Reiser4**, **NILFS**, **F2FS**, **bcachefs**, **HAMMER/HAMMER2**
  (modern/experimental Linux & BSD — detected-not-read where they collide with
  supported magics)

### Explicitly out of scope

Not disk images of vintage machines — no plan to support:

- **Modern flash translation FS** — JFFS2, YAFFS, UBIFS, F2FS
- **Read-only Linux packing FS** — SquashFS, cramfs (possible exception: the
  Buildroot appliance images, if ever needed)
- **Network / distributed / clustered** — NFS, SMB/CIFS, AFS, Ceph, Lustre,
  GlusterFS, GFS2, OCFS2, ZFS-as-pool
- **Pseudo / virtual** — procfs, sysfs, tmpfs, overlayfs, FUSE shims
- **Mainframe** — z/OS VTOC/VSAM, VM/CMS, and other non-sector-image stores
- **Windows Server modern** — ReFS

---

## 7. Known code gaps & drift (fixed in this sweep)

Discrepancies between the code and the prose docs, found while auditing. The
doc-side items were corrected in the same commit as this file; the code-side
items are logged for follow-up.

**Doc drift (fixed):**

1. **JFS, ReiserFS, UFS shipped full read drivers but were missing from the
   README Filesystems table** — now listed.
2. README Filesystems table was also missing **MFS, Apple DOS 3.3, CP/M, ADFS,
   QDOS** (all implemented) — now listed.
3. MiSTer status doc understated **QDOS** as "read" — it is read + write +
   in-place resize.

**Code gaps (one fixed here, rest logged for follow-up):**

4. **UFS edit — FIXED in this sweep.** `impl EditableFilesystem for
   UfsFilesystem` was complete and fixture-tested but unreachable — no arm of
   `open_editable_filesystem` routed `"ufs"`. It is now wired into the `0x00`
   and `0x83` arms (mirroring the read dispatch), guarded by a dispatch-level
   regression test. UFS is read + edit + fsck-with-repair.
5. **XFS has no shrink or backup compaction — a known, accepted limitation.**
   It edits (v4) and fscks/repairs, but has no `CompactXfsReader` or in-place
   resize, so XFS backups are written full-size. Grow is disk-layout-level only
   (`xfs_growfs`); clone-into-fresh shrink is the planned path (OPEN-WORK §2.2).
6. **JFS fsck is check-only** — it implements `fsck()` but not `repair()` (the
   code comments flag repair as future work). Every other interactive fsck
   repairs: AFFS, EFS, HFS, HFS+, UFS (replica SB / bitmap / orphan), XFS (R1–R8).
7. **AIX JFS1 and Reiser4 are detected then rejected** — magic recognized,
   read not implemented.
8. **fsck reach exceeded the Inspect-tab "Check" button — FIXED.** The button
   gate (`is_checkable_type`) enabled only classic HFS + AmigaDOS, but
   browse-view and `rb-cli fsck` call `fs.fsck()` directly, so AFFS, EFS, HFS,
   HFS+, JFS, UFS, and XFS all actually run. The gate now also consults
   `is_checkable_fs_name(type_name)` — the resolved family the inspect grid
   shows after content-probing — so the Unix filesystems (SGI EFS, UFS/FFS,
   XFS, JFS) and HFS+ get the "Check" button too. The button lives inside the
   `partition_is_browsable` block, and fsck reuses the same `open_filesystem`
   factory as Browse, so routing is guaranteed to match. Guarded by
   `checkable_fs_name_covers_probed_unix_families`.

---

## Maintenance

When you add or change a filesystem / container / partition driver, update this
file alongside the three surfaces in [`../CLAUDE.md`](../CLAUDE.md)'s
"Pre-commit documentation sync" checklist (README tables,
`full_MiSTer_support_status.md`, `DISK_IMAGE_EXTS`). This audit is the
superset view; the others are the user- and core-facing slices of it.
