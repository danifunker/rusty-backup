# Rusty Backup — Filesystem Support Matrix

At-a-glance support checklist for every filesystem Rusty Backup understands.
This is the quick reference; the detailed prose, per-family notes, gap analysis,
and CP/M DPB list live in the **[coverage audit](filesystem_coverage_audit.md)**,
and the roadmap for filling the empty cells lives in the
**[completion plan](filesystem_completion_plan.md)**.

_Last updated: 2026-07-10 (PFS3 + SFS fsck shipped; El Torito boot-image
extract/replace shipped)._

## Legend

| Value | Meaning |
|---|---|
| **Yes** | Fully supported |
| **Partial** | Works with documented limitations |
| **Read-only** | Browse/extract only — no write path |
| **—** | Not applicable (e.g. fixed-geometry floppy can't resize) |
| **No** | Not implemented (yet) |

Columns: **Browse** (read/list/extract) · **Create** (`rb-cli new` blank format)
· **Edit** (add / delete / rename) · **Resize** (shrink / grow) · **fsck**
(integrity check — `(repair)` = also fixes, `(check)` = reports only,
`validate` = lightweight consistency check).

Everything below is also backed up / restored at the raw-sector level regardless
of the filesystem-aware cells.

## PC / DOS / Windows

| Filesystem | Browse | Create | Edit | Resize | fsck | Systems / era |
|---|:--:|:--:|:--:|:--:|:--:|---|
| FAT12 / FAT16 / FAT32 | Yes | Yes | Yes | Yes (in-place + defrag) | Yes (repair) | DOS, Windows 3.x/9x, MSX, universal |
| exFAT | Yes | Yes | Yes | Yes (in-place + defrag clone) | Yes (repair) | Modern removable media, large SD/CF |
| NTFS | Yes | Yes | Yes | Yes (in-place + defrag clone) | Yes (repair) | Windows NT / 2000 / XP+ |

## Classic Mac & Apple

| Filesystem | Browse | Create | Edit | Resize | fsck | Systems / era |
|---|:--:|:--:|:--:|:--:|:--:|---|
| MFS | Yes | No | Yes | No | No | Mac 128K / 512K / Plus, 400 KB SS floppies (1984–86) |
| HFS (Mac OS Standard) | Yes | Yes | Yes | Yes (block-size clone) | Yes (repair) | Classic Mac OS (System 7–9), 68k / early PPC |
| HFS+ / HFSX | Yes | Yes | Yes | Yes (defrag clone) | Yes (repair) | Mac OS 8.1 → macOS; embedded + wrapped variants |
| HFV (flat HFS) | Yes | Yes (`--fs hfv`) | Yes | Yes | Yes (repair) | BasiliskII / SheepShaver; classic HFS ≤ 2047 MB, no APM |
| ProDOS | Yes | No | Yes | Yes (in-place) | validate | Apple II / IIgs |
| Apple DOS 3.3 | Yes | No | Yes | No | No | Apple II 5.25" (`.dsk` / `.do` / `.po`) |
| Apple Lisa FS | Read-only | No | No | No | No | Apple Lisa — reconstructed from sector tags |

## Linux / Unix / BSD / SGI

| Filesystem | Browse | Create | Edit | Resize | fsck | Systems / era |
|---|:--:|:--:|:--:|:--:|:--:|---|
| ext2 / ext3 / ext4 | Yes | Yes | Yes | Yes (in-place) | Yes (repair, incl. ext4 `metadata_csum`) | Linux |
| btrfs | Yes | No | Read-only | Yes (volume resize) | validate | Modern Linux |
| XFS (v4 / v5) | Yes | No | Partial (v4 edit) | Grow only (`xfs_growfs`) | Yes (repair) | SGI IRIX 6.x, Linux |
| JFS (JFS2) | Yes | No | Read-only | Compaction | Yes (check) | IBM OS/2 Warp Server, AIX 5+, Linux JFS2 |
| ReiserFS (v3.5 / v3.6) | Yes | No | Read-only | Compaction | No | Linux, late-1990s → mid-2000s |
| UFS / FFS (UFS1 / UFS2) | Yes | No | Yes | Compaction | Yes (repair) | 4.2/4.4BSD, FreeBSD, SunOS/Solaris, NeXTSTEP |
| SGI EFS | Yes | Yes | Yes | Yes (in-place + aggressive shrink) | Yes (repair) | SGI IRIX < 6.0 |

## Amiga

| Filesystem | Browse | Create | Edit | Resize | fsck | Systems / era |
|---|:--:|:--:|:--:|:--:|:--:|---|
| AFFS (OFS / FFS, Intl, DirCache) | Yes | Yes | Yes | Yes (in-place) | Yes (repair — Disk Validator) | Commodore Amiga (`DOS\0`..`DOS\7`) |
| PFS3 / PDS3 / muFS | Yes | Yes | Yes | Yes (in-place + defrag clone) | Yes (repair) | Amiga Professional File System 3 |
| SFS (Smart File System) | Yes | Yes | Partial (single-leaf btree) | Yes (in-place) | Yes (repair) | Amiga `SFS\0` / `SFS\2` |

## 8-bit & home micros

| Filesystem | Browse | Create | Edit | Resize | fsck | Systems / era |
|---|:--:|:--:|:--:|:--:|:--:|---|
| CBM DOS (1541/1571/1581/8050/8250) | Yes | Yes | Yes | — (fixed) | Yes (repair — VALIDATE) | Commodore C64 / 128 / VIC-20 / C16 / PET |
| Atari DOS 2 (2.0S / 2.5) | Yes | No | Yes | — (fixed) | No | Atari 8-bit (400/800/XL/XE) |
| RS-DOS / Disk BASIC | Yes | Yes | Yes | — (fixed) | No | Tandy CoCo 1 / 2 / 3 |
| OS-9 / NitrOS-9 RBF | Yes | No | Yes | — (fixed) | No | Tandy CoCo, Dragon, 6809 systems |
| DragonDOS | Yes | Yes | Yes | — (fixed) | No | Dragon 32 / 64 |
| Acorn DFS | Yes | Yes | Yes | — (fixed) | No | BBC Micro / Master, Acorn Electron |
| Acorn ADFS / FileCore | Yes | No | Partial (create/delete, E-format) | No | No | Acorn Archimedes, BBC Master, RISC OS |
| CP/M (2.2 / 3 / Plus) | Yes | No | Yes | No | No | Amstrad, Einstein, SV-328, Altair, ZX +3 (9 DPBs) |
| Human68k (FAT12 / FAT16) | Yes | Yes | Yes | Yes (HDD in-place + defrag repack) | No | Sharp X68000 (big-endian FAT) |
| QDOS (QXL.WIN) | Yes | No | Yes | Yes (in-place resize) | No | Sinclair QL hard-disk container |

## Xerox research systems

| Filesystem | Browse | Create | Edit | Resize | fsck | Systems / era |
|---|:--:|:--:|:--:|:--:|:--:|---|
| Alto BFS / TFS | Yes | Yes | Yes | Yes (resize) | No | Xerox Alto (Diablo 31/44), Trident T-80/T-300 |
| Pilot / Cedar | Yes | (test infra) | Read-only | No | No | Xerox D-machines (Dolphin / Dorado / Dandelion) |

## Detect-only scaffolds & fallback

| Filesystem | Browse | Notes |
|---|:--:|---|
| QDOS Microdrive (`.mdv`) | Detect-only | Cartridge name surfaced; directory walk not implemented |
| ANDOS | Detect-only | Signature probe only; Soviet BK0011M / Elektronika BK |
| Carve (raw recovery) | Yes (synthetic) | Last-resort for unmountable / NDOS images |

## Optical-disc filesystems (read-only)

Browse + extract via the [`opticaldiscs`](https://github.com/danifunker/opticaldiscs-rs)
crate (`optical` feature) — no edit / resize / fsck. El Torito boot **images**
are extracted (`optical boot extract`) and then handled by the disk-image
filesystems above; `optical boot replace` writes an edited image back.

| Filesystem | Typical discs |
|---|---|
| ISO 9660 (+ Joliet, Rock Ridge) | PC / Unix / mixed data CDs & DVDs |
| High Sierra | Pre-ISO 9660 CD-ROMs |
| UDF (1.02–2.01) | DVDs, data discs (2.50+ metadata-partition = detect-only) |
| HFS / HFS+ | Classic Mac & Mac OS X CDs, Mac/PC hybrids |
| SGI EFS | IRIX install / distribution CDs |
| UFS / FFS | Tru64 / Solaris CDs, NeXT / OpenStep / Rhapsody |
| VMS ODS-2 / Files-11 | OpenVMS (VAX / Alpha) discs |

---

## Keeping this current

When a filesystem gains or loses a capability, update the matching cell **here**
and in the [coverage audit](filesystem_coverage_audit.md) (and the README
Filesystems table + [MiSTer status](full_MiSTer_support_status.md) if a core is
affected) in the same commit — per the CLAUDE.md pre-commit documentation-sync
checklist.
