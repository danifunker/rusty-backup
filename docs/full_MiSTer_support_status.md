# MiSTer Computer Cores — Rusty Backup Filesystem Support Status

This document cross-references every **computer** core listed on the MiSTer
wiki (<https://mister-devel.github.io/MkDocs_MiSTer/cores/computer/>) against
the filesystems Rusty Backup currently implements, and estimates the work to
support the disk types (floppy / hard disk / CD-ROM) of the outstanding cores.

## What Rusty Backup supports today

- **Filesystems:** FAT12/16/32, exFAT, NTFS, HPFS (OS/2; read + edit + create +
  fsck), HFS, HFS+/HFSX,
  APFS (read-only browse + extract, incl. FileVault via password / recovery
  key; no snapshots),
  ext2/3/4, XFS,
  SquashFS v4.0 (read + edit — the read-only root filesystem of Buildroot and
  other appliance images, and of `casper/filesystem.squashfs` inside live-CD
  ISOs. Compressors read: gzip [fixture-tested] / XZ / LZMA / LZ4 / zstd;
  write: gzip / XZ / zstd. The format has no in-place write, so create /
  delete / mkdir / rename / symlink commit by rebuilding the whole image the
  way `mksquashfs` does — unchanged files keep their exact bytes, permissions,
  ownership, timestamps and xattrs. No fsck: SquashFS carries no checksums, so
  there is nothing to repair from),
  JFS, ReiserFS, UFS1/UFS2, btrfs, Minix V1/V2/V3 (read + edit + create + fsck),
  ProDOS (read + edit + create + fsck),
  Apple DOS 3.3 (read + edit + create + fsck),
  UCSD p-System (read + edit + create + fsck),
  MacPlus MFS (read + edit + create + fsck),
  Amiga OFS/FFS (AFFS) / PFS3 / SFS (all three creatable: `new volume affs` /
  `pfs3` / `sfs`; AFFS output mounts Read/Write under a real Kickstart 3.1,
  R-020), IRIX EFS (validated by IRIX 6.5's own fsck, which also mounts and
  writes to volumes we format, R-039), SGI EFS v1 (read-only — the original
  1986 Extent File System from the IRIS 2000 / 3000 series, System V
  directories and all; auto-detects the byte-swapped images period SGI disk
  controllers produce), CP/M (read + edit + create +
  fsck; multi-DPB: amstrad_data / amstrad_sys / amstrad_pcw / einstein /
  svi328_cpm / altair_8in / altair_cf / multicomp / zxplus3), Human68k,
  ADFS (read + edit [new-map + old-map D] + E-format create + new-map fsck),
  QDOS (QXL.WIN read + write + resize), QDOS Microdrive (detect-only scaffold),
  CBM DOS (1541 / 1571 / 1581 + PET 8050 / 8250 IEEE-488 read + write + fsck;
  add/delete bidirectionally cross-validated against the `c1541` / Python `d64`
  reference, fsck = VALIDATE with the BAM rewrite byte-verified against
  `c1541 validate`), Atari DOS 2 (2.0S / 2.5 read + write + create + fsck,
  `.atr` / `.xfd`),
  RS-DOS / CoCo Disk BASIC (read + write + fsck, raw 35- / 40-track `.dsk` /
  `.jvc`; cross-validated against an independent clean-room reader/writer
  derived from the toolshed `libdecb` semantics; fsck = granule-table
  reconciliation vs the directory chains, VALIDATE model),
  OS-9 / NitrOS-9 RBF (hierarchical Unix-like FS — read + write incl.
  subdirectories + create + fsck, raw `.dsk` / `.vdk`; cross-validated
  byte-exact against an independent clean-room RBF reader on real NitrOS-9
  toolshed disks, fsck validated clean on the real fixture),
  DragonDOS (Dragon 32/64 read + write + fsck, single- / double-sided 40-track
  `.dsk`; cross-validated byte-exact against an independent clean-room
  reader/writer and against real third-party DragonDOS disks; fsck =
  sector-bitmap reconciliation, VALIDATE model),
  Acorn DFS (BBC Micro / BBC Master / Acorn Electron read + write + fsck,
  single-sided `.ssd` 40-/80-track **and** double-sided `.dsd` — the two
  track-interleaved sides de-interleave into two Acorn DFS partitions
  (`@1` / `@2`), edits re-interleaving on save; flat catalogue in sectors 0–1,
  contiguous files in descending start-sector order, single-character
  directory namespaces; bidirectionally cross-validated byte-exact against
  an independent clean-room DFS reader/writer; fsck = contiguous-file
  consistency + canonical-order repair),
  TR-DOS (ZX Spectrum Beta Disk read + write + create + fsck, raw `.trd`
  80-/40-track single-/double-sided; a flat 128-entry catalogue of contiguous
  files packed from a first-free high-water mark; every write cross-validated
  against an independent clean-room oracle, fsck = catalogue-packing +
  disk-info-counter reconciliation),
  TI-99/4A (read + write + create + fsck, flat V9T9 `.dsk` SSSD/DSSD/DSDD; a
  VIB allocation bitmap + a sorted FDIR of extent-based File Descriptor Records,
  big-endian; every write cross-validated against BOTH MAME's `imgtool` reader
  and an independent clean-room oracle, fsck = bitmap-vs-directory-walk
  reconciliation),
  Oric Jasmin (read + edit + create + fsck, flat 256-byte-sector `.dsk`
  SS 178 KB / DS 356 KB; free map + chained directory + sector-list inodes;
  modeled on MAME's `fs_oric_jasmin` and cross-validated against `floptool`),
  ANDOS (detect-only scaffold), and the optical-disc filesystems (ISO 9660
  + Joliet / Rock Ridge, High Sierra, UDF, HFS, HFS+, SGI EFS, UFS/FFS,
  VMS ODS-2, plus the video-game console filesystems Nintendo GameCube / Wii,
  Philips CD-i, and 3DO Opera — browse/extract, see **Optical / CD-ROM**
  below).
- **Partition tables:** MBR, GPT, APM, Amiga RDB, Atari AHDI, Sun SMI VTOC,
  SGI volume header, Sharp X68000 — every one of them can now be **written**
  from scratch as well as parsed (`rb-cli new hd <table>`). The pre-IRIX SGI
  disk label (IRIS 2000 / 3000) is parsed read-only.
- **Containers:** CHD, VHD (fixed + dynamic), QCOW2, VMDK, 2MG, WOZ,
  DC42, HFV, IMZ (encrypted ZIP), `.zip` (a RAW disk image inside a plain
  ZIP, inflated sparsely; `--inside` picks among multiple), GHO/GHS (Ghost
  SECTOR mode), MSA (Atari ST), EDSK/DSK (Amstrad CPC / PCW / Einstein /
  etc.), `.d88` (Sharp X68000 / PC-88 / PC-98 / MSX / FM-7), Acorn `.hdf`
  (bare + Arculator-wrapped), Apple-II `.do` / `.po` / `.dsk` sector-order,
  gzip-wrapped Amiga `.adz` / `.hdz`.
- **Raw / superfloppy** (partitionless) images are handled.
- **Optical / CD-ROM:** rip a physical CD/DVD drive to ISO or BIN/CUE
  (`optical rip`), list drives (`optical drives`), convert ISO <-> BIN/CUE <->
  CD-CHD (`optical convert`), and browse/extract ISO 9660 (+ Joliet /
  Rock Ridge), High Sierra, UDF, HFS, HFS+, SGI EFS, UFS/FFS, VMS ODS-2, and
  the video-game console discs — Nintendo GameCube / Wii (Wii decrypted
  internally, no key needed), Sega Dreamcast GD-ROM (`.gdi` + CHD), Philips
  CD-i, and 3DO — disc images, with console / serial / title / region
  identification. Built into both the desktop release and the MiSTer
  `rb-cli-mini` armv7 build (via opticaldiscs 0.8.0), for devices with an
  attached drive (e.g. the SuperStation One).
- **Remote optical ripping:** the desktop app / CLI can drive a *remote*
  daemon's optical drive over the rb-daemon — the device (e.g. a MiSTer) only
  issues SCSI reads while the desktop does all the encoding (CHD compression
  never taxes the armv7 CPU). `optical drives --remote host:port` to discover;
  `optical rip --device rb://host:port/dev/sr0`, or the GUI Optical tab's
  "Add remote daemon..." button. See `docs/remote_ripping.md`.

Legend for the **Support** column:

- **Yes** — filesystem + media path already work end to end.
- **Partial** — one media path or one filesystem is covered, others are not.
- **No** — outstanding; needs new filesystem (and sometimes container/partition) work.
- **N/A** — tape / cartridge / ROM-only core; no disk filesystem to support.

---

## Unified support table (all 64 cores)

| Core | System | Media | Filesystem(s) | Rusty Backup support |
|---|---|---|---|---|
| ao486 | 486DX33 PC | Floppy, HDD, CD | FAT12 / FAT16/32 (MBR) / HPFS (OS/2) / ISO9660 | **Yes** |
| PCXT | IBM PC/XT | Floppy, HDD | FAT12 / FAT12-16 (MBR) | **Yes** |
| MSX | MSX/MSX2/TurboR | Floppy, HDD | FAT12 / FAT16 (Nextor VHD) | **Yes** |
| MSX1 | Microsoft MSX1 | Floppy, HDD | FAT12 / FAT12-16 | **Yes** |
| ZXNext | ZX Spectrum Next | SD/HDD (VHD) | FAT32/16/12 | **Yes** |
| TSConf | ZX-Evolution | SD/HDD (VHD) | FAT32 (non-MBR) | **Yes** |
| Minimig-AGA | Commodore Amiga | Floppy, HDD, CD | OFS/FFS, PFS3, SFS (RDB) / ISO9660 | **Yes** — AFFS read + edit + `new volume affs`, and volumes we write mount Read/Write under a real Kickstart 3.1 in FS-UAE (R-020, `oracles/fsuae/affs_mount.py`); PFS3 and SFS read + edit + create (`new volume pfs3` / `sfs`, F-003), with a bare RDB-less PFS3 image now detected by superfloppy probe; RDB written by `new hd rdb` and read back clean by amitools `rdbtool` |
| MacPlus | Macintosh Plus | Floppy, HDD | HFS / MFS (400K floppy) | **Yes** — HFS + MFS both read/edit/create/fsck (400K / 800K MFS floppy) |
| AtariST | Atari ST/STe | Floppy, HDD | GEMDOS = FAT12 / FAT16 | **Yes** — floppy (`.st` / `.msa` FAT12) and HDD. The AHDI partition table reads and writes: the four primary entries plus XGM extended chains are parsed, and `rb-cli new hd atari` lays down a fresh root sector with the tags you name (GEM / BGM / RAW; a GEM partition over 16 MiB is promoted to BGM as TOS requires). Partitions route to the existing FAT12/16 driver, so browse / extract / edit / fsck / backup / restore all work. Creating an XGM chain and writing a bootable bootstrap into the root sector are the remaining gaps. |
| Apple-II | Apple IIe | Floppy, HDD | DOS 3.3 / ProDOS | **Yes** — ProDOS (read + edit + create + fsck; `rb-cli new volume prodos`) + Apple DOS 3.3 (read + edit + create + fsck; `rb-cli new floppy apple-dos`, 140 KB `.dsk`/`.do`/`.po`). Sector-order auto-detect via `containers::sector_order`. |
| ZX-Spectrum | Sinclair ZX Spectrum | Floppy, SD/HDD | TR-DOS, G+DOS, +3DOS (CP/M-like), esxDOS FAT | **Partial** — TR-DOS (`.trd`, full quartet) + esxDOS FAT yes; +3DOS via CP/M (`zxplus3` preset); G+DOS (MGT) no |
| X68000 | Sharp X68000 | Floppy, SASI/SCSI HDD | Human68k (FAT-derived dialect) | **Yes** — floppy (Human68k read/browse/extract + add/delete/mkdir on `.d88` / `.xdf` / `.hdm` / `.dim`; edits decode->mutate->re-encode back into the container, GUI and CLI) and SASI/SCSI HDD (`.hda` / `.hdf` / `.hds`) read/browse/extract/add/delete/mkdir + in-place FS grow/shrink (`rb-cli resize`) + defragmenting repack (`rb-cli repack` / Inspect-tab "Defragment…" — packs files contiguously, reclaiming holes left by deletions) + fsck (`rb-cli fsck` / Inspect-tab Check — FAT-chain reconciliation + FAT-mirror resync, lost-cluster reclaim), incl. real BlueSCSI `X68SCSI1` 1024-byte-sector images (sector size derived from the boot signature; Sharp/KG big-endian BPB + big-endian FAT). Verified byte-exact on the BlueSCSI HD10 SCSI fixture and the Populous/Lemmings/SSF2/Votoms 256-byte SASI game disks across grow→shrink round-trips (multi-cluster `COMMAND.X` + Japanese filenames survive). Backup/restore/reconstruct honor the true (non-512-aligned) partition byte offset via the persisted `start_byte`, so SASI backup→restore lands the partition region byte-identical. **New 2026-06-10:** `rb-cli new hd x68k` scaffolds self-bootable SASI/SCSI HDDs from scratch — emits the Sharp IPL signature, X68K partition table, IPL stub (halt or printed-banner via IOCS B_PRINT), and optionally clones an entire Human68k donor floppy (flat or `.dim` / `.D88` / `.xdf` / `.hdm`) into the partition. MAME-verified on `x68000 -sasi` (SASI variant, 256-B sectors) and `x68030 -hard` (SCSI variant, 1024-B sectors). The donor's `SWITCH.X /HD` installs the partition boot sector on first FDD0 boot, after which the HDD self-boots to C:. **Zero-manual-step mode** also available: `--boot-sector-donor hd0.hds --size 100M --variant scsi` extracts the Sharp partition boot sector (Sharp IPL Copyright 1990 SHARP) from the well-known `hd0.hds` 100 MB Sharp/Keisoku Giken donor at build time and overlays it onto the output partition — no `SWITCH.X` step needed, self-boots straight to C:> on first power-on. Sharp's boot-sector bytes never live in the rusty-backup repo; same legal pattern as `--system-disk` (you provide the donor file, the bytes flow user→user). |
| Archie | Acorn Archimedes | Floppy, HDD | ADFS / FileCore | **Yes** (new-map) — full quartet on the new-map layouts (E / F / HD): read + browse, edit (create / delete / rename via the FSM allocator), create blank E-format floppies (`rb-cli new floppy adfs`, 800 KB single-zone new-map FSM), and fsck (zone-checksum + cross-check verify + allocated-fragment-vs-directory-tree reconciliation; repair re-stamps the zone checksums, leaked-fragment reclaim withheld pending a full `*CheckMap`-style map rebuild). Disc Record scan spans the HD / E-format-floppy / legacy-floppy candidate offsets (0xFC0 / 0x404 / 0xDC0); byte-correct against marutan.net blank HD samples (`blank256E.hdf`, `blank1024Eplus.hdf`) and the 8bs.com `arc-04.800.adf` (E-format populated). **Old-map D-format** (no Disc Record; old free-space map + Hugo root at byte 1024) now reads and writes too — detect + read + create/delete/rename validated against real Repton 3 / Lemmings floppies (writes keep the old-map checksums valid so RISC OS still mounts the disc). `.adf` 800K floppy + bare and Arculator-wrapped `.hdf` HDD containers all handled. (Double-sided **DFS** `.dsd` is now supported via the `fs::dfs` path — see the BBCMicro row. **Still open:** double-sided ADFS L-format `.adl`.) |
| QL | Sinclair QL | Microdrive, HDD | QDOS (QXL.WIN) | **Partial** — QXL.WIN HDD read + write end-to-end (byte-correct against kilgus + smsqe MiSTer samples; write path validated against headless sQLux oracle — rb-cli put → SuperBASIC COPY → host file round-trips byte-exact, per-file 64-byte QDOS header convention honoured); `.mdv` microdrive detect + cart-name surfaced (full directory walk parked at OPEN-WORK §7 behind real-hardware oracle) |
| Amstrad | Amstrad CPC 6128 | Floppy | AMSDOS, CP/M 2.2/Plus | **Yes** — `fs::cpm` ships the `amstrad_data` + `amstrad_sys` DPBs covering both CPC data + system formats. |
| AmstradPCW | Amstrad PCW | Floppy | CP/M Plus | **Yes** — `fs::cpm` `amstrad_pcw` DPB. |
| TatungEinstein | Tatung Einstein | Floppy | Xtal/DOS (CP/M-compatible) | **Yes** — `fs::cpm` `einstein` DPB. |
| SVI328 | Spectravideo SV-328 | Floppy | CP/M (MSX-DOS/FAT12 possible) | **Partial** — CP/M side covered by `svi328_cpm` DPB; the MSX-DOS / FAT12 floppy variant routes through the existing FAT driver but no SV-specific BPB quirks have been validated. |
| Altair8800 | MITS Altair 8800 | Floppy, IDE/CF | CP/M | **Yes** — `fs::cpm` ships both `altair_8in` (8-inch floppy) and `altair_cf` (CompactFlash HDD) DPBs. |
| MultiComp | Grant Searle MultiComp | Floppy | CP/M | **Yes** — `fs::cpm` `multicomp` DPB. |
| C64 | Commodore 64/128 | Floppy | CBM DOS (flat track/sector) | **Yes** — `fs::cbm` reads + writes `.d64` (1541) / `.d71` (1571) / `.d81` (1581); add/delete cross-validated against `c1541` / Python `d64`. |
| C128 | Commodore 128 | Floppy | CBM DOS | **Yes** — `fs::cbm` `.d64` / `.d71` / `.d81`. |
| C16 | Commodore C16/Plus4 | Floppy | CBM DOS | **Yes** — `fs::cbm` `.d64` (1541). |
| VIC20 | Commodore VIC-20 | Floppy | CBM DOS | **Yes** — `fs::cbm` `.d64` (1541). |
| PET2001 | Commodore PET | Floppy (IEEE-488) | CBM DOS (D80/D82) | **Yes** — `fs::cbm` reads + writes the 8050 `.d80` (533248 B, 77 trk) and 8250 `.d82` (1066496 B, 154 trk) geometries: 29/27/25/23 zone map, BAM on track 38 (2–4 chained sectors, 5-byte/track entries), directory on track 39. Bidirectionally cross-validated against the Python `d64` reference (read + write byte-exact). `.d64` (1541/2031/4040) also works. |
| BBCMicro | BBC Micro B/Master | Floppy | Acorn DFS / ADFS | **Yes** — `fs::dfs` reads + writes Acorn DFS single-sided `.ssd` (40-track 100K / 80-track 200K) **and** double-sided `.dsd` (the two track-interleaved sides de-interleave into two Acorn DFS partitions `@1` / `@2`, edits re-interleaving on save): flat catalogue in sectors 0–1, up to 31 contiguous files in descending start-sector order, single-character directory namespaces, 18-bit load/exec/length, lock bits. Read/browse/extract + add/delete bidirectionally cross-validated byte-exact against an independent clean-room DFS reader/writer (locked files, non-`$` dirs, real load/exec all round-trip); the `.dsd` de-interleave + `@1`/`@2` split validated against real BBC PD music discs. ADFS floppies (`.adf`) are handled by `fs::adfs` (see the Archie row). |
| AcornElectron | Acorn Electron | Floppy | DFS / ADFS | **Yes** — same `fs::dfs` Acorn DFS read + write as BBCMicro (single-sided `.ssd` + double-sided `.dsd`, the latter presented as two partitions). ADFS floppies handled by `fs::adfs`. |
| AcornAtom | Acorn Atom | Tape, Floppy | Atom DOS | **No** |
| CoCo2 | Tandy CoCo 2 | Floppy | RS-DOS / DragonDOS, OS-9 (RBF) | **Yes** — `fs::rsdos` reads + writes RS-DOS / Disk BASIC (granule allocation table on track 17, 72-file directory, granule-chain files; raw 35- / 40-track `.dsk` / `.jvc`), `fs::os9` reads + writes OS-9 / NitrOS-9 RBF (hierarchical FS; `.dsk` / `.vdk`, byte-exact cross-validated against a clean-room RBF reader on real toolshed disks), and `fs::dragondos` reads + writes DragonDOS (see Dragon row). |
| Dragon | Dragon 32/64 | Floppy | DragonDOS, OS-9 (RBF) | **Yes** — `fs::dragondos` reads + writes DragonDOS (directory track 20 + backup track 16, one's-complement geometry signature, set-bit-free sector bitmap, 25-byte header/continuation directory entries; single- / double-sided 40-track `.dsk`). Byte-exact cross-validated against an independent clean-room reader/writer AND against real third-party DragonDOS disks (rolfmichelsen/dragontools' empty volume plus a populated 9-file AGD-suite disk, all files byte-identical across both readers). `fs::os9` covers OS-9 / NitrOS-9 RBF. |
| CoCo3 | Tandy CoCo 3 | Floppy/virtual | RS-DOS, OS-9 / NitrOS-9 (RBF) | **Yes** — `fs::rsdos` RS-DOS / Disk BASIC read + write (see CoCo2) and `fs::os9` OS-9 / NitrOS-9 RBF read + write (add/delete incl. subdirectories), the two filesystems the CoCo3 core uses. |
| TRS-80 | Tandy TRS-80 | Floppy (JV1) | TRSDOS / LDOS / NEWDOS | **No** |
| Atari800 | Atari 8-bit | Floppy, ltd HDD | Atari DOS (DOS 2.x) | **Yes** — `fs::atari_dos` reads + writes + creates + fscks Atari DOS 2.0S/2.5 (VTOC@360 bit-set-free bitmap, 64-file directory @361-368, linked-sector files). Single + enhanced density `.atr` / `.xfd`; `rb-cli new floppy atari` formats a blank SD disk, and fsck reconciles the VTOC bitmap + free-count against the directory chains. Read validated byte-exact against a real DOS 2.0S system disk + an independent clean-room reader; write validated the same way; fsck validated clean on the real fixture. |
| TI-99_4A | TI-99/4A | Floppy | TI floppy FS (VIB/FDIR) | **Yes** — full quartet on the V9T9 `.dsk` (read + edit + create + fsck) |
| Oric | Tangerine Oric | Floppy | Jasmin (read+edit+create+fsck); Sedoric pending | **Partial** |
| SharpMZ | Sharp MZ | Floppy, Tape | Sharp MZ FD format | **No** |
| PC88 | NEC PC-8801 mkII SR | Floppy | N88-BASIC Disk BASIC | **No** |
| SAM-Coupe | SAM Coupe | Floppy | SAM DOS / MasterDOS | **No** |
| ColecoAdam | Coleco Adam | Floppy, DDP tape | EOS (read-only in core) | **No** |
| BK0011M | Elektronika BK | Floppy, HDD (VHD) | ANDOS / CSIDOS | **Partial** — `fs::andos` ships a detect-only scaffold (boot-block signature probe surfaces "ANDOS" through `fs_type()`). Browse / extract returns `Unsupported`; CSIDOS not started. Sparse public docs limit the work to what real-disc fixtures can validate. |
| Vector-06C | Vector-06C | Floppy | MicroDOS | **No** |
| Specialist | Specialist | Floppy | Specialist-MX FS | **No** |
| ZX81 | Sinclair ZX80/ZX81 | Tape | — | **N/A** |
| Apple-I | Apple I | ROM/program | — | **N/A** |
| Aquarius | Mattel Aquarius | Cart, Tape | — | **N/A** |
| Apogee | Apogee BK-01 | Tape/ROM | — | **N/A** |
| AliceMC10 | Matra Alice | Tape | — | **N/A** |
| Casio_PV-2000 | Casio PV-2000 | Cartridge | — | **N/A** |
| Chip8 | CHIP-8 | Program/ROM | — | **N/A** |
| EDSAC | EDSAC | Paper tape | — | **N/A** |
| EG2000 | EACA Colour Genie | Tape, Cart | — | **N/A** |
| Galaksija | Galaksija | Tape | — | **N/A** |
| Homelab | Compukit Homelab | Tape | — | **N/A** |
| Interact | Interact | Tape | — | **N/A** |
| Jupiter | Jupiter Ace | Tape, Cart | — | **N/A** |
| Laser310 | Vtech Laser 310 | Tape | — | **N/A** |
| Lynx48 | Camputers Lynx | Tape | — | **N/A** |
| OndraSPO186 | Tesla Ondra SPO-186 | Tape | — | **N/A** |
| Orao | PEL Varazdin Orao | Tape | — | **N/A** |
| PDP1 | DEC PDP-1 | Paper tape | — | **N/A** |
| PMD85 | Tesla PMD 85 | ROM, Tape | — | **N/A** |
| RX-78 | Bandai RX-78 | Cartridge | — | **N/A** |
| SordM5 | Sord M5 | Cart, Tape | — | **N/A** |
| TomyTutor | Tomy Tutor | Cart, Tape | — | **N/A** |
| UK101 | Compukit UK101 | Tape/program | — | **N/A** |

**Tally (recounted 2026-08-17):** 29 fully supported, 5 partial, 8 outstanding (No), 23 N/A (tape/cart/ROM-only).

---

## Outstanding work, grouped by filesystem

Estimates assume one developer fluent in the codebase, and cover
**detect + browse/inspect + backup compaction**. Edit / fsck / resize is an
additional increment, only worthwhile on HDD-capable cores. Effort:
**S** ~= days, **M** ~= 1-3 weeks, **L** ~= 3-6 weeks.

Rusty Backup's differentiators (resize, compaction, CHD/VHD/zstd compression)
only matter for **hard-disk-capable** cores. Most outstanding cores are
floppy-only with tiny fixed-geometry disks (140 KB-800 KB) where there is
nothing to resize or compact — the work there is browse / inspect / verify /
convert only.

> **All of the work planned in the two tables below has shipped.** CP/M
> (multi-DPB), Human68k, ADFS/FileCore, QDOS, CBM DOS, Acorn DFS, RS-DOS,
> DragonDOS, OS-9 RBF, Atari DOS and TI-99 are all graded **Yes** in the matrix
> above and documented in README § Filesystems. The container-format sub-task
> below ( `.d88` / EDSK / `.g64` / `.msa` / `.dim` / `.vdk` ) has shipped too.
>
> The tables are kept as the planning record — they show what was scoped and
> what it was estimated at — and are **not** a to-do list. Re-read the matrix
> above for current status.

### Highest value — hard-disk-capable (resize/compaction pays off)

| Filesystem | Cores unlocked | Effort | Notes / extra support needed |
|---|---|---|---|
| **CP/M (parameterized)** | Amstrad, AmstradPCW, TatungEinstein, SVI328, Altair8800, MultiComp (+ ZX +3DOS) | M-L | One implementation with a per-machine diskdef/DPB registry (like cpmtools) covers ~6 cores. Biggest core-count payoff. Needs **EDSK container decoder**. |
| **Human68k** | X68000 | M | FAT-derived — can reuse FAT scaffolding, but needs 18.3 names, Shift-JIS, case-sensitivity, X68k partition scheme. Needs **.d88/.dim container decoder**. |
| **ADFS / FileCore** | Archie (+ BBC/Electron ADFS) | M-L | Acorn-proprietary 256-byte-block FS with free-space map. Strip Arculator <=0.99 512-byte header. |
| **QDOS** | QL | M | QXL.WIN hard-disk container; resize valuable. Microdrive .mdv is fixed-size. |

### Medium value — mostly floppy-only (browse/inspect/convert, no resize)

| Filesystem | Cores unlocked | Effort | Notes / extra support needed |
|---|---|---|---|
| **CBM DOS** | C64, C128, C16, VIC20, PET2001 | M | Flat single-directory track/sector. Raw .d64/.d71/.d81/.d80/.d82 easy; needs **GCR decoder for .g64/.g71** (+S-M). No resize concept. |
| **Acorn DFS** | BBCMicro, AcornElectron | S | Trivial flat catalogue in track 0. (ADFS shares FileCore work above.) |
| **RS-DOS/DragonDOS + OS-9 RBF** | CoCo2, CoCo3 | M each | RS-DOS granule table (a "FAT" in name only). OS-9 RBF is a separate Unix-like FS. Needs **.jvc/.vdk handling**. |
| **Atari DOS** | Atari800 | S-M | DOS 2.x family on .atr. |
| **TI-99 FS** | TI-99_4A | M | VIB + FDIR sector scheme on .dsk. |
| **TRS-80 DOS** | TRS-80 | M | TRSDOS/LDOS/NEWDOS variants on JV1 sector dumps. |
| **Sedoric** (Oric DOS) | Oric | S-M | Jasmin DONE (`src/fs/oric.rs`); Sedoric still pending. |
| **N88-BASIC** | PC88 | M | Needs **.d88 container** (shared with X68000). |
| **Sharp MZ FD** | SharpMZ | S-M | Per-machine floppy format. |
| **EOS** | ColecoAdam | S-M | Block-based, read-only in the core -> read/browse only. |
| **Atom DOS** | AcornAtom | S-M | Niche; mostly a tape machine. |
| **SAM DOS / MasterDOS** | SAM-Coupe | S-M | MGT 800K layout (shares family with Spectrum G+DOS). |

### Low value — niche Soviet / hobby cores

| Filesystem | Cores | Effort | Notes |
|---|---|---|---|
| **ANDOS / CSIDOS** | BK0011M (has .vhd HDD) | M | Only outstanding Soviet core with HDD. |
| **MicroDOS** | Vector-06C | M | Needs **.fdd/.edd container**. |
| **Specialist-MX FS** | Specialist | M | Needs **.odi container**. |

### Near-complete touch-ups (small wins)

| Item | Core(s) | Effort | Notes |
|---|---|---|---|
| **Apple DOS 3.3** | Apple-II | S-M | Complements existing ProDOS; covers sector-order .dsk/.do. |

---

## CD-ROM summary

CD-ROM support is effectively **done**. Only **ao486** (ISO9660) and **Amiga
CD32** (data CDFS = ISO9660; audio not implemented in the core) use CD media.
No other computer core uses CD-ROM, so no new optical work is required.

(The optical browser reads far more than the cores need — UDF, High Sierra,
HFS / HFS+, SGI EFS, UFS/FFS, VMS ODS-2, and the video-game console
filesystems GameCube / Wii, CD-i, and 3DO Opera in addition to ISO 9660 — so
no CD-using core is filesystem-blocked.)

---

## Recommended sequencing (best coverage per unit effort)

1. **CP/M (parameterized + EDSK decoder)** — unlocks ~6 cores including two
   HDD-relevant ones. Biggest payoff.
2. **CBM DOS (+ GCR)** — unlocks 5 Commodore cores.
3. **Near-complete touch-ups** — Apple DOS 3.3. (AtariST AHDI is done: read,
   write, and `rb-cli new hd atari`.)
4. **Human68k, ADFS/FileCore, QDOS** — the three outstanding HDD filesystems
   where resize/compaction actually pays off.
5. Everything else is floppy-only long-tail: implement on demand;
   browse/inspect-only is usually enough.

A recurring sub-task: several formats are **container formats** (`.d88`, EDSK,
`.g64/.g71`, `.msa`, `.dim`, `.vdk/.jvc`, `.fdd/.edd`, `.odi`, `.nib`) that wrap
sectors with per-track geometry. Build a shared "decode container -> flat LBA
sectors" layer feeding the superfloppy path, reused across cores — budget that
as one **M** infrastructure piece rather than re-solving it per filesystem.

---

## Prior art: TotalImage and existing Rust crates

Survey done 2026-05-31 to answer "can we reuse existing code instead of
writing every outstanding filesystem from scratch?"

### TotalImage (`../TotalImage`)

- **What it is:** an MIT-licensed disk-image editor written in **C# / .NET 10 /
  WinForms**. MIT is compatible with our AGPL-3.0 (port with attribution), but
  it is a **different language** — nothing links or imports; every reused line
  must be **manually ported to Rust**.
- **Filesystem overlap:** it implements FAT12/16/32, exFAT, NTFS, ISO9660, UDF,
  and IMGFS (Windows CE). We already have everything except UDF and IMGFS,
  neither of which is in MiSTer scope. **It contains none of the outstanding
  MiSTer filesystems** (no CP/M, CBM DOS, ADFS, Human68k, QDOS, OS-9, etc.) —
  it is a PC/DOS/Windows-centric tool.
- **The one genuinely portable gem:** `FileSystems/FAT/FatFactory.cs` does
  robust **BPB-less FAT detection** — infers geometry from image size + media
  descriptor + dual-FAT validation (4 disambiguation rounds), tries the BPB at
  alternate offsets (0x04 Zenith Z-100, 0x50 Apricot), and special-cases
  Acorn-DOS-800K and Victor 9000. Paired with the geometry table in
  `DiskGeometries/FloppyGeometry.cs`. Our `src/fs/fat.rs::open` currently
  **requires a BPB at the standard offset with no fallback**, so this is a real
  robustness gap-filler — but it only helps cores we **already support** (raw
  Atari ST `.st`, MSX `.dsk`, odd PC floppies). It does nothing for the
  outstanding non-FAT cores.
- **IMZ note:** `Containers/ImzContainer.cs` confirms an *unencrypted* WinImage
  IMZ is just a ZIP with a single entry + zip comment. It does **not** touch the
  encrypted IMZ (MD5 + Rijndael) tracked in `imz_encryption.md`.
- **Verdict:** not a "use most of their code" opportunity. Treat as a narrow
  *reference*: port the BPB-less FAT detection + geometry table (with MIT
  attribution) if/when we harden raw-floppy FAT handling. Everything else
  either overlaps what we have or is out of scope.

### Pure-Rust crates (the bigger lever)

All three are MIT / MIT-OR-Apache, so they can be **vendored as dependencies**
or ported with attribution under our AGPL.

| Crate | License | Covers | MiSTer cores unlocked | Maturity |
|---|---|---|---|---|
| **a2kit** (`dfgordon/a2kit`) | MIT | CP/M 1/2/3, Apple DOS 3.3, ProDOS, Pascal FS, FAT; containers 2MG/DSK/DO/PO/IMD/IMG/NIB/TD0/WOZ | Apple-II DOS 3.3 (our gap) + **CP/M family: Amstrad, AmstradPCW, TatungEinstein, SVI328, Altair8800, MultiComp, ZX +3DOS** | Active, v4.4.2 |
| **cbm** (`simmons/cbm`) | MIT/Apache | D64/D71/D81 read/write/format/rename | **C64, C128, C16, VIC20** (not PET D80/D82) | Complete but dormant |
| **fluxfox** (`dbalsom/fluxfox`) | MIT | Container/track layer: TD0, IMD, HFE, 86F, IPF, SCP, raw IMG/DSK/ADF/ST + Amiga/Mac/Atari ST/Apple GCR | The shared **container-decode infra layer** feeding the superfloppy path | Active, ~150 stars |

**Caveats to verify before depending on them:**

- **a2kit CP/M** may be Apple-CP/M-centric — confirm it ingests arbitrary
  per-machine **disk parameter blocks** (Amstrad/PCW/Einstein geometries)
  before counting it for the CP/M cores.
- **fluxfox `.d88`** support is ambiguous (crate docs omit it; a secondary
  source claimed it) — matters for PC88 / X68000 floppies.

### Per-filesystem crate availability matrix

| Outstanding filesystem | Cores | Rust crate? | Best path |
|---|---|---|---|
| CP/M | Amstrad, PCW, Einstein, SVI328, Altair, MultiComp, ZX+3 | **a2kit** (verify DPBs) | Vendor/port a2kit |
| CBM DOS | C64, C128, C16, VIC20 | **cbm** | Vendor cbm |
| CBM DOS (D80/D82) | PET2001 | No | Extend cbm or ground-up |
| Apple DOS 3.3 | Apple-II | **a2kit** | Vendor/port a2kit |
| Container decode (TD0/IMD/ADF/ST/MSA) | many | **fluxfox** | Vendor fluxfox |
| Container `.d88` | PC88, X68000 | fluxfox (uncertain) | Verify, else custom |
| Atari DOS (`.atr`) | Atari800 | No (`atrfs` is Go/Python) | Ground-up; reference atrfs spec |
| Human68k | X68000 | No (`dis68k`/`fathuman` are C/Go) | FAT-derived; our FAT + custom dir layer |
| ADFS / FileCore | Archie, BBC/Electron ADFS | No | Ground-up; ref OpenAcornExplorer, Linux adfs |
| QDOS | QL | No | Ground-up |
| OS-9 RBF / RS-DOS / DragonDOS | CoCo2, CoCo3 | No | Ground-up |
| TRS-80 (TRSDOS/LDOS) | TRS-80 | No | Ground-up |
| Oric Jasmin | Oric | Yes | Done (`src/fs/oric.rs`) |
| Sedoric (Oric DOS) | Oric | No | Ground-up |
| N88-BASIC | PC88 | No | Ground-up |
| Sharp MZ FD | SharpMZ | No | Ground-up |
| EOS | ColecoAdam | No | Ground-up (read-only) |
| TI-99 FS | TI-99_4A | No | Ground-up |
| ANDOS/CSIDOS, MicroDOS, Specialist-MX | BK0011M, Vector-06C, Specialist | No | Ground-up (niche) |

### Revised takeaway

The crates change the build-vs-reuse math for the **highest-payoff** targets:
**a2kit** can cover the entire CP/M family plus our Apple DOS 3.3 gap, **cbm**
covers four Commodore cores outright, and **fluxfox** supplies the
container-decode infrastructure piece budgeted above. The long tail (ADFS,
Human68k, QDOS, OS-9, TRS-80, and the niche/Soviet filesystems) still has no
Rust prior art and remains ground-up work. TotalImage is a minor reference for
hardening FAT floppy detection, not a reuse source for the outstanding cores.
