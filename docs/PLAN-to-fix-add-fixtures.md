# Plan: support the `to-fix-add` fixture drop + open issues #89 / #90 / #91

Status: **agreed scope, nothing implemented yet** (2026-10-27).

Decisions (2026-10-27):
- Everything ships **read-only first**, but writes are wanted later. Structure each
  driver so edit support can follow, the way PFS3 / SFS / JFS2 grew theirs.
- Every LVM is **scoped to its own OS platform** (`HP-UX LVM`, `AIX LVM`, later
  `Linux LVM2`), each in its own module, **single-PV only**.
- AIX JFS2 is **shelved** until an AIX 5L+ fixture exists. Anything stubbed along
  the way is recorded in `docs/stubs.md`.
- Sun QIC tapes are **in scope**, browsable as a filesystem in Inspect and Commander.
- `hp715-11.11v1.img` is the HP-UX 11.x fixture for #90.
- #91's "CD support" means: every filesystem on every ISO in the drop is readable.

This covers every image in the corpus's `to-fix-add/` drop and the three open
GitHub issues. Fixtures are named by their path under the corpus root, never by
a host path (see `regression-tests/FIXTURES.md`).

## 1. What each fixture is, and what works today

Probed with the current `rb-cli` (`inspect` / `ls` / `optical browse`).

| Fixture | What it is | Today |
|---|---|---|
| `HP385-HPUXOS.img` (640 MB) | HP 9000/385 (68030), HP-UX, whole-disk HP-HFS with the classic FFS magic | **Works** (browses as UFS) |
| `HP385-NetBSD92-release-test.img` | NetBSD/hp300: LIF boot header at sector 0, big-endian BSD disklabel at byte 1024, FFS | **Fails**: no partition table recognised |
| `AIX433_4GB.img` | AIX 4.3.3 (PowerPC): IBM IPL record (`IBMA`), PReP type-0x41 boot partition, AIX LVM (`_LVM` at sector 7), JFS1 inside LVs | **Fails**: shows two "Unknown" MBR entries |
| `Sun-2/sd0-1g.img`, `Sun-3/sd0-1g.img` | Sun label + SunOS 4.0.3 / 4.1.1 UFS | **Works** (all slices browse) |
| `Sun-2/*.qic`, `Sun-3/*.qic` | `SUN2TAPE` tape images (format from Sun-3_MiSTer `tools/mktape`): boot code, TOC, a.out kernels, UFS miniroots, V7 tar, `compress(1)` `.Z` tar | **Not supported** (no tape container) |
| `ShapeShifter-*/SS-5G.hdf` | Minimig RDB, two PFS\3 partitions; the Mac drives are 2 GB hardfiles `ShapeShifter/HardFiles/CH1.dsk`, `CH2.dsk` | PFS **works**; the nested Mac hardfiles can't be opened from Inspect (#89) |
| `ShapeShifter-*/Mac-4G.hdf` | Minimig RDB whose two partitions are declared `PFS\3` but actually hold classic **HFS** (boot `LK`, MDB `BD`) used raw by ShapeShifter | **Fails**: "not a PFS volume: boot magic = 4c4b6000" (#89 screenshot 3) |
| `cp-2088.7z` -> `cp-2088.vhd` (81 MB) | Atari Falcon disk: AHDI table, one `BGM` partition. Boot sector starts `60 1C` (68000 `BRA.S`); BPB is valid: 2048-B logical sectors, 2 FATs, 41472 sectors | **Fails**: "invalid FAT boot sector: bad jump instruction" (#89 screenshot 1) |
| `CDs/AIX_433_PPC_BONUSCD_CD1.ISO` | AIX bonus pack, ISO 9660, `usr/sys/inst.images/*.bff` | ISO **works** |
| `CDs/LCD4_0286_07_VOLUME1.ISO` | AIX CHRP diagnostics CD (ISO 9660 + MBR/PReP boot) | ISO **works**; `inspect` shows "Unknown" MBR entries |
| `CDs/HP-UX 11.11 ... DVD.iso` (4.7 GB) | HP-UX 11.11 TCOE install DVD, ISO 9660 + SD depots | ISO **works** |
| `CDs/NetBSD-11.0-hppa.iso` | ISO 9660 (+ LIF boot) | **Works** |
| `CDs/OS2-Warp-PowerPC.iso` | ISO 9660 + PReP boot | **Works** |
| `CDs/sms107.img` | RS/6000 SMS diskette, FAT12 | **Works** |
| `CDs/rs6karc151.img` | RS/6000 1.44 MB diskette: PReP type-0x41 boot image, no filesystem | Detected; partition shows as "Unknown" |

Related, outside the drop: the 8 GB HP-UX 11.11 disk next to the corpus
(`hp715-11.11v1.img`, from the hp9000-715 project) is the only **HP-UX 11.x**
disk on hand. It's needed for #90: LIF boot area, HP-UX LVM (`LVMREC01` at
sector 16, mirror at 144), VxFS **version 4** superblocks, and `/stand` as HP-HFS
with the long-filename magic `0x095014`.

## 2. The open issues

- **#89 "Falcon"** is three separate bugs; see the screenshots:
  1. Atari BGM partition rejected by the FAT parser: TOS boot sectors don't start with `EB`/`E9`.
  2. RDB partition type lies (`PFS\3` over raw HFS).
  3. ShapeShifter hardfiles inside PFS can't be opened from Inspect.
- **#90 "HP-UX hard disks"** asks for VxFS + HP HFS. Getting there needs LIF and HP-UX LVM too.
- **#91 "JFS and JFS2 for AIX"** asks for JFS + JFS2, "also CD support". Getting there needs AIX LVM too.
  - AIX JFS2 is the same layout as the Linux JFS2 we already ship, but **big-endian**.
  - AIX JFS1 (AIX 3/4, the fixture we have) is a different on-disk format.
  - The AIX CDs already browse at the ISO level, so "CD support" needs clarifying (see §6).

## 3. LVM scope (single-disk volume groups only)

Two LVM formats are needed, and **neither is Linux LVM2**:

| | HP-UX LVM | AIX LVM |
|---|---|---|
| Needed for | #90 (HP-UX 10.x/11.x root disks) | #91 (every AIX disk) |
| Anchor | `LVMREC01` at byte 8192 (mirror at 72 KiB) | IPL record `IBMA` at sector 0, `_LVM` record at sector 7 (mirror at 70) |
| Metadata | VGRA: VGDA (LV table, PV table, PE map) + VGSA | VGDA (LV entries, PP map) + VGSA |
| Unit | Physical extent (default 4 MiB) | Physical partition (PP; 4 MiB on this disk era) |
| Public reference | None known: reverse it from the 11.11 disk, using the HFS/VxFS superblock offsets we already found as ground truth | Linux `block/partitions/aix.c` (read-only LV map) |

Each LVM is its own platform module (`partition/lvm/hpux.rs`, `partition/lvm/aix.rs`,
later `partition/lvm/linux2.rs`). They share only a small extent-map type: an
LV as an ordered list of (PV byte offset, length) runs behind a `Read + Seek`
reader. Nothing else is shared, so each OS's quirks stay in its own file. The
UI names them by platform: "HP-UX LVM", "AIX LVM".

Behaviour for both:

- Present each logical volume as a partition row (`lvol1`..`lvolN` / `hd4`, `hd2`, ...).
- **Single-PV only.** If the VGDA lists more than one PV, show the LVs but refuse
  to open any LV that has extents on a missing PV, with a clear message.
- An LV whose extents are non-contiguous is read through an extent-mapping reader,
  not assumed contiguous.
- Backup is the whole disk verbatim (metadata included). **No LV resize, LV create
  or restore-time re-layout** in this phase.
- Linux LVM2 is planned as a later third module on the same model; no fixture needs it yet.
- Writes come later. Keep the parsed VGDA round-trippable (no lossy parsing) so
  in-place edits inside an LV, and later LV resize, have something to build on.

## 4. Work items

Effort: **S** = about a day, **M** = a few days, **L** = about a week, **XL** = more than that, with research risk.

### Phase 1: #89 fixes (Falcon + ShapeShifter) — **done 2026-10-27**

- 1.1 `fat::has_fat_boot_start`: any boot start is accepted over a strict BPB, with
  logical sectors up to 8192 B on the read paths. Content detection in
  `fs::detect_filesystem_type` follows suit.
- 1.2 `RdbPartition::content_type`: sniffed at parse time; partitions and the
  restore plan route by it, while the declared DosType is kept for the RDB rewrite.
- 1.3 Inspect "Open as Disk Image", plus Commander's nested disk-image /
  optical descend now streaming to a temp file instead of RAM.
- Fixtures catalogued (`fmt.ahdi.falcon-bgm.hd`, `part.rdb.shapeshifter-raw-hfs.hd`,
  `part.rdb.shapeshifter-hardfiles.hd`), with cases in
  `regression-tests/cases/tier2/read-issue89-fixtures.toml`.
- Found along the way: classic HFS fsck reports `FreeNodeNotErased` on volumes a
  real Mac wrote. Fixed as Phase 1c, below.

### Phase 1c: classic HFS "unused node not erased" — **done 2026-10-27**

The System 7.5.5 volume on `part.rdb.shapeshifter-raw-hfs.hd` got 696
`ERROR [FreeNodeNotErased]` lines and exit 1 from `fsck`, on a volume a working
Mac used daily.

What Apple's `fsck_hfs` actually does (`lib_fsck_hfs`, `BTCheckUnusedNodes` called
from `CheckCatalogBTree`): it checks the **catalog** tree on HFS *and* HFS+, reports
`E_UnusedNodeNotZeroed` once as a level-1 minor repair, stops at the first one, and
its repair zeroes them all. System 6-8 and Disk First Aid never erased freed nodes
or checked for it, so nearly every classic volume trips it. The check itself wasn't
wrong; its weight was. It was one error per node, and there was no repair for it.

Changes (classic HFS only; HFS+ is untouched, since modern macOS writes HFS+ and
erases nodes):

- One `FreeNodeNotErased` **warning** per tree (catalog, extents), with the count
  and first node. The volume checks clean (exit 0) and is marked repairable.
- `repair_hfs` zeroes unused nodes. When the check found no errors, that is
  *all* it does: no catalog index rebuild on a healthy volume. On the fixture, the
  repair changes exactly the 696 node sectors, plus the MDB's modify date and the
  alternate-MDB refresh every sync does.
- `rb-cli fsck --repair` acts on a repairable warning, matching the GUI's Repair
  button. Minix and UCSD already marked warnings repairable and were blocked by
  the same gate.


| # | Item | Effort |
|---|---|---|
| 1.1 | FAT: accept a non-x86 boot jump (Atari `0x60xx` 68k BRA, or zeros) when the BPB is otherwise sane. This fixture's 2048-B logical sectors are already under the 4096 cap; raise the cap to 8192 for larger BGM partitions. Fixture: `cp-2088.vhd` | S |
| 1.2 | RDB: when the declared DosType's magic check fails, sniff the content (HFS `LK` + `BD`@1024, FAT, ...) and open that instead; label it e.g. `HFS (ShapeShifter, declared PFS\3)`. Fixture: `Mac-4G.hdf` | S |
| 1.3 | Inspect browse view: "Open as disk image" on a file row, reusing `model::commander_descend` (already does this for Commander panes). Stream through the parent filesystem instead of materialising 2 GB to a temp file, since root disk space is tight. Check `.dsk` routes by content, not as an Apple II DSK. Fixture: `SS-5G.hdf` -> `CH1.dsk` | M |

### Phase 1b: every filesystem on every ISO (#91) — **done 2026-10-28**

- `fs::lif`: an HP LIF reader (volume header and directory, BCD dates, aliased
  extents), plus a read-only `LifFilesystem` that Phase 2 reuses for disks.
- `optical::boot_area`: on a plain `.iso`, the LIF volume and any MBR type-`0x41`
  PReP partition are detected and opened as `opticaldiscs` filesystems. They are
  offered after the hybrid volumes in the Optical tab's picker, Commander
  (`optical_filesystems` / `open_optical_hybrid`, so the TUI gets them too) and
  `rb-cli optical browse|extract|du --filesystem lif|prep`; `optical info` lists them.
- MBR type names: `0x41` PReP boot, `0x96` CHRP ISO-9660 (fixes the "Unknown"
  rows on the AIX diagnostics CD and `rs6karc151.img`).
- Fixed on the way: the Optical tab's Extract always reopened the *primary*
  filesystem, so extracting from the HFS side of a hybrid disc was broken. It now
  reopens the picked one.
- Verified by byte-comparing one file per disc against `7z`, and LIF `ISL` against
  a raw read of its extent. Five ISOs and two RS/6000 floppies catalogued, with
  cases in `regression-tests/cases/tier2/read-to-fix-add-isos.toml`.
- Still to come from 2.1 / 3.1: LIF and the IBM IPL record on *disks*, in Phases 2 and 3.


ISO 9660 + Rock Ridge already browse on all five ISOs. What each disc also carries:

| ISO | Extra areas |
|---|---|
| AIX bonus CD | Rock Ridge |
| AIX CHRP diagnostics (LCD4) | IBM IPL record + PReP MBR, Rock Ridge |
| HP-UX 11.11 DVD | LIF boot area at sector 0, Rock Ridge |
| NetBSD 11.0 hppa | LIF boot area at sector 0, Rock Ridge |
| OS/2 Warp PPC | PReP MBR, plain ISO 9660 |

| # | Item | Effort |
|---|---|---|
| 1b.1 | Optical view: surface the LIF boot area (item 2.1's reader) and the PReP/IPL boot records next to the ISO tree | S–M |
| 1b.2 | End-to-end check of each ISO: Rock Ridge names, symlinks and permissions, extract a sample file per disc, and a regression case for each | S |

### Phase 2: HP 9000 (LIF, BSD label, HP-HFS, HP-UX LVM, VxFS)

**2a done 2026-10-28 (items 2.1-2.3 on disks):**
- `PartitionTable::Bsd` (`partition/bsd.rs`): finds the label at 1024 / 512 / 64 / 128
  in either byte order, checks the XOR checksum, hides the raw whole-disk slot, and
  lists a LIF boot volume ahead of it as a *LIF boot area* row (type string `LIF`,
  opened by `fs::lif::LifFilesystem`). Whole-disk CHD backup works; editing and
  resize-on-restore are recorded in `docs/stubs.md`.
- HP-UX HFS: `fs::ufs` accepts big-endian `0x095014` / `0x195612` at the UFS1
  superblock (`ufs::is_ufs_magic`, which also feeds both content detectors), and fsck
  checks backup superblocks against the magic the volume actually has. Editable opens
  are refused for now (`open_ufs_editable`). The 11.11 `/stand`, carved by hand,
  lists (`vmunix`, `bootconf`) and fscks clean.
- Fixtures: `part.bsd.netbsd92-hp300.hd`, `fs.hpux-hfs.hp385-hpux.hd`;
  cases in `cases/tier2/read-hp9000-disks.toml`.

**2b (item 2.4) done 2026-10-28: HP-UX LVM.** `partition/lvm/mod.rs` holds the
shared `LogicalVolume` (byte runs in logical order, marked complete when every
extent is on this disk); `partition/lvm/hpux.rs` parses `LVMREC01` at 8 KiB,
takes the newer of the two VGDA copies, and maps each `lvolN` from this PV's
extent map. The on-disk layout is documented in that module's header; it was
worked out from the hp715 disk and checked against its `/stand` and VxFS
superblocks. `PartitionTable::HpuxLvm` lists the LIF boot area as row 0, then
the LVs. An LV that is incomplete (multi-disk group) or fragmented lists but
refuses to open with the reason (`LVM_UNAVAILABLE`, see `docs/stubs.md`). The hp715
disk is catalogued (`part.hpux-lvm.hpux1111-hp715.hd`), case
`cases/tier2/read-hpux-lvm.toml`.


| # | Item | Effort |
|---|---|---|
| 2.1 | LIF volume header + directory: recognise it, and expose the LIF files (ISL, HPUX, BOOT43, ...) as a read-only listing | S–M |
| 2.2 | BSD disklabel (big- and little-endian; sector 0/1, and at byte 1024 behind a LIF header). Read + backup. Fixture: `HP385-NetBSD92-release-test.img` | M |
| 2.3 | UFS: accept HP-UX HFS magics `0x095014` (long filenames) and `0x195612`; check HP inode/dirent differences against `/stand` on the 11.11 disk | S–M |
| 2.4 | HP-UX LVM, single-PV (§3). Fixture: `hp715-11.11v1.img` | L |
| 2.5 | VxFS read-only (browse/extract/fsck-lite), versions 2–4 first; v5/v6 (HP-UX 11.23/11.31) later. Reference: Linux `freevxfs` (read-only, handles HP-UX big-endian). Fixture: the 11.11 disk's LVs | L |

### Phase 3: AIX (IPL/PReP, AIX LVM, JFS1; JFS2-BE shelved)

| # | Item | Effort |
|---|---|---|
| 3.1 | Recognise the IBM IPL record; label MBR type `0x41` as "PReP boot" (fixes the "Unknown" rows on the AIX disk, LCD4 CD and `rs6karc151.img`) | S |
| 3.2 | AIX LVM, single-PV (§3). Fixture: `AIX433_4GB.img` | M–L |
| 3.3 | AIX JFS1 read-only (superblock, inode map, indirect blocks, directories). No open-source reader exists; reverse it from the fixture. Highest-risk item | XL |
| 3.4 | ~~AIX JFS2 (endian-generic `fs/jfs.rs`)~~ **Shelved** until an AIX 5L+ fixture exists. If AIX LVM meets a JFS2 LV before then, report it as "AIX JFS2 (not yet supported)" and log that in `docs/stubs.md` | n/a |

### Phase 4: Sun QIC tapes, browsable as a filesystem (Inspect + Commander)

A `.qic` image opens as one read-only `Filesystem`, so Inspect's browse view,
Commander panes, `rb-cli ls/get` and export all work on it unchanged:

```
/                          one dir per volume when there are several, else the files directly
  vol1/
    01-boot                raw tape file (68k boot block)
    02-toc                 raw
    03-kernel              a.out, raw
    04-miniroot/           UFS image, browsed as a directory
    05-usr/                tar or .Z tar, its contents as a directory
    ...
```

Tape files have no names on tape, so the leading number is the file's position.
Where the tape's TOC file names the files (SunOS install tapes have one), use
those names instead.

| # | Item | Effort |
|---|---|---|
| 4.1 | `SUN2TAPE` container parser (`.qic`): volumes, file table, per-file classification (boot, TOC, a.out, UFS, tar, `.Z` tar) | S |
| 4.2 | `TapeFilesystem`: the tree above. Tar via the `tar` crate, `.Z` via the existing `compress(1)` LZW in `macarchive/stuffit_lzw.rs` (16-bit codes), and UFS miniroots via the UFS driver over a sub-range reader. Large `.Z` members decompress on demand, not into RAM up front | M |
| 4.3 | Wire into Inspect (source picker + browse) and Commander (open + descend); `.qic` in `DISK_IMAGE_EXTS` + regression row; CLI parity; README + MiSTer Sun-2/Sun-3 rows | S–M |

### Phase 5: corpus, regression cases, docs (alongside every phase)

- Catalogue each fixture (`fixture-map.tsv` rows; large ones in `fixtures-large/`),
  with an `rb-regress` case per item above.
- README format/filesystem/partition tables, `docs/full_MiSTer_support_status.md`
  (Sun-2, Sun-3, Atari Falcon, Minimig cores), `DISK_IMAGE_EXTS` (+ regression row),
  per the CLAUDE.md pre-commit doc sync.

## 5. Suggested order

1. **Phase 1 + 1b**: small, fixes a reported issue, and finishes the ISO check.
2. **3.1, 2.1, 2.2, 2.3**: cheap recognition work that makes three more fixtures
   inspect cleanly.
3. **2.4 + 2.5**: closes #90.
4. **3.2, then 3.4 or 3.3**: closes #91. JFS1 is the long pole.
5. **Phase 4** whenever tapes matter.

**Testing cadence:** run targeted regression suites during regular development:
unit tests for the touched module, `rb-regress run --filter` for the touched
fixtures, and a direct `rb-cli` check on the real image. Only run the full
regression suite when asked. Suggest it, with a time estimate (about 95 min in a
debug build), after major changes such as a new driver or partition scheme, or
the end of a phase.

All new drivers start **read-only** (browse, extract, backup, inspect). Edit
support, fsck repair and resize for VxFS / JFS1 / the LVMs are separate,
later decisions.

## 6. Open questions (answered 2026-10-27, kept for the record)

1. *Answered: read every filesystem on every ISO (Phase 1b).* **#91 "also add CD support":** the AIX CDs already browse as ISO 9660. Does
   this mean reading inside the `installp` `.bff` images in `inst.images` (AIX
   backup-by-name format)? Or something else, like CD-ROMs carrying an AIX
   LVM / JFS (the `cdrfs` case)?
2. *Answered: JFS2 shelved; stubs go in `docs/stubs.md`.* **JFS1 vs JFS2 priority:** the only AIX fixture is 4.3.3 (JFS1, the XL item).
   JFS2-BE is cheap but has no fixture. Is there an AIX 5L/6/7 disk to drop in?
3. *Answered: p7zip installed; unpacked and the cause is confirmed.* **`cp-2088.7z`:** no 7-Zip on this box. Unpack it to a `.vhd` in the drop, or
   install `p7zip`?
4. *Answered: in scope, as a filesystem in Inspect + Commander (Phase 4).* **Tapes:** are the Sun QIC images in scope now, or later?
5. *Answered: yes.* **`hp715-11.11v1.img`:** catalogue it as the HP-UX 11.x fixture for #90?
