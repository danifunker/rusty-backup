# Plan: QNX filesystems (QNX 2, QNX 4, QNX 6 Power-Safe)

Goal: full support (browse, extract, edit, create, fsck) for the three QNX
filesystem generations, starting read-only like every other format. The
trigger is the Unisys ICON archive (York University) on the NAS:
`ICON_ICON2/From YorkU/UNISYS_ICON`.

## What is in the ICON archive

| Item | Contents |
|---|---|
| `Disk_1/**/*.img` | 36 floppy images: 34 are 800 KiB (1600 x 512) **QNX 2** volumes (ICON System Software 3.00-4.10.05, C compiler, Logo, Watcom BASIC / Pascal / GKS, help, fileserver boot disks, and Unisys "QDOS" 4.01.02, which is QNX too); 2 are 360 KiB MS-DOS 3.2 FAT disks we already read |
| `KryoFlux/` beside each set | raw flux streams for the same disks, including the six marked "(bad sectors)" |
| `Firmware/ICON1` | ICON 1 boot ROM halves and keyboard MCU ROM |
| `Manuals/` | ICON administration / operations / QDOS manuals, WD1002-05 MFM controller datasheets |
| `QNX 2 filesystem reader/` | `q2files.c` / `q2read.c` (public domain, Mitchell Schoenbrun): the only written description of the QNX 2 format we have |

Note the name clash: the ICON "QDOS" disks are QNX 2, unrelated to the
Sinclair QL QDOS that `fs::qdos` already reads. Detection must be by content.

## QNX 2 on-disk format (from the C sources, checked on the ICON disks)

- 512-byte blocks numbered from 1 (block 1 = the first sector of the
  volume). Block 1 holds boot data and, at byte 68, the root directory's
  48-byte entry; block 2 starts the free-block bitmap (also listed in the root
  as `bitmap`). Boot disks place the root at block 12 or 15.
- Little-endian, 16-bit `int`s. Directory entry (48 bytes): status (0x40 =
  used), first and last extent, block count, extent count, owner, group,
  unused bytes in the last block, seconds, type, group / world permissions,
  attributes (0x20 = directory), date, 16-character name + NUL.
- A file is a chain of extents; each starts with a 16-byte header
  `{prev, next, size, bound}`; data follows the header. File size =
  `blocks * 512 + 512 - (chars_free & 0x1ff) - extents * 16`.
- A directory file starts `{parent_xtnt, dir_index}` (plus `ext_parent` when
  the top bit of `dir_index` is set), then entries.
- Pinned down on the ICON disks: `size` in an extent header is the data
  bytes the extent holds, `bound` the blocks it spans beyond the first, and a
  file's length the sum of its extents' `size` (the C sources' formula from
  the directory entry overshoots). The entry's block count is the blocks held
  minus one. The `bitmap` file is one bit per block (200 bytes for 1600).
- Dates: word 0 is year-1980 in bits 15-12, month in 11-8, day in 5-1 and a
  PM flag in bit 0 (bits 7-6 are 0); word 1 is seconds since 12 o'clock. A
  first decode (PM in bit 5, day in 4-0) produced "day 0" and "31 April"
  dates that looked like a clock quirk; QNX's own `files` in icon-emu showed
  the real layout (28-May-85 3:01pm is `5539 2A91`). The `seconds` field is 0
  on every ICON file.
- Status: `0x40` in use, `0x10` modified (busy is not seen on these disks).
  Owner is `group.member` (`+16` group, `+15` member; 255.255 on nearly every
  ICON file). Attributes (`+26`, owner) and permissions (`+24` group, `+25`
  other): files `r w a e m` = `0x01 0x02 0x04 0x08 0x10` (executables are
  `0x19`); directories `r c b m`, with `c` = `0x04` and `b` = `0x08` inferred
  (every directory is owner `0x35` = r c m + dir, others `0x21` = r). Letter
  meanings are from the ICON `chattr` help page.
- Deleted entries keep their name and first extent with `0x40` clear. The
  seven on these disks were zapped while empty: a cleared header and no data.
- The bitmap is LSB first, set = in use. On every readable disk each block a
  file holds is marked; the only marked blocks no file holds are block 1, a boot
  loader (blocks 3 up to the root) and, on the 3.00-era disks, the last block.
  Watcom BASIC's bad sectors cost `basic.qnx` and `gks` their extent headers,
  which the fsck reports as 303 lost blocks (286-480, 646-753).
- `/dos/work.ph` on the QDOS disks is a blank 202-sector FAT12 disk whose jump
  bytes are zeroed; it opens as FAT. No `arch` archive floppies are in the
  archive (only the `arch` command and its help page).
- All 34 QNX 2 images export to tar (2498 entries), the bad-sector ones too.

## LEXICON hard disk (read, 2026-10-28)

`lexicon-10mb_postInst.img` (NAS `ICON_ICON2`, fixture `fs.qnx2.lexicon-10mb.hd`),
10,653,696 bytes = 20,808 sectors = 306 cyl x 4 heads x 17 spt (ST-412):

- Sector 0: a drive record in bytes 0x00-0x3F (u32 total sectors 20,808 at
  0x14, heads 4 at 0x13, 17 sectors per track at 0x1C) plus an ordinary MBR
  table and `55AA`. One entry: bootable, type `0x07`, start **30**, 20,779
  sectors.
- Sector 1: the bad-block table, `{u32 bad, u32 spare}` pairs; spares 21-31,
  every `bad` 0 (nothing remapped, so remapping is not implemented yet).
- Sectors 2-8: boot loader; 9-28 blank.
- The QNX 2 volume's block 1 (root entry at byte 68) is sector **29** and the
  bitmap's extent is sector 30: the MBR start is 1-based. 29 + 20,779 = 20,808.
  `partition::qnx2_one_based_starts` moves such an entry back one sector when
  no NTFS / HPFS / exFAT is at its start and a QNX 2 root is one sector
  earlier. Restore leaves the on-disk entry alone (`patch_mbr_entries` skips an
  entry whose stored start differs from the parsed one).
- The volume is the same format as the floppies: 425 files / 20 dirs, fsck
  clean, `/sys/icon2_emul` among them.

## A real LEXICON drive: Seagate ST-251 (2026-10-29)

`ICON4_ST251_final.bin` (NAS `ICON_ICON2/ICON4_ST251_final`, fixture
`fs.qnx2.icon4-st251.hd`) is a real fileserver disk read with David Gesswein's
MFM reader. The drive is 820 x 6 x 17 = 83,640 blocks, but the extraction wrote
8 heads per cylinder, so heads 6 and 7 of every cylinder are zeros. The header
(heads at +0x13, total at +0x14) and the file length give it away;
`rbformats::icon_hd::IconHd` reads and writes through the drive's own geometry,
and also applies bad-block-table entries (`{u32 bad, u32 replacement}`, bad 0 =
unused; every entry here is unused).

- Partition 1 (type 07, start 60 -> LBA 59): 431 files / 20 dirs; 423 files are
  byte-identical to the 4.10.01 install, `/bitmap` differs, and `QNX_1`-`QNX_5`,
  `package`, `convert_4.02` are extra. QNX's `query 1` in icon-emu: 58698 free,
  6838 used of 65536.
- Partition 2 (type 08, LBA 65594) was never formatted: zero apart from a
  16-byte stamp `fdisk` leaves at the end of its block 1. On an ICON disk the
  start - 1 rule now applies to types 07 / 08 / 09 whether or not a QNX root is
  there, and such a partition is reported as not formatted.
- The clock was unset: dates are `w0 = 0` with seconds in `w1`, which QNX lists
  as `0-----80 12:12am`. They decode as 1 January 1980 plus the time.
- RB's writes to the padded `.bin` left every padding head zero, and QNX's
  `chkfsys` agreed with the bitmap; a 150 KB file copied by QNX came back
  byte-identical.
- The `.emu` capture (Gesswein MFM reader format, `rbformats::flux::mfm_emu`)
  decodes byte-identical to the sector dump. Cells are 32-bit little-endian
  words read MSB first; Intel iSBC 214 IDs are `{FE ^ cyl_high, cyl_low,
  0x20 | head, sector}` + CRC-CCITT over `A1` and those bytes; data fields are
  `F8` + 512 bytes + a 32-bit ECC (poly 0x140A0445, init FFFFFFFF, over `A1 F8`
  and the data). The file ends with a track header of cylinder / head
  0xFFFFFFFF. `rb-cli convert` now exports what the read layer decodes for any
  container the image detector sees as raw (it used to copy flux files as is).

## Writing QNX 2 (done 2026-10-28, verified in icon-emu)

icon-emu (`github.com/danifunker/icon-emu`, CLI `icon-cli serve` + `icon-ci`;
needs the ICON ROM, NAS `ICON_ICON2/From YorkU/UNISYS_ICON/Firmware/ICON1`)
runs the real QNX 2.05b (ICON 4.10.01). The writer was learned by having QNX
make directories and files, rename, `zap` and `frel` them, and diffing the
disk after each step:

- Block 1 is laid out like a directory extent; its slot 1 (byte 68) is the
  root entry. Each directory's header `{parent_xtnt, dir_index}` names the
  slot of its own entry (the root's says block 1, slot 1); that entry's date
  changes whenever the directory does.
- `mkdir`: one block, header `{0, 0, 484, 0}`, data `{parent first extent,
  slot}` then ten empty slots. Entries form one stream across a directory's
  extents; adding the 11th fills the first block's last 12 bytes and spills 36
  into a new one-block extent (a slot straddles two extents).
- New entries: status `0x50`; files attributes `0x17`, permissions `0x01`;
  directories `0x35` / `0x21`; owner 255.255 for root. Freed slots (status
  without `0x40`) are reused.
- Extents hold at most 128 blocks: `size` is checked as a 16-bit value (65520 =
  128 x 512 - 16), and `chkfsys` reports "Non-zero extent extension" past it.
  QNX itself splits big files into 128-block extents.
- `frel` (delete) sets status 0, zeroes the first extent's `size`, frees the
  bitmap bits and keeps the name and pointers. `zap` only clears `0x40` and
  leaves the blocks marked (chkfsys then finds them unused).
- Owner: `+15` member, `+16` group. Permissions: `+24` group, `+25` other,
  `+26` owner attributes (`chattr g= m= a= p= pg=` confirmed each).

Verified: create (1 to 391 blocks, 1-4 extents), mkdir, directory growth
past one block, rename, delete, recursive delete, chmod, chown, on the 10 MB
disk, both partitions of the 64 MB disk (drives 1 and 3) and an 800 KiB
floppy (drive 2). Each time `chkfsys` ended "Rebuilt bitmap agrees with
existing bitmap" with no errors, and a file RB wrote, copied by QNX, read back
byte-identical; QNX's writes read back through RB with a clean RB fsck.

## QNX 4 on real media (2026-10-28)

`qnxfs1g.img` (qnxfs.narod.ru, fixture `fs.qnx4.qnxfs1g.hd`) was taken for FAT:
the QNX 4 boot block starts with an x86 jump and a BPB (OEM `QNX4FS`), and both
the partition and the filesystem probes tried FAT first. QNX 4's root entry at
block 1 is now probed ahead of FAT. The root also holds `lost+found` with status
`0x04` (busy) and a 1 GB `tmp` with status 0 whose extent runs a block past the
image; both are hidden, as Linux's readdir hides entries that are neither USED
nor LINK. `.inodes`, `.boot` and `.altboot` are links into the superblock.

## QNX 4 and QNX 6

- **QNX 4** (QNX 4.x, and QNX Neutrino 6.0-6.3 via `fs-qnx4`): 512-byte
  blocks, root directory entry in the superblock at block 1, `/.inodes`,
  `/.bitmap`, extent blocks (`xblk`), 48-character names via link info. Format
  references: Linux's in-tree read-only `fs/qnx4`, the qnxfs.narod.ru
  read/write driver and its QNX4 filesystem document (format documentation
  only, as with `freevxfs`). The site's `chkfs` (QNX4 checker that runs on
  Linux) and its sample image `qnxfs1g.img` are useful test inputs.
- **QNX 6 Power-Safe** (`fs-qnx6`, Neutrino 6.4+): two superblocks with a
  CRC and a sequence number (copy-on-write), inode and long-filename trees,
  512 B - 4 KiB blocks, either byte order. Format reference: Linux's in-tree
  read-only `fs/qnx6`.
- MBR partition types: 0x4D / 0x4E / 0x4F for QNX 4 (also used by QNX 6
  installs, with 0xB1-0xB3 for Power-Safe); confirm the QNX 2 hard-disk type
  from real media before relying on it.

## Phases

| Phase | Work | Verified against |
|---|---|---|
| Q1 (done) | QNX 2 read, plus dates, owners, attributes, deleted entries, RS to LF, fsck (bitmap cross-check) and the QDOS `.ph` hand-off: detection, browse, extract, Inspect / Commander, `.img` 800 KiB ICON geometry; tolerate unreadable sectors (named, not fatal); ICON fixtures + regression cases | The ICON disks themselves (files that are known text / known binaries), `q2files`-style listing |
| Q2 (done; checked on `qnxfs1g.img`) | QNX 4 read: superblock / root, inode file, extents, links, MBR types 0x4D-0x4F | `qnxfs1g.img`, images made by QNX 4.25 under QEMU, `chkfs` |
| Q3 (code done; needs real media) | QNX 6 Power-Safe read: superblock pair, trees, long names, both byte orders | Images made by QNX 6.5 / 7 under QEMU |
| Q4 | QNX 2 edit + create + fsck | QNX 2 `chkfsys` running under emulation (see Oracles) |
| Q5 | QNX 4 edit + create + fsck | QNX 4.25 `chkfsys` / `dcheck` under QEMU, plus `chkfs` |
| Q6 | QNX 6 edit + create + fsck | QNX 6.5 / 7 `chkqnx6fs` under QEMU |
| Q7 (optional) | KryoFlux stream decode for the "(bad sectors)" disks: re-read marginal sectors from flux instead of the existing `.img` | Sector checksums; files that read whole afterwards |

## Oracles needed for the write phases

Every write path ships only after the native checker accepts it, as for the
HP-UX work. That needs a running QNX of each generation:

- QNX 2: QNX 2.x for the IBM PC (it runs in PCem / 86Box, possibly QEMU) with
  its `chkfsys`. An ICON emulator would be ideal but none is known; the ROMs
  in `Firmware/ICON1` are the starting point if one is ever built.
- QNX 4: QNX 4.25 boots in QEMU.
- QNX 6: QNX 6.5 or QNX 7 boots in QEMU.

Install media for these is the one thing not already on hand.

## KryoFlux (done: read-only, IBM MFM)

Read-only KryoFlux stream support (`trackNN.S.raw`), as a disk-image
abstraction beneath the filesystems: decode flux to sectors for the known
formats (MFM PC / ICON 800 KiB, and the others MiSTer users image), present
the result as an ordinary sector image, and report unreadable sectors rather
than failing. The ICON archive's "(bad sectors)" disks are the first test.

Follow-ups done 2026-10-28: a phase-tracking PLL (Greaseweazle's period /
phase adjust, 5% / 60%) ahead of the old per-interval rounding, a CRC-checked
byte vote across damaged copies, the fault list in the GUI Inspect tab, and
SCP / HFE v1 / HFE v3 readers (verified on `gw convert` output, byte-identical
to the dumps). Unreadable sectors went from 257 to 253 across the six damaged
sets; what is left is media damage, not clock recovery. On the fileserver boot
disk the faults sit in a band (records 2, 3, 7, 8 of head 0, cylinders 0-16),
and the 16-24 copies of each disagree in hundreds of bytes. IBM FM and AmigaDOS MFM decoders followed (picked by probing; checked on
`gw convert` output of an ADF and an Acorn DFS SSD). Still open: GCR.
