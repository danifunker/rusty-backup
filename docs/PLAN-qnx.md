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
- Dates: the `seconds` field is 0 on every ICON file and the two-word `date`
  packing is not yet decoded, so no timestamps are shown.
- All 34 QNX 2 images export to tar (2498 entries), the bad-sector ones too.

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
| Q1 (done) | QNX 2 read: detection, browse, extract, Inspect / Commander, `.img` 800 KiB ICON geometry; tolerate unreadable sectors (named, not fatal); ICON fixtures + regression cases | The ICON disks themselves (files that are known text / known binaries), `q2files`-style listing |
| Q2 (code done; needs real media) | QNX 4 read: superblock / root, inode file, extents, links, MBR types 0x4D-0x4F | `qnxfs1g.img`, images made by QNX 4.25 under QEMU, `chkfs` |
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
