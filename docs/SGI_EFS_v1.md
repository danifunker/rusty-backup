# SGI EFS v1 and the SGI disk label (IRIS 2000 / 3000)

The on-disk formats used by Silicon Graphics' first workstations — the IRIS
2000 and 3000 series, 68020 machines running SGI's System V derivative around
1986-1988. Two formats are involved:

- the **SGI disk label** at block 0 of the drive, which carves it into slots
  (`src/partition/sgi_dklabel.rs`), and
- **EFS v1**, the original Extent File System, in each slot
  (`src/fs/efs_v1.rs`).

Support is **read-only**: parse the label, browse / inspect / extract from the
filesystems. Writing, resize and fsck are not implemented.

## Relationship to IRIX EFS

`src/fs/efs.rs` implements the EFS that shipped with IRIX (magic `0x072959`).
This is its ancestor, and the two are **not interchangeable**:

| | EFS v1 (here) | IRIX EFS (`efs.rs`) |
|---|---|---|
| `fs_magic` | `0x041755` (`EFS2_MAGIC` `0x041756`) | `0x072959` / `0x07295A` |
| Superblock | 162 bytes, 2-byte packed, no `fs_bmblock` / `fs_replsb` / `fs_lastialloc` | 92 bytes, 4-byte packed |
| Directories | System V `struct direct`: 16-bit inode + 14-byte name | slotted blocks with a `0xBEEF` header |
| Partition table | SGI disk label, `D_MAGIC` `0x072959` | SGI volume header, `0x0BE5A941` |
| Inode / extents | identical | identical |

The number `0x072959` appears in both eras, but not for the same thing: here it
is the *disk label's* magic, and SGI later reused it as the *filesystem's*
magic. Detection keys off position and structure, not the constant alone.

Only the inode and extent layout is shared, which is why this is a separate
driver rather than a flag on the existing one.

## Provenance

Every structure below is transcribed from SGI's own headers, recovered from the
`/usr` filesystem of a working IRIS 3130 disk image (a 60 MB `Priam V170`):

| Header | RCS revision | Defines |
|---|---|---|
| `<sys/efs_sb.h>` | 1.5, 87/06/17 | `struct efs`, `EFS_MAGIC`, `EFS2_MAGIC` |
| `<sys/efs_ino.h>` | 1.4, 86/10/08 | `struct efs_dinode`, `struct extent` |
| `<sys/efs_fs.h>` | — | the block layout and the `EFS_ITO*` macros |
| `<sys/dir.h>` | 1.1, 86/05/19 | `struct direct` |
| `<sys/dklabel.h>` | 1.5, 87/11/20 | `struct disk_label`, `D_MAGIC`, `DT_*`, `DC_*` |

The reader was cross-checked against that disk file for file: 2,982 entries
across its two filesystems, every SHA-256, mode, size, mtime and symlink target
matching an independent decoder written from the same headers.

Three further invariants close independently on all three volumes of that disk,
which is what makes the field offsets certain rather than merely plausible:

1. `fs_size == fs_firstcg + fs_ncg * fs_cgfsize` — exactly.
2. The blocks reachable from every in-use inode's extents, plus the metadata
   blocks the geometry implies, equal `fs_size - fs_tfree` — exactly (11,706 /
   3,929 / 49,606 blocks).
3. `fs_checksum` reproduces under the same rotate-and-XOR IRIX EFS uses, run
   over offsets `0x00..0x9E`. A field misplaced by even two bytes breaks it.

## Byte order

Images taken off these machines' disk controllers are **byte-swapped within
every 16-bit word** — label, superblocks, inodes and file data alike. The
sample disk reads as `rPai m1V07` where the drive name `Priam V170` should be,
and `0700 5929` where `D_MAGIC` should be.

Both the label parser and the filesystem driver probe their magic in both
orientations and fix up every block on the way in. Nothing is normalised at the
reader level: a backup of one of these disks must stay byte-identical to the
source, so only the *interpretation* is swapped, never the stored bytes.

Because both orientations read identically, the orientation an image happens to
be in is invisible unless we say so. `rb-cli inspect` and `rb-cli ls` therefore
name it (`Partition table: SGI-DkLabel (byte-swapped)`), as does the GUI's
Inspect tab, and `rb-cli inspect --format json` carries it as `byte_order`.

To move an image between the two, `rb-cli swab16 IN OUT` (or `--in-place`)
rewrites every 16-bit word; the GUI's Inspect tab exposes the same thing as a
*Swap Word Order...* button. The transform is an involution, so one command
converts in either direction, and running it twice returns the original bytes.
It is deliberately format-agnostic — it knows nothing about SGI and will happily
fix up any medium captured through a word-swapping controller — but it probes
the partition table before and after so the flip is visible rather than blind.

Whether the swap happened in the drive, the controller, or the dumping program
is not something the image can tell us. It does not matter for reading: what
matters is that the ASCII fields read correctly in exactly one orientation, and
that is the one the drivers interpret.

## Writing

The driver is read/write. `EditableFilesystem` covers create / delete / rename
for files and directories, symlinks, `set_permissions` and `set_owner`;
`create_blank_efs_v1` formats a fresh volume (`rb-cli new volume efs-v1`).

Four things the write path has to get right, each verified against the real
IRIS 3130 disk rather than assumed:

- **Byte order is symmetric.** `write_blocks` applies the image's word order on
  the way out exactly as `read_blocks` applies it on the way in. Writing into a
  byte-swapped volume therefore leaves it byte-swapped and internally
  consistent; the alternative is a volume that is half one orientation.
- **The checksum span is 0x9E, not IRIX's 0x58.** `efs_v1_superblock_checksum`
  runs the same rotate-and-XOR over the longer span. Verified: it reproduces the
  stored `fs_checksum` of both EFS volumes on the sample disk exactly, and the
  IRIX-span routine does not.
- **Inode tables are marked in use in the bitmap.** This is the opposite of
  IRIX EFS, where `mkfs` leaves those bits set and a bitmap-only first-fit
  allocator walks into live inodes. On the sample disk every inode-table bit
  reads as in-use and the free-bit count matches `fs_tfree` exactly, on both
  volumes. The allocator still refuses to hand out anything outside a cylinder
  group's data area, so a damaged or foreign bitmap cannot talk it into
  overwriting an inode table.
- **`fs_tinode` is one below the free-inode count.** Counting `di_mode == 0`
  slots and comparing with the stored field gives a delta of exactly 1 on both
  volumes, so `mkfs` holds one back. `create_blank_efs_v1` reproduces that.

Directories are the System V shape throughout: fixed 16-byte records, a removed
entry has its inode number zeroed and the directory keeps its size, and a name
is capped at `DIRSIZ` (14 bytes) because a full-length name is not
NUL-terminated.

There is no journal, so ordering is the only durability tool available. On
create the bitmap reaches disk *before* the inodes that cite those blocks, and
on delete it reaches disk *after* the inode is cleared. Both orders fail toward
leaked free space, which a future fsck can reclaim, rather than toward two
inodes sharing a block. File payloads stream one extent at a time (at most
`EFS_MAXEXTENTLEN` blocks), so a large file never lands in RAM whole.

Adding a file to a copy of the real 60 MB disk, in both orientations, leaves
every pre-existing file byte-identical (a `diff -r` of the extracted tree is
clean), leaves the untouched `/usr` volume bit-identical, and leaves a valid
superblock checksum with `fs_dirty` still zero.

## m68k struct packing

These headers were compiled for the 68020, where the alignment requirement for
a `long` is **2 bytes, not 4**. So `long` fields sit at merely-even offsets and
the C compiler inserts no padding to reach a multiple of four.

This is the single easiest way to misread both formats. In `struct efs`,
`fs_time` follows the `short fs_dirty` at offset 0x16; assume 4-byte alignment
and it lands at 0x18, putting `fs_magic` two bytes out of place and every field
after it with it. The same applies to `d_altstart` at 0x0E in the disk label.

## `struct disk_label` — block 0

274 bytes (`0x112`). Big-endian, 2-byte packed.

| Offset | Type | Field | Notes |
|--------|------|-------|-------|
| 0x00 | be32 | `d_magic` | `D_MAGIC` = `0x00072959` |
| 0x04 | be16 | `d_type` | drive type, `DT_*` |
| 0x06 | be16 | `d_controller` | `DC_DSD5217` 0, `DC_XYLOGICS450` 1, `DC_INTERPHASE2190` 2, `DC_STORAGER` 3 |
| 0x08 | be16 | `d_cylinders` | |
| 0x0A | be16 | `d_heads` | |
| 0x0C | be16 | `d_sectors` | sectors per track |
| 0x0E | be32 | `d_altstart` | first block of the alternates region |
| 0x12 | be16 | `d_nalternates` | blocks reserved there |
| 0x14 | u8 | `d_bootfs` | slot the PROM boots from |
| 0x15 | u8 | `d_swapfs` | slot used as swap |
| 0x16 | 8 × 8 B | `d_map[NFS]` | `struct disk_map { be32 d_base; be32 d_size; }` |
| 0x56 | i8 | `d_interleave` | |
| 0x57 | i8 | `d_trackskew` | |
| 0x58 | i8 | `d_cylskew` | (one pad byte follows) |
| 0x5A | be16 | `d_badspots` | |
| 0x5C | char[50] | `d_name` | drive model, e.g. `Priam V170` |
| 0x8E | char[50] | `d_serial` | |
| 0xC0 | be32[20] | `d_misc` | gap and group sizes |
| 0x110 | u8 | `d_rootnotboot` | nonzero when root and boot differ |
| 0x111 | u8 | `d_rootfs` | root slot when the above is set |

Blocks 1-4 hold the bad-block map: 64 × `struct disk_bbm { long d_bad;
long d_good; }` per block, 256 entries maximum. No filesystem starts before
block 5.

`d_cylinders * d_heads * d_sectors` is the whole drive;
`d_altstart + d_nalternates` equals it, with `d_altstart` marking the end of
usable space.

The slots carry **no type field**. Roles come from `d_bootfs` / `d_swapfs` /
`d_rootfs`, and the label conventionally repeats the whole usable disk in one
or more trailing slots. Those wrapper slots overlap the real ones, so — as with
the SGI volume header's VOLHDR / VOLUME and Sun's `backup` slice — a slot that
swallows another, or duplicates an earlier one, is filtered out of the browse
list.

### Sample: the IRIS 3130 `Priam V170`

987 cylinders × 7 heads × 17 sectors = 117,453 blocks; `d_altstart` 115,430 +
`d_nalternates` 2,023 = 117,453. `d_bootfs` 0, `d_swapfs` 1.

| Slot | `d_base` | `d_size` | Ends | Holds |
|---|---|---|---|---|
| 0 | 119 | 17,850 | 17,969 | root filesystem (17,848 blocks) |
| 1 | 17,969 | 17,731 | 35,700 | swap |
| 2 | 35,700 | 79,730 | 115,430 | `/usr` filesystem (79,704 blocks) |
| 3, 6 | 119 | 115,311 | 115,430 | whole-disk wrappers |
| 7 | 0 | 115,430 | 115,430 | whole volume |

Slot 1 still contains a stale EFS superblock from an earlier layout — the
label says it is swap, and the filesystem there is inconsistent (three inodes
sharing one block, one more in-use inode than `fs_tinode` accounts for). That
is a property of the disk, not of the decode; it is why the swap slot is
presented as swap rather than as a mountable volume.

## `struct efs` — the superblock, block 1 of the slot

The on-volume portion is 162 bytes (`0xA2`). Big-endian, 2-byte packed.

| Offset | Type | Field | Notes |
|--------|------|-------|-------|
| 0x00 | be32 | `fs_size` | blocks, excluding the tail past the last group |
| 0x04 | be32 | `fs_firstcg` | block of the first cylinder group |
| 0x08 | be32 | `fs_cgfsize` | blocks per cylinder group |
| 0x0C | be16 | `fs_cgisize` | inode blocks per cylinder group |
| 0x0E | be16 | `fs_sectors` | sectors per track |
| 0x10 | be16 | `fs_heads` | heads per cylinder |
| 0x12 | be16 | `fs_ncg` | cylinder groups |
| 0x14 | be16 | `fs_dirty` | needs fsck |
| 0x16 | be32 | `fs_time` | last superblock update |
| 0x1A | char[6] | `fs_fname` | filesystem name, e.g. `root` |
| 0x20 | char[6] | `fs_fpack` | pack name, e.g. `sgi` |
| 0x26 | be32 | `fs_magic` | `EFS_MAGIC` `0x041755`, `EFS2_MAGIC` `0x041756` |
| 0x2A | be32 | `fs_prealloc` | preferred pre-allocation run (16 on the sample) |
| 0x2E | be32 | `fs_bmsize` | bitmap length in **bytes** |
| 0x32 | be32 | `fs_tfree` | free data blocks |
| 0x36 | be32 | `fs_tinode` | free inodes |
| 0x3A | char[100] | `fs_spare` | zero |
| 0x9E | be32 | `fs_checksum` | rotate-and-XOR over 0x00..0x9E |

Note what is *absent* relative to IRIX EFS: there is no `fs_bmblock` (the
bitmap is fixed at block 2), no `fs_replsb` (no replicated superblock), and no
`fs_lastialloc`.

## Block layout

Per `<sys/efs_fs.h>`, in 512-byte basic blocks, relative to the slot:

```
  0                unused
  1                superblock                       (EFS_SUPERBB)
  2 ..             bitmap, ceil(fs_bmsize/512) blocks  (EFS_BITMAPBB)
  ..  fs_firstcg   unused
  fs_firstcg ..    fs_ncg groups of fs_cgfsize blocks,
                   each opening with fs_cgisize inode blocks
  fs_size ..       trailing blocks, outside the filesystem
```

`fs_size == fs_firstcg + fs_ncg * fs_cgfsize`.

The free-block bitmap uses **set bit = free**, LSB-first within each byte —
block *N* is bit *N* % 8 of byte *N* / 8. Same convention as IRIX EFS, and the
opposite of most other filesystems. Counting set bits over `fs_bmsize` bytes
reproduces `fs_tfree` exactly on all three sample volumes; MSB-first does not.

## `struct efs_dinode` — inodes

128 bytes, 4 per block. Identical to IRIX EFS except that offset 0x1E is
`di_refs` (reorganiser bookkeeping) where IRIX put `di_version` + `di_spare`.

| Offset | Type | Field |
|--------|------|-------|
| 0x00 | be16 | `di_mode` |
| 0x02 | be16 | `di_nlink` |
| 0x04 | be16 | `di_uid` |
| 0x06 | be16 | `di_gid` |
| 0x08 | be32 | `di_size` |
| 0x0C | be32 | `di_atime` |
| 0x10 | be32 | `di_mtime` |
| 0x14 | be32 | `di_ctime` |
| 0x18 | be32 | `di_gen` |
| 0x1C | be16 | `di_numextents` |
| 0x1E | be16 | `di_refs` |
| 0x20 | union | `di_extents[12]`, or `di_dev` for a character / block special |

Inode 2 is the root. Locating inode *i*, from the `EFS_ITO*` macros:

```
  cg     = i / (fs_cgisize * 4)
  block  = fs_firstcg + cg * fs_cgfsize + ((i / 4) % fs_cgisize)
  offset = (i % 4) * 128
```

`di_size` is a 32-bit `off_t` the kernel treats as signed, capping a file at
2 GiB - 1.

Despite the System V lineage, **symbolic links exist** (`S_IFLNK`) — SGI
carried the 4.2BSD ones across. The target is stored as ordinary file data,
bounded by `MAXPATHLEN`, not by the 14-byte directory-entry limit. The sample
disk has three, including `/usr/include/machine`.

## `struct extent` — 8 bytes

```c
typedef struct extent {
    unsigned int ex_magic:8,    /* MUST BE ZERO */
                 ex_bn:24,      /* first block */
                 ex_length:8,   /* blocks */
                 ex_offset:24;  /* logical block offset into the file */
} extent;
```

Limits from `<sys/efs_ino.h>`: `EFS_DIRECTEXTENTS` 12, `EFS_MAXEXTENTS` 2048,
`EFS_MAXEXTENTLEN` 248.

An extent is bad when `ex_magic` is nonzero, when `ex_length` is 0 or over
`EFS_MAXEXTENTLEN`, when `ex_bn` is below `fs_firstcg` or reaches `fs_size`, or
when `(ex_offset, ex_length)` overlaps another extent.

`ex_bn == 0` is **not** a bad extent — it marks a **hole**: a range never
written, which reads as zeros.

When `di_numextents` exceeds 12 the inode switches to **indirect** mode. The
inline slots stop describing data and instead point at runs of blocks packed
with extent records, 64 to a 512-byte block; `di_extents[0].ex_offset` holds
the number of inline slots used that way. Extents are not necessarily stored in
logical order, so the reader sorts by `ex_offset`.

## `struct direct` — directories

Flat arrays of 16-byte records, no slotting, no `0xBEEF` header:

```c
#define DIRSIZ 14
struct direct {
    ino_t d_ino;            /* be16 — 65,535 inodes maximum */
    char  d_name[DIRSIZ];   /* NUL-padded, not NUL-terminated at 14 */
};
```

`d_ino == 0` marks a free slot. The directory's `di_size` is the number of
records times 16. `.` and `..` are real entries and are hidden by the browse
layer.

A record pointing past the inode table is treated as damage and dropped with a
warning, rather than failing the whole listing — the sample disk's stale swap
slot contains exactly that.

## What is not implemented

Write, resize, fsck, and volume creation. The format is fully understood and
the free-space bitmap decodes correctly, so a writer is tractable; it is simply
out of scope for read-only support.

An emulation oracle would also be welcome and does not currently exist: no
emulator runs IRIS 3000-series hardware well enough to boot this OS and check
our work the way FS-UAE, IRIX-on-MAME and the BasiliskII fixtures do elsewhere.
The three self-consistency invariants listed under **Provenance**, plus the
file-for-file comparison against an independent decoder, are what stand in for
one.
