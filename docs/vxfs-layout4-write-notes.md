# VxFS disk layout 4: what a write touches

Working notes for plan phase W3 (`docs/PLAN-hpux-writes.md`). Every fact here
was read off a volume made by HP-UX 11.11's `mkfs -F vxfs -o version=4`
(64 MiB, 1 KiB blocks, two 32768-block allocation units), with `fsdb -F vxfs`
naming the fields and a byte diff around one controlled HP-UX change. All
fields are big-endian on HP-UX.

## Structural inodes (fileset 1, "STRUCTURAL")

| Ino | Type | Role | Replica |
|---|---|---|---|
| 3 | IFFSH | fileset headers: block 0 = fileset 1, block 1 = fileset 999 | 35 |
| 4 | IFIAU | structural inode-allocation unit | |
| 5 | IFILT | structural inode list | 37 |
| 6 | IFCUT | current usage table, one entry per fileset | |
| 7 | IFOLT | object location table | 39 |
| 8 | IFDEV | device record | 40 |
| 9 | IFLOG | intent log (1 MiB here) | 41 |
| 10 | IFEMP | extent map: typed extents, `auemlen` (8) blocks per AU | 42 |
| 33 | IFLAB | label | |
| 34 | IFAUS | allocation-unit summary | |

The primary fileset (999, "UNNAMED") has its IAU at 64, inode list at 65,
attribute IAU 66 / list 67, LCT 68, user quotas 69. A fresh volume's primary
list holds only 32 inodes (`fsh_ninode`), so creating more files grows it.

## Creating one 6-byte file at `/hello` changes

| Block | Structure | Change |
|---|---|---|
| 8 | superblock | `free` -1 (+0xc0); `efree[0]` 1->2, `efree[1]` 1->0 (+0xc8); flags / time (+0x150) |
| 10, 1281 | fileset 999 header and its replica | `fsh_time` (+0x8), `fsh_checksum` (+0x64), `fsh_mtime` (+0x140) |
| 15 | CUT | fileset 999 entry: blocks used 0->1 |
| 35 | AU summary | AU 0's per-order free extent counts `[1,1]` -> `[2,0]` (u16 each) |
| 37-38 | primary IAU | header (`magic a505fcf5`, `au_rifree` 28->27 at +0xc); bitmap word: inode 4's bit set, MSB first |
| 40-45 | intent log | records; leave alone on a clean volume |
| 1064 | extent map | level-0 bit for block 1286 and the level-1 bit for 1286-1287 cleared |
| 1080-1081 | primary inode list | root dir (immediate) and the new inode 4 |
| 1286 | data | `hello\n` |

The superblock's own `checksum` field did not change (it covers static
geometry only).

## Checksums

- `fsh_checksum` (+0x64): 32-bit wrapping sum of the header's big-endian words
  with the checksum field taken as zero. Matches before and after.

## Extent map (IFEMP)

Each 512-byte piece covers 2048 blocks, MSB first, set = free: level 0 (one
bit per block) at +0x000 (256 bytes), level 1 (pairs) at +0x100, level 2 at
+0x180, level 3 at +0x1c0, halving to level 8 at +0x1fe and levels 9-11 in
+0x1ff. A 1 KiB map block covers 4096 blocks; `auemlen` (8) blocks cover an AU.

### Confirmed against the whole map of the fresh volume

- Level 0 is a plain free-block bitmap: every free block's bit is set.
- Levels 1-11 are a buddy map: a bit is set only for a *maximal* free aligned
  extent of 2^k blocks (allocating 1304-1307 from the free 8-block extent at
  1304 cleared its level-3 bit and set the level-2 bit for 1308-1311).
- Byte +0x1ff of a piece holds level 9 (bits 7-4), level 10 (bits 3-2) and
  level 11 (bit 1), MSB first. A wholly free piece sets only its level-11 bit.
- Orders 12-15 are never stored: they come from combining aligned free pieces.
- The AU summary (IFAUS) is 16 `u16` counts per AU, orders 0-15; the
  superblock's `efree[k]` is their sum over all AUs, `free` the free blocks.

## Directories and the inode list

- A small directory is immediate (in the inode); past 96 bytes it moves to
  directory blocks (`d1` with 40 entries used two).
- Regular files are never immediate on HP-UX: `hello\n` got a data block.
- Creating more inodes than `fsh_ninode` grows the primary inode list by one
  4-block extent (16 inodes), written to both the list inode (65) and its
  replica (97), with the IAU bitmap and the fileset header following.
- The intent log takes many records per operation; on a clean volume
  (`clean 0x5a`) nothing replays it, so a writer leaves it untouched.
