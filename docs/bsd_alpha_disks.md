# FreeBSD/Alpha disks

`rb-cli new hd bsd-alpha` writes a disk FreeBSD 4.x and 5.x on Alpha can use as it is. Its
installer (5.2.1's `libdisk`) only sets up a disk when GEOM already reports a BSD label with a
real, non-`c` partition, so a blank disk is no use to it. Built for the DEC AlphaStation 200
MiSTer core; the layout is checked against two reference disks (below).

```
rb-cli new hd bsd-alpha OUT.img --size 4G \
    a:512M:4.2BSD:/ b:512M:swap e:512M:4.2BSD:/var f:256M:4.2BSD:/tmp g:rest:4.2BSD:/usr \
    --format [--bootblock boot1]
```

Each partition is `LETTER:SIZE[:FSTYPE[:MOUNT]]` in disk order: a letter `a`-`h` other than
`c`, a size (`rest` once), an fstype (`4.2BSD` by default, `swap`, `unused`, or any name or
number FreeBSD's `fstypenames` knows). MOUNT is only echoed back: a label holds no mount points.
Starts are rounded to `--align` (1 MiB).

## Sector 0

| Bytes | What |
|-------|------|
| 0-63 | zero |
| 64-339 | little-endian 4.4BSD `struct disklabel` (`LABELSECTOR 0`, `LABELOFFSET 64` on Alpha) |
| 480-503 | the SRM boot-block descriptor: block count, start LBA, flags (one quadword each) |
| 504-511 | the SRM checksum: the sum of the first 63 little-endian quadwords |

The label: `d_magic` / `d_magic2` `0x82564557`, `d_type` 4 (SCSI), `d_typename` `da0`, 512-byte
sectors, 255 heads x 63 sectors (`d_secpercyl` 16065, `d_ncylinders` = sectors / 16065),
`d_rpm` 3600, `d_interleave` 1, `d_npartitions` 8, `d_bbsize` and `d_sbsize` 8192. Slot `c` is
the whole disk, fstype unused. `d_checksum` makes the label (148 + 16 x 8 bytes) XOR to zero.

Partition `a` starts at sector 0, around the label: UFS leaves the first 8 KiB of a partition
alone, and FreeBSD keeps its label and boot code there.

## `--format`

Every 4.2BSD partition becomes a little-endian UFS1 with 16 KiB blocks and 2 KiB fragments,
which is what FreeBSD 4.6 / 5.2.1's `newfs` gave these disks. Once they exist the label is
written again with each one's `p_fsize`, `p_frag` and `p_cpg` read from its superblock
(`provision::finish_bsd_label`), and both checksums recomputed. Our writer's cylinder groups
are its own, so `p_cpg` differs from `newfs`'s (1 rather than 89-94); it always matches the
superblock it describes.

## `--bootblock`

FreeBSD/Alpha's `/boot/boot1` (7484 bytes on 4.6 and 5.2.1) goes from sector 1, at most 15
sectors so it stays clear of the superblock at 8 KiB. Sector 0's descriptor then reads (blocks,
1, 0), as `disklabel -B` writes it, and the SRM checksum is recomputed.

## Checked against

* FreeBSD 4.6, labelled by its own `sysinstall` (2 GiB, a/b/c/e/f/g).
* FreeBSD 5.2.1, labelled and formatted from 4.6 (4 GiB, the command above's layout, boot block
  installed).

For the 4 GiB command above, sector 0 matches the 5.2.1 disk byte for byte apart from `p_cpg`
and the label checksum that follows from it; with `--bootblock` the descriptor matches too.
`partition.bsd-alpha.*` (tier 1) and `bsd_alpha_build` (unit test) cover it.

## Reading

The reader probes a BSD label at bytes 1024, 512, 64 and 128 before any other table, so a
valid label (both magics and the checksum) wins over the UFS at sector 0 and every slice
browses. Editing an existing BSD label is not implemented; the partition editor treats one as
read-only.
