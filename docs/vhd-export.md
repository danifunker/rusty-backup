# VHD Export

Rusty Backup can export disk images and backup folders as Fixed VHD files,
compatible with VirtualBox, QEMU, Hyper-V, and other virtualisation tools.

## Export Modes

### Whole Disk

Produces a single `.vhd` file containing the MBR, all partitions at their
original offsets, and inter-partition gaps. Partition sizes can be
individually adjusted; the MBR partition table entries are patched to
reflect the new sizes.

### Per Partition

Produces one `.vhd` file per partition. Each file contains only the
partition data (no MBR or gap regions).

## Partition Resizing

Each partition can be exported at one of three sizes:

- **Original** -- the full partition size as recorded in the partition table.
- **Minimum** -- the smallest size that preserves all filesystem data,
  computed via filesystem analysis (FAT16/32 only). Shown only when the
  minimum is smaller than the original.
- **Custom** -- any value between the minimum and 2 TiB (the Fixed VHD
  spec maximum). Values larger than the original are allowed for growing
  partitions; the extra space is zero-filled.

Partition start offsets are never changed. Shrinking a partition leaves
unused space before the next partition; growing pads with zeros after the
original data.

### What Gets Patched

When a partition is resized:

1. **MBR `total_sectors`** (whole-disk mode only) -- the 4-byte
   little-endian field at bytes 12-15 of each partition entry is updated.
2. **FAT filesystem resize** (`resize_fat_in_place`) -- a full in-place
   resize of FAT12/16/32 filesystems, described below.

Non-FAT partitions (NTFS, ext2/3/4, etc.) are exported without filesystem
patching. The filesystem tools inside the guest OS can extend or repair the
filesystem after mounting.

### FAT In-Place Resize

The `resize_fat_in_place()` function handles both shrinking and growing of
FAT12, FAT16, and FAT32 partitions during VHD export.

**Shrinking** updates the BPB `total_sectors` field only. The existing FAT
remains oversized, which is harmless -- the extra entries are all free
(`0x0000`) and legacy operating systems ignore them.

**Growing** follows one of two paths depending on whether the existing FAT
has enough capacity to address the new clusters:

- **FAT has spare capacity** (existing FAT sectors can already address the
  new cluster count): only the BPB `total_sectors` is updated. The new
  clusters are already covered by the existing FAT padding.

- **FAT needs additional sectors** (existing capacity exhausted):
  1. The existing FAT is read into memory.
  2. The data region (and root directory for FAT12/16) is shifted forward
     using a backward copy to avoid overwriting unread data. The gap left
     behind is zero-filled.
  3. The extended FAT -- padded with free-cluster entries (`0x0000`) for
     the new clusters -- is written to each FAT copy.
  4. The BPB `sectors_per_fat` field is updated.

**FAT32 extras:** the backup BPB at sector 6 is always kept in sync, and
the FSInfo sector is updated (free cluster count set to `0xFFFFFFFF` so the
OS recomputes it on the next mount; the next-free-cluster hint is set to
the first new cluster when growing, or to unknown when shrinking).

**Safety guard:** if the resize would cause the FAT type to cross a
boundary (e.g. FAT16 → FAT12), only the BPB `total_sectors` is updated and
the FAT tables are left untouched to avoid data loss.

#### Helper functions

| Function | Purpose |
|----------|---------|
| `compute_fat_sectors()` | Calculates required FAT sectors per copy. Uses closed-form formulas for FAT16/32 and an iterative approach for FAT12 (1.5 bytes per entry). |
| `shift_region_forward()` | Bulk-moves file data forward by reading backward in chunks, then zero-fills the gap. |
| `patch_bpb_total_sectors()` | Updates the 16-bit or 32-bit BPB total sectors field, matching whichever field the original BPB used. |
| `write_bpb()` | Writes the BPB to the primary boot sector and (FAT32) the backup at sector 6. |

## VHD Footer Format

Every Fixed VHD ends with a 512-byte footer following the Microsoft VHD
v1.0 specification:

| Offset | Size | Field                |
|--------|------|----------------------|
| 0      | 8    | Cookie (`conectix`)  |
| 8      | 4    | Features             |
| 12     | 4    | Format Version (1.0) |
| 16     | 8    | Data Offset (fixed: `0xFFFFFFFFFFFFFFFF`) |
| 24     | 4    | Timestamp (seconds since 2000-01-01 UTC) |
| 28     | 4    | Creator App (`rsbk`) |
| 32     | 4    | Creator Version      |
| 36     | 4    | Creator Host OS      |
| 40     | 8    | Original Size        |
| 48     | 8    | Current Size         |
| 56     | 4    | Disk Geometry (CHS)  |
| 60     | 4    | Disk Type (`2` = Fixed) |
| 64     | 4    | Checksum             |
| 68     | 16   | Unique ID            |

The checksum is the one's complement of the sum of all footer bytes,
treating the checksum field itself as zero.

## Supported Source Types

- **Raw disk images / devices** -- partition data is read directly at the
  LBA offset. VHD files used as sources are auto-detected (the 512-byte
  footer is excluded from the data portion).
- **Backup folders** -- partition data is decompressed from the backup's
  compressed files (raw, zstd, or CHD) and written into the VHD.

## Limitations

- Only MBR partition tables are patched. EBR entries for logical partitions
  are not updated during resize.
- Filesystem analysis (minimum size calculation) is limited to FAT12/16/32.
  NTFS and ext partitions fall back to the original size as the minimum.
- Split VHD output is not supported; each export produces a single file.
