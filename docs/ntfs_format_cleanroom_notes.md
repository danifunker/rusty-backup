# Clean-room NTFS formatter — provenance & geometry notes

This document records where every structure the NTFS formatter
(`src/fs/ntfs_format.rs`) writes was derived from, so the clean-room
provenance is auditable, and documents the supported geometry and the
cluster-size-by-volume-size defaults.

## License discipline

`mkntfs` (ntfsprogs / ntfs-3g) is GPL-2-only and is **not** license-compatible
with this project's AGPL-3.0. Therefore **no `mkntfs.c` / `ntfs-3g` source was
read or translated**. The formatter was written from:

- The public NTFS on-disk format documentation (the Linux-NTFS project /
  flatcap.org "NTFS Documentation" by Richard Russon & Yuval Fledel) — a spec,
  not GPL code: BPB layout, `FILE` record header, attribute headers, the
  mapping-pairs (runlist) encoding, `$STANDARD_INFORMATION`, `$FILE_NAME`,
  `$DATA`, `$BITMAP`, `$INDEX_ROOT` / `$INDEX_ALLOCATION`,
  `$SECURITY_DESCRIPTOR`, `$VOLUME_NAME` / `$VOLUME_INFORMATION`.
- Microsoft's published NTFS boot-sector / BPB field documentation.
- **Black-box observation of `mkntfs` *output*** (not its source): images were
  formatted at many `-s`/`-c` geometries and their on-disk *result* inspected
  with `ntfsinfo` / `hexdump`. Observing output is not deriving from source.

The geometry-dependent backbone (boot sector / BPB, `FILE` record headers,
runlists, update-sequence/fixup arrays, the cluster and MFT bitmaps, `$MFTMirr`,
the root directory index) is **synthesised parametrically** from the geometry.
No raw bytes are lifted from a `mkntfs`-produced image for any of it.

## Data tables (geometry-independent) — `src/fs/ntfs_tables.rs`

These are *data*, not code, and do not change with sector/cluster size. They are
reproduced verbatim from observed `mkntfs` output and the published tables:

| Table | Source / provenance | Why it is geometry-independent |
|-------|---------------------|--------------------------------|
| `ATTRDEF` (`attrdef.bin`, 2560 B) | The fixed Microsoft `$AttrDef` attribute-definition table. | Same on every NTFS volume regardless of geometry. |
| `UPCASE` (`upcase.bin`, 131072 B) | The standard Unicode uppercase mapping (`$UpCase`), 65536 UTF-16 code units. | Unicode case folding does not depend on disk geometry. |
| `SECURE_SDS` (`secure_sds.bin`, 262396 B) | The standard `$Secure:$SDS` security-descriptor stream mkntfs writes. | Byte offsets/ids within the stream, not clusters. |
| `ROOT_SECDESC` (`root_secdesc.bin`, 4140 B) | Root directory `$SECURITY_DESCRIPTOR` value. | A self-relative SD; contains no cluster references. |
| `SEC_DESC_100` | Standard 100-byte resident `$SECURITY_DESCRIPTOR` for the metafiles that carry their own descriptor. | A self-relative SD. |
| `SECURE_SDH_IDXROOT` / `SECURE_SII_IDXROOT` | `$Secure` `$SDH`/`$SII` `$INDEX_ROOT` values. | Index over security ids/hashes → `$SDS` byte offsets. |
| `UPCASE_INFO` | `$UpCase:$Info` (a CRC of the `$UpCase` table). | A checksum of `UPCASE`, geometry-independent. |
| `EXTEND_I30_IDXROOT` | `$Extend` directory `$I30` `$INDEX_ROOT`. | MFT-reference index, no cluster references. |

The three **geometry-dependent** blobs that previously backed the formatter —
`boot.bin`, `mft_records.bin`, `root_index.bin` — have been **deleted**; the
formatter now synthesises the boot sector, the 16 system MFT records, and the
root index parametrically.

## Geometry observations (black-box, from `mkntfs` output)

Verified by formatting at sector ∈ {512, 4096} and cluster ∈
{512 … 131072} and decoding the boot sector:

- **`total_sectors` @0x28** = `volume_bytes / bytes_per_sector − 1`. The final
  sector holds the backup boot sector.
- **MFT-record size** defaults to `max(1024, bytes_per_sector)`. With 4096-byte
  sectors `mkntfs` uses 4096-byte MFT records (a record must contain whole
  sectors for its fixups). Index-record size defaults to a fixed **4096 bytes**
  (`max(4096, bytes_per_sector)`), even at 64 KiB clusters.
- **`sectors_per_cluster` @0x0D** is a plain unsigned byte when
  `spc ≤ 128`; for `spc ≥ 256` (clusters where `cluster/sector ≥ 256`, e.g.
  128 KiB clusters on 512-byte sectors) it switches to the *signed negative
  power* encoding `byte = −log2(spc)` (so `0xF8 = −8 → 2^8 = 256`).
- **clusters-per-MFT-record @0x40 and clusters-per-index-record @0x44** use the
  signed power-of-two encoding: a positive cluster count when the structure is
  ≥ one cluster, else the signed `−log2(size_in_bytes)` (e.g. a 1024-byte record
  on 4096-byte clusters → `0xF6 = −10 → 2^10 = 1024`).
- **`$MFTMirr` LCN @0x38**: `mkntfs` places it at `total_clusters/2 − 1`. This
  formatter instead uses a simple **contiguous** metadata layout (mirror placed
  right after `$MFT`); both are valid — the mirror's location is read from the
  BPB. The reserved MFT-growth "zone" `mkntfs` keeps is **not** replicated: for a
  clone/format target a contiguous pack is the goal (see "no silent caps"
  below — this is a documented, deliberate choice, not a hidden one).
- The boot **code** region (offset 0x54…0x1FD) is not validated by
  `ntfs-3g`/`ntfsfix`; this formatter writes a minimal VBR (jump + `"NTFS    "`
  + BPB + extended BPB + `0x55AA`) with the boot-code area **zeroed**, so no
  `mkntfs` bootstrap code is copied. Verified: `ntfsfix -n` accepts a VBR with
  the boot code fully zeroed.

## Default cluster size by volume size (`NtfsGeometry::for_volume_size`)

Replicates the classic `mkntfs` default-cluster table (values cited as facts):

| Volume size | Default cluster |
|-------------|-----------------|
| ≤ 512 MiB | 512 B |
| ≤ 1 GiB | 1024 B |
| ≤ 2 GiB | 2048 B |
| > 2 GiB | 4096 B |

4096 B is the modern NTFS default and covers volumes up to 16 TiB; larger
clusters are available explicitly via `with_cluster_size`.

## Supported / unsupported geometry

- **bytes_per_sector** ∈ {512, 1024, 2048, 4096} (power of two).
- **cluster_size** = `bytes_per_sector × sectors_per_cluster`, power of two,
  `≥ bytes_per_sector`, NTFS limits 512 B … 2 MiB.
- **mft_record_size** / **index_record_size**: power-of-two multiples of the
  sector size; defaults `max(1024, sector)` and `max(4096, sector)`.

## Validation

The `#[cfg(test)]` harness in `ntfs_format.rs` and `ntfs_clone.rs` formats
every supported geometry and round-trips files back through our own reader;
these run in the normal `cargo test` suite (and therefore in the existing
release CI).

`scripts/ntfs-oracle.sh` is a **manual** developer tool — not wired into CI —
that cross-checks our output against the real, independent NTFS tooling
(`ntfsfix -n`, `ntfsinfo`, `mkntfs`). It needs `ntfs-3g` installed but the
read-only checks run unprivileged on a plain image file. Run
`scripts/ntfs-oracle.sh matrix` after any change to the formatter (e.g. before
a release) to confirm the output still mounts under ntfs-3g across the geometry
matrix.

## Geometry surface (CLI + clone)

- `rb-cli new --fs ntfs` formats a blank NTFS superfloppy. `--cluster-size`
  (e.g. `4K`, `64K`) and `--sector-size` (512/1024/2048/4096) select the
  geometry; both default automatically (`NtfsGeometry::for_volume_size`, then
  floored to the sector size). The requested size is rounded down to a whole
  cluster.
- The defragmenting clone `ntfs_clone::stream_defragmented_ntfs` **inherits the
  source volume's sector and cluster size** (via `NtfsGeometry::with_cluster_size`)
  rather than forcing a fixed 4 KiB/512 geometry, so a repacked volume stays
  geometry-compatible with the original. `defragmented_minimum_size` already
  sizes the target as a multiple of that inherited cluster.

## NTFS 1.2 (Windows NT 3.5 / 3.51 / 4.0)

`create_ntfs_version(.., NtfsVersion::V1_2)` writes the on-disk version NT 3.5 to 4.0 (before SP4)
mount; they refuse major version 3 with STATUS_WRONG_VOLUME. `rb-cli new volume ntfs
--ntfs-version 1.2`, `rb-cli build --ntfs-version 1.2` and Build Disk (whose filesystem list has an
"NTFS 1.2 (Windows NT 3.5 to 4.0)" entry) reach it, and the defragmenting clone keeps a source volume's version. Default output is
still 3.1, byte for byte as before.

### License discipline

As for 3.1, plus: no Microsoft NT source code was read (leaked trees exist; their license allows
nothing). Everything below is from the public NTFS documentation and black-box observation of
volumes NT 4.0 itself wrote.

### References observed

Three 2 GB disks with an NTFS logical partition written by NT 4.0: Terminal Server (Alpha),
Workstation (Alpha) and Workstation (PowerPC). All three report version 1.2. All three were
converted from FAT by NT setup rather than formatted (512-byte clusters at 2 GB, the MFT in the
middle of the volume, the mirror at `total/2 - 1`), so they settle the structures, not `format`'s
layout choices; the contiguous layout of the 3.1 formatter is kept.

### Observed and reproduced (all three agree unless noted)

| Item | NTFS 1.2 as NT 4.0 writes it |
|---|---|
| `FILE` header | update sequence array at 0x2A, first attribute at 0x30; no record-number field (3.1 has it at 0x2C) |
| `$STANDARD_INFORMATION` | 48 bytes: four times and the attribute flags; no owner, security id, quota or USN |
| Security | no `$Secure`. Each metafile carries a resident 104-byte `$SECURITY_DESCRIPTOR`: one granting SYSTEM and Administrators read on 0, 1, 2, 4, 6, 7, 8, 10, one granting read/write on 3, 9, 11-15. The root's is an 80-byte one granting Everyone full control (Terminal Server's setup replaces it) |
| Record 9 | `$Quota`: SI, `$FILE_NAME`, descriptor, empty `$DATA` |
| Records 11-15 | reserved: in use, no `$FILE_NAME`, link count 0, SI + descriptor + empty `$DATA` |
| `$Extend` | absent, so no `$ObjId`, `$Quota`, `$Reparse` or `$UsnJrnl` under it |
| `$UpCase` | no `$Info` stream |
| `$AttrDef` | 36000 bytes, identical on all three (`nt4_attrdef.bin`) |
| `$UpCase` table | NT writes the running build's table: Terminal Server and PowerPC agree, Workstation differs in 39 code points (Georgian, final sigma); the majority table is `nt4_upcase.bin` |
| `$FILE_NAME` sizes | allocated and real sizes filled in for the metafiles (3.1 leaves them 0) |
| User files | from record 16 (the MFT bitmap reserves 0-15; 3.x reserves 0-23) |
| Boot sector | same BPB as 3.1; media 0xF8; drive 0x80 with the 0x80 signature; MFT and index records 1024 / 4096 bytes. NT wrote 32 sectors/track, 64 heads and the partition's offset as hidden sectors; we write zeros there, as for 3.1 (they only matter to boot code, and the volume does not boot) |
| `$LogFile` | NT 4.0 wrote restart areas of log version 1.1 (page size = the CPU's: 8 KiB on Alpha, 4 KiB on PowerPC). We write the log 0xFF-filled, as for 3.1: no restart area is better than one naming log records that do not exist. Whether NT 4.0 initialises such a log as XP does is open until a real NT mounts one |
| Version flags | `$VOLUME_INFORMATION` 1.2, flags 0 (clean) |

Our formatter's records 0-15 were compared field by field with Workstation's: flags, link counts,
sequence numbers, header offsets, attribute types / names / residency, SI size and flags,
`$FILE_NAME` names, flags and namespace, every security descriptor byte for byte, and the version:
no differences.

### Geometry

512-byte sectors only (the formatter refuses others). The default cluster follows the same table
as 3.1, which is also NT 4.0's (512 B to 512 MiB, 1 KiB to 1 GiB, 2 KiB to 2 GiB, 4 KiB above).
A larger cluster asked for explicitly is allowed with a note: NT 4.0 cannot compress on clusters
over 4 KiB.

### Writing to a 1.2 volume

The writer (`ntfs.rs`) now builds records with the 1.2 header on a 1.2 volume (it used the 3.1
header, and put the update sequence number where a 1.2 record keeps its first attribute) and
hands out user records from 16. This also applies to volumes NT 4.0 formatted.

### Validated

- `cargo test`: 1.2 at 512 B / 1 KiB / 2 KiB / 4 KiB clusters, 16 MiB to a sparse 4 GiB; files and a
  directory written and read back; every record keeps the 1.2 header and a 48-byte SI; the clone
  keeps 1.2 (and 3.1); `resize_ntfs_in_place` grows a 1.2 volume, which then takes 30 MiB of new
  data; our fsck finds each clean.
- `scripts/ntfs-oracle.sh matrix12`: `ntfsfix -n` passes and `ntfsinfo` reports 1.2 for every cluster
  size at 8, 100 and 1024 MiB; `matrix` (3.1) still passes.
- The Terminal Server volume cloned with the defragmenting clone (4590 files, 373 directories)
  comes out 1.2; our fsck and ntfs-3g find it clean, and system files compare byte for byte.

### Not yet validated (required before merging)

A real NT mounting the volumes: NT 4.0 (x86 in QEMU, or PINT) and NT 3.5, each attaching a
formatted image, writing files and running `chkdsk X:`; again after `rb-cli resize` grows it; and
the cloned Terminal Server volume. Record the NT builds and results here.
