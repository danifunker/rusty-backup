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

`scripts/ntfs-oracle.sh` and the `#[cfg(test)]` harness in `ntfs_format.rs`
cross-check every formatted geometry with the real NTFS tooling
(`ntfsfix -n`, `ntfsinfo`) and field-diff the geometry-dependent BPB/record
fields against an equivalent `mkntfs` image, then round-trip files through both
our own reader and `ntfs-3g`. The same `scripts/ntfs-oracle.sh matrix` runs in
CI on any change to the NTFS sources
(`.github/workflows/ntfs-oracle.yml`, unprivileged on `ubuntu-latest`).

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
