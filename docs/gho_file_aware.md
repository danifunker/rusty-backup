# GHO file-aware reconstruction

Status (2026-05-27):
- **Slice A** — typed body parsers + record-type predicates: **done**
  (commit `e8847d0`).
- **Slice B** — directory tree walker: **done** (commit `28d9ab1`).
- **Slice C** — FAT image emitter: **done** (commit `ae77e3e`).
- **Slice D** — unified streaming `GhoReader`: **done** (commit `922a035`).
- **Full-disk variant** — `0xae17`/`0xae04`/`0x0117` accepted: **done**
  (commit `94c7987`).
- **GUI wiring** — inspect tab + browse view stream natively: **done**
  (commits `fe358eb`, `df4078c`).

All file-aware GHO fixtures in the corpus now open in the GUI without
tempfiles. SECTOR-mode (raw block) GHOs already worked since the
CHD-style `GhoReader` refactor (`e308dd2`).

Goal: turn a file-aware GHO into a browsable FAT partition, sitting
alongside the SECTOR-mode `GhoReader` for the file-aware case. **Done.**

## Architecture

`GhoReader::open(path)` is the single entry point for all GHO
containers. Internally it dispatches on `image_type`:

- `Sector` + `compression = None` → `GhoReaderMode::Uncompressed`
- `Sector` + `compression in {Fast, High}` → `GhoReaderMode::Blocked`
  (block-indexed, single-block decode cache)
- `FileAware` → `GhoReaderMode::FileAware` (in-RAM virtual FAT image)

All three modes implement `Read + Seek + DataLen`. Spanning
(multi-file `.GHS` / `.GHO.NNN`) is handled by `SpanReader` under
the hood for all modes.

### File-aware virtual FAT image

Instead of writing an 8 GB tempfile, `GhoReader` builds a
`VirtualFatImage` in RAM:

1. Parse + walk the file-aware tree (~2-5 s for 22k-entry fixtures).
2. Compute FAT layout via `compute_fat_blank_layout` (extracted from
   `create_blank_fat`).
3. Write metadata (BPB, FAT tables, FSInfo, dir clusters) into a
   `SparseSink` — sector-granular sparse `Read+Write+Seek` store.
4. Call `FatFilesystem::create_file` with `ZeroReader` to allocate
   cluster chains without materialising content bytes.
5. Record each file's `(cluster_chain, content_record_offsets)` and
   evict file-cluster sectors from the sparse store.
6. At read time: metadata sectors come from the sparse map; data
   clusters decompress the appropriate `0x0002`/`0x0102` records from
   the GHO on demand via `GhoFileContentReader`.

Peak RAM for an 8 GB FAT32 reconstruction: ~tens of MB (reserved +
2x FAT + dir clusters + per-file metadata index).

### GUI integration

- **Inspect tab** (`src/gui/inspect_tab.rs`): `run_inspect` detects
  GHO/IMZ via `is_gho_path`/`is_imz_path` and opens them through
  `source_reader::open_read` (no tempfile). Regular images and
  devices keep the existing `File::try_clone` path.
- **Browse view** (`src/model/browse_session.rs`): `BrowseSession::open`
  sniffs `FE EF` magic and routes through `GhoReader::open`.
- **File picker** (`src/gui/mod.rs`): `prepare_disk_image_path` only
  tempfile-decompresses `.adz`/`.hdz` (gzip). GHO/IMZ/CHD pass
  through unchanged.
- `open_disk_image_for_reading(path)` — thin wrapper over
  `source_reader::open_read`, ready for callers that want
  `Box<dyn ReadSeek>` directly.

## Exploration tools

- `examples/gho_dump_records.rs` — hex-dump first N records.
- `examples/gho_record_histogram.rs` — type/marker histogram.
- `examples/gho_dump_tree.rs` — dir entries inline with
  name/attr/cluster/size.
- `examples/gho_scan_partitions.rs` — scan a fixture tree for
  image_type + partition_count + span count.

## Record types

| type     | body_len | meaning                                                  |
|----------|----------|----------------------------------------------------------|
| `0x0002` | 32768    | full 32 KiB cluster of file content                      |
| `0x0017` | 512      | boot sector (Ghost 7.5 partition-only / 11.5 full-disk)  |
| `0x0717` | 512      | boot sector (Ghost 11.5 partition-only)                  |
| `0xae17` | 512      | boot sector (Ghost 7.5 full-disk, disk-level header)     |
| `0x0117` | 512      | boot sector (Ghost 7.5 full-disk, partition copy)        |
| `0x0004` | 56       | dir entry — Ghost 7.5 "header section"                   |
| `0x0704` | 56       | dir entry — Ghost 11.5 "header section" (appears once)   |
| `0xae04` | 56       | dir entry — Ghost 7.5 full-disk header section           |
| `0x0104` | 56       | dir entry — all later entries on both 7.5 and 11.5       |
| `0x0102` | variable | file content (whole small file OR tail fragment)         |
| `0x0103` | 20       | per-file checksum: `[u32 cksum][u32 cksum_dup][12 zeros]`|
| `0x0118` | 16384    | unknown, Ghost 7.5 full-disk (1 occurrence per fixture)  |

Markers: `0x0000` (default), `0x95FD` (~10% on 11.5), `0xC01E`
(first 3 records on 11.5 header section).

The `0x07xx` codes are Ghost 11.5 equivalents of `0x00xx`. The
`0xae` high-byte tags the disk-level header section on 7.5 full-disk
mode.

## Directory walker algorithm

Implemented in `walk_file_aware_tree(reader, image) -> GhoFileAwareTree`.
Depth-first pre-order descent matching Ghost's writer:

1. `current_dir_cluster` initialised from boot sector's BPB_RootClus.
2. `.` entry sets `current_dir_cluster`.
3. `..` entry back-fills parent on the most-recent pending DIR entry.
4. FILE entries use `current_dir_cluster` as parent.
5. LFN slots buffer before each 8.3 and attach as `long_name`.
6. Empty/deleted slots ignored.

**Known limitation**: implicit-pop ambiguity for FILE parent
attribution. Not blocking; the tree is internally consistent.

## Full-disk variant

Ghost 7.5's full-disk file-aware mode (`FULLDISK.GHO`,
`HPVectra95C.gho`, `fromdanilaptop.GHO`, `XP_SP2FU.GHO`) is
structurally identical to single-partition file-aware — just uses
`0xae` high-byte for the header section. After accepting the new
codes, the walker + emitter handle these transparently.

Ghost 11.5's full-disk (`GH11/fulldisk.GHS`) uses ordinary `0x0017`
and works without changes.

### Corpus scan results (2026-05-27)

All `ManualGhostBackups/` fixtures are the same source disk with
different Ghost versions/modes. Scan via `gho_scan_partitions`:

| fixture                  | type      | #p | spans | comp | notes          |
|--------------------------|-----------|----|-------|------|----------------|
| HPVectra95C.gho          | FileAware | 1  | 1     | Fast | Win95 disk     |
| 7.5/FULLDISK.GHO         | FileAware | 2  | 1     | None | 7.5 full-disk  |
| 7.5/PART.GHO             | FileAware | 1  | 1     | None | 7.5 partition  |
| 7.5/PART-HI.GHO          | FileAware | 1  | 1     | High | 7.5 part+high  |
| 7.5/FAST.GHO             | FileAware | 1  | 1     | Fast | 7.5 part+fast  |
| 7.5/FD-HIGH.GHO          | FileAware | 1  | 1     | High | 7.5 FD+high    |
| 7.5/SECTOR.GHO           | Sector    | 0  | 4     | None | 7.5 raw+spans  |
| 11.5/gh11part.GHO        | FileAware | 1  | 1     | None | 11.5 part, PWD |
| 11.5/partcom.GHO         | FileAware | 1  | 1     | High | 11.5 part, PWD |
| 11.5/High.GHO            | FileAware | 1  | 1     | High | 11.5 part      |
| 11.5/gh11-spl.GHO        | FileAware | 1  | 12    | None | 11.5 spans     |
| 11.5/secthigh.GHO        | Sector    | 0  | 1     | High | 11.5 raw       |
| 11.5/fulldisk.GHS        | FileAware | 2  | 1     | None | 11.5 full-disk |
| 11.5/GH11PW.GHO          | FileAware | 1  | 1     | None | 11.5 part, PWD |
| 11.5/hipwd.GHO           | FileAware | 1  | 6     | High | 11.5 spans,PWD |
| 11.5/gh11pwd.GHO         | FileAware | 1  | 12    | None | 11.5 spans,PWD |
| Unknown/fromdanilaptop   | FileAware | ?  | 6     | High | unknown origin |
| Split/Win7_86xAMB.GHO    | FileAware | 0  | 1*    | High | 0 recs parsed* |
| XP_SP2FU.GHO             | FileAware | 2  | 2     | High | XP disk        |

`*` Win7_86xAMB.GHO uses `.GHO.NNN` numeric spans not discovered by
the single-file scan; may need the full span set to parse.

## What's NOT in scope

- **NTFS file-aware**: no fixtures exist. Would need a separate
  reconstructor if one turns up.
- **Password-protected GHOs**: blocked on cipher RE
  (`docs/gho_password.md`).
- **Multi-partition full-disk**: no fixture with >1 partition exists
  in the corpus. The walker handles the first partition; if a real
  multi-partition fixture appears, per-partition record-run splitting
  would need to be added.

## Open questions

1. **Implicit-pop ambiguity for FILE parent attribution.** Resolvable
   with Ghost Explorer extract or mounting the reconstructed image.
2. **`0x0103` checksum algorithm.** CRC-32 vs Adler-32 vs custom.
   Not blocking reconstruction.
3. **`0x95FD` and `0xC01E` marker semantics.** Not blocking.
4. **`0x0118` record (16 KiB, 1 per 7.5 full-disk).** Unknown purpose.
5. **Win7_86xAMB.GHO**: 0 records parsed — likely needs its full
   `.GHO.NNN` span set to parse.
