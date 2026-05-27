# NTFS File-Aware GHO: Implementation Resume

Use this document to resume work on NTFS file-aware Ghost backup support.
The uncompressed path is complete; the next task is adding compressed
NTFS support when a fixture arrives.

## What was built (commits 63a57af, 84e3d26, 7e29fda, b9f8924)

Support for NTFS partitions inside Norton Ghost `.GHO` file-aware backups.
All code is in `src/rbformats/gho.rs` (bottom, before `#[cfg(test)]`) and
`src/fs/ntfs.rs` (visibility changes only).

### Files changed
- `src/rbformats/gho.rs` — ~450 lines of new code: detection, GHPR VBR
  extraction, cluster run indexing, read path, MFT queue fallback
- `src/fs/ntfs.rs` — made 8 items `pub(crate)`: `NtfsVbr`, `parse_vbr`,
  `MftAttribute`, `DataRun`, `decode_data_runs`, `parse_mft_attributes`,
  `apply_fixup`, `ATTR_DATA`
- `examples/test_ntfs_gho.rs` — manual test harness
- `docs/gho_file_aware.md` — format documentation

### Key types and functions in gho.rs

- `NTFS_RUN_HEADER_ENTRY1` — 16-byte constant prefix of every cluster run header
- `NTFS_RUN_HEADER_SIZE` — 42 bytes total (16 + 16 + 10)
- `NtfsGhoClusterRun` — `{lcn_start, cluster_count, file_offset}`
- `NtfsGhoIndex` — `{runs: Vec<NtfsGhoClusterRun>, volume_size, cluster_size, vbr}`
- `GhoReaderMode::NtfsFileAware` — new enum variant with `index` + `last_run_hint`
- `find_ntfs_vbr_in_header()` — scans GHPR region for NTFS VBR
- `parse_ntfs_run_header()` — extracts cluster count from 42-byte header
- `parse_inline_mft_records()` — extracts data runs from FILE records in gaps
- `index_ntfs_file_aware()` — the main 3-phase index builder
- `try_open_ntfs_file_aware()` — orchestrator called from `GhoReader::open`
- `read_ntfs_file_aware_into()` — LCN binary search + seek + read

### Detection path in GhoReader::open (~line 1102)

```
if FileAware {
    if find_inner_stream_start() fails (no FAT record magic 0x012F18D8) {
        if compression != None {
            bail("compressed NTFS not yet supported")  // <-- THIS IS NEXT
        }
        try_open_ntfs_file_aware()  // finds VBR, indexes runs
    } else {
        existing FAT file-aware path (parse_gho_image, walk tree, etc.)
    }
}
```

## NTFS file-aware binary format (reverse-engineered)

Completely different from FAT file-aware. No record stream (no 0x012F18D8
magic except a single End record at EOF).

### File layout
```
[0x000] Container header (FE EF, 12 bytes + optional password/description)
[0x200] Zero padding
[0x600] Compressed/packed metadata (purpose unclear, ~3KB non-zero)
[0x800] More metadata with "HB4", "DTA" tags (purpose unclear)
[~0x1400] Second FE EF sub-header (version byte 0x04, not 0x01)
[~0x1600] GHPR metadata blocks (tagged TLV-ish entries)
          Contains the NTFS VBR (512 bytes with "NTFS    " OEM + 55 AA)
[~0x1A9C] RPHG end marker, zero padding
[~0x2268] First cluster run (always $MFT)
  ... cluster runs with inline MFT records in gaps ...
[EOF-34]  Single GHO End record (type 0x0023, magic 0x012F18D8, 24-byte body)
```

### 42-byte cluster run header (constant structure)
```
Bytes  0-15: 0E 20 80 00 00 00 00 00 00 00 00 00 00 00 00 0F  (entry 1, constant)
Bytes 16-31: 0E 02 00 00 00 00 00 00 00 00 00 00 00 00 00 0F  (entry 2, constant)
Bytes 32-41: 0F <u64_le cluster_count> 0E                       (entry 3, variable)
```

### Between cluster runs (variable-length gaps)
- Zero or more MFT FILE records (1024 bytes each, standard NTFS format)
- These describe files whose data follows in subsequent cluster runs
- MFT records appear in ascending MFT-record-number order
- But cluster data appears in a DIFFERENT order (larger runs first within a batch)
- Some runs have NO gap (immediately follow the previous run's data)

### Corpus fixture
- `~/new-fixtures/gho/ManualGhostBackups/NTFS/smallNTFS.GHO`
- 1,825,953,213 bytes (1.7 GiB), uncompressed, file-aware
- Source: 10 GiB NTFS partition, Windows XP with dirty journal
- Contents: `$Extend`, `flynntech12.iso` (735 MB), `PermTest/`,
  `System Volume Information/`, `WINDOWS/` (partially copied XP)
- NTFS geometry: 512 bytes/sector, 8 sectors/cluster (4096 cluster size),
  20,964,761 total sectors, MFT at LCN 786432
- 5613 cluster runs, 442,903 stored clusters, 6048 MFT records

## 3-phase LCN mapping algorithm

### Phase 1: Read MFT (run 0)
- First run header at ~0x2268 is always $MFT, LCN from VBR field
- Read all MFT records, extract ALL non-resident attribute data runs
  (not just $DATA — also $INDEX_ALLOCATION, $SECURITY_DESCRIPTOR, $BITMAP, etc.)
- Skip $MFT itself (record 0, already mapped) and $BadClus (record 8, sparse)
- Accept LCN 0 (`cluster_offset >= 0`) — $Boot legitimately starts at LCN 0
- Produces `data_run_queue: Vec<(lcn, cluster_count)>`, ~6103 entries

### Phase 2: Scan file for run headers + inline MFT matching
- Scan entire file for the 16-byte run header needle in 4MB chunks
- For each gap between runs, call `parse_inline_mft_records()` to extract
  data runs from FILE records in the gap — appended to `pending_lcns`
- Match each cluster run to a pending LCN by cluster count using
  **`rposition`** (last match, not first) — Ghost stores MFT records in
  ascending order but writes cluster data largest-first within each batch
- Runs without a matching pending LCN are collected in `unmapped_runs`

### Phase 3: MFT queue fallback for unmapped runs
- Build a set of already-mapped LCNs
- Filter `data_run_queue` to exclude already-mapped entries
- Match unmapped runs by cluster count against the filtered queue (rposition)
- In practice: only 1 run needed this fallback (the $MFT $BITMAP at LCN 786431)

### Result: 100% coverage (5613/5613 runs)

## Compressed NTFS file-aware (DONE)

### Compressed format (reverse-engineered from `ntfscomp.GHO`)

The compressed format does NOT use `[u16 stored_len]` framing. Instead,
it wraps the same decompressed data layout (run headers + MFT records +
cluster data) in back-to-back zlib streams separated by 2-byte gaps:

```
[2-byte gap][78 01 zlib block][2-byte gap][78 01 zlib block]...
```

Each zlib block (CMF=`0x78`, FLG=`0x01`) decompresses to:
- 10 bytes (run header entry 3)
- 16 bytes (run header entries 1 and 2)
- 1024 bytes (MFT FILE records)
- 4096–32768 bytes (cluster data)

The decompressed stream is byte-for-byte identical in structure to the
uncompressed format: same 42-byte run headers, same inline MFT records,
same cluster data ordering.

### Implementation

- `NtfsCompressedBlock` — block map entry: file offset, compressed size,
  decompressed offset, decompressed size
- `NtfsCompressedState` — block map + single-block decompression cache
- `scan_ntfs_compressed_blocks()` — builds the block map by scanning for
  `78 01` zlib headers in 4 MB chunks. Uses `flate2::Decompress` directly
  (not `ZlibDecoder`) for precise byte-count tracking.
- `NtfsDecompressingReader` — `Read + Seek` adapter over the block map.
  Binary-searches the block map, decompresses on demand with a single-block
  cache. Used during indexing so `index_ntfs_file_aware` works unchanged.
- `read_ntfs_file_aware_into` — extended with `compressed: &mut Option<NtfsCompressedState>`
  parameter. When present, routes reads through `NtfsDecompressingReader`.

### Corpus fixture
- `~/new-fixtures/gho/ManualGhostBackups/NTFS/ntfscomp.GHO`
- 821,594,548 bytes (783 MB), High (zlib) compressed, file-aware
- Source: same 10 GiB NTFS partition as `smallNTFS.GHO`
- 100,892 compressed blocks, 1741.3 MB decompressed
- 5613 cluster runs, 442,903 stored clusters (identical to uncompressed)
- Block scan: ~3 seconds on M1, indexing: ~2 seconds

### Key insight: `flate2::Decompress` vs `ZlibDecoder`
`ZlibDecoder::into_inner().len()` is unreliable for determining how many
compressed bytes were consumed, because `ZlibDecoder` wraps a `BufReader`
that reads ahead. Using `flate2::Decompress` directly gives exact
`total_in()` / `total_out()` counts.

## Testing

### Manual test (examples/test_ntfs_gho.rs)
```bash
# Uncompressed
cargo run --release --example test_ntfs_gho -- ~/new-fixtures/gho/ManualGhostBackups/NTFS/smallNTFS.GHO

# Compressed
cargo run --release --example test_ntfs_gho -- ~/new-fixtures/gho/ManualGhostBackups/NTFS/ntfscomp.GHO
```

### CLI tests
```bash
# Inspect (both files produce identical partition info)
rb-cli inspect ~/new-fixtures/gho/ManualGhostBackups/NTFS/smallNTFS.GHO
rb-cli inspect ~/new-fixtures/gho/ManualGhostBackups/NTFS/ntfscomp.GHO

# List root (both produce identical output)
rb-cli ls ~/new-fixtures/gho/ManualGhostBackups/NTFS/smallNTFS.GHO@1 /
rb-cli ls ~/new-fixtures/gho/ManualGhostBackups/NTFS/ntfscomp.GHO@1 /

# List subdirectory
rb-cli ls ~/new-fixtures/gho/ManualGhostBackups/NTFS/ntfscomp.GHO@1 /WINDOWS
```

### Expected output for root listing (both files)
```
DIR  $Extend                    0 bytes
FILE flynntech12.iso    734781440 bytes
DIR  PermTest                   0 bytes
DIR  System Volume Information  0 bytes
DIR  WINDOWS                    0 bytes
```
