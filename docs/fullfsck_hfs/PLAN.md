# HFS fsck — Full Implementation Plan

15 missing filesystem checks identified by comparing our `src/fs/hfs_fsck.rs` against
Apple's `fsck_hfs` source. Organized into 6 stages, each completable in a single session.

## Gap Summary

| # | Gap | Apple error code(s) | Severity |
|---|-----|---------------------|----------|
| 1 | LEOF > PEOF | E_PEOF / E_LEOF | High |
| 2 | File CNID range (3–15 reserved) | E_InvalidID | Medium |
| 3 | Reserved fields in file records ≠ 0 | E_CatalogFlagsNotZero | Low |
| 4 | Alternate MDB validation | E_MDBDamaged | High |
| 5 | drNmAlBlks vs calculated total blocks | E_NABlks | Medium |
| 6 | drAlBlSt minimum position | E_ABlkSt | Medium |
| 7 | drVBMSt >= 3 | E_VBMSt | Medium |
| 8 | Block size upper bound (0x7FFFFE00) | E_ABlkSz | Low |
| 9 | Clump size validation | (various) | Low |
| 10 | Catalog name validation | E_CName / E_BadFileName | Medium |
| 11 | Thread key name must be zero-length | E_ThdKey | Low |
| 12 | Directory loop detection | E_DirLoop | High |
| 13 | Catalog nesting depth > 100 | E_CatDepth | Low |
| 14 | Embedded HFS+ wrapper check | E_InvalidWrapperExtents | Medium |
| 15 | Bad block file bitmap cross-check | (bitmap) | Low |

---

## Stage 1: Extended MDB Validation (Gaps 5, 6, 7, 8, 9)

**Scope**: Pure `check_mdb` additions. No struct changes, no API changes.
Read raw MDB fields from `mdb.raw_sector`.

### New `HfsFsckCode` variants

- `MdbNmAlBlksMismatch` — total_blocks sanity (Gap 5)
- `MdbAlBlStTooLow` — first alloc block start too low (Gap 6)
- `MdbVBMStTooLow` — VBM start < 3 (Gap 7)
- `BlockSizeUpperBound` — block_size > 0x7FFFFE00 (Gap 8)
- `InvalidClumpSize` — clump not multiple of block_size or too large (Gap 9)

### Changes

**`check_mdb`** (line 1718): Add 5 validation blocks after the existing VBM collision check:

1. **Gap 7**: `volume_bitmap_block < 3` → error. Sectors 0–1 are boot blocks, sector 2
   is the MDB itself. The VBM cannot start before sector 3.

2. **Gap 6**: `first_alloc_block < volume_bitmap_block + vbm_sectors` → error. The data
   area must start after the VBM ends.

3. **Gap 8**: `block_size > 0x7FFF_FE00` → error. Apple's documented maximum allocation
   block size.

4. **Gap 5**: `total_blocks == 0` → error. Also check
   `total_blocks as u64 * block_size as u64 > u32::MAX` → warning (HFS 4 GB limit).

5. **Gap 9**: Read `drClpSiz` from `raw_sector[24..28]`, `drXTClpSiz` from
   `raw_sector[72..76]`, `drCTClpSiz` from `raw_sector[76..80]`. Each must be:
   - A multiple of block_size (or zero)
   - ≤ total_blocks × block_size
   Warn if zero (unusual), error if not a multiple.

### Repairability

All 5 are non-repairable in `is_repairable`.

### MDB raw_sector offset reference

| Offset | Size | Field |
|--------|------|-------|
| 14..16 | u16 | drVBMSt (VBM start sector) |
| 18..20 | u16 | drNmAlBlks (total alloc blocks) |
| 20..24 | u32 | drAlBlkSiz (alloc block size) |
| 24..28 | u32 | drClpSiz (default clump size) |
| 28..30 | u16 | drAlBlSt (first alloc block start sector) |
| 72..76 | u32 | drXTClpSiz (extents overflow file clump size) |
| 76..80 | u32 | drCTClpSiz (catalog file clump size) |

### Tests (7)

- `test_mdb_vbm_start_too_low` — set VBMSt to 1, expect `MdbVBMStTooLow`
- `test_mdb_alblst_too_low` — set first_alloc_block to 1, expect `MdbAlBlStTooLow`
- `test_mdb_block_size_upper_bound` — set block_size to 0x80000000, expect `BlockSizeUpperBound`
- `test_mdb_block_size_at_max_valid` — set block_size to 0x7FFFFE00, expect no error
- `test_mdb_clump_size_not_multiple` — set raw_sector clump to non-multiple, expect `InvalidClumpSize`
- `test_mdb_clump_size_valid` — set all three clump sizes to valid multiples, expect no error
- `test_mdb_total_blocks_zero` — set total_blocks to 0, expect error

---

## Stage 2: Catalog Record Field Validation (Gaps 1, 2, 3)

**Scope**: Expand `CatalogEntry::File` to capture more fields, validate them in
`check_catalog_consistency`.

### New `HfsFsckCode` variants

- `LeoFExceedsPeoF` — logical size > physical size (Gap 1)
- `InvalidCnidRange` — CNID in reserved range 3–15 (Gap 2)
- `ReservedFieldNonZero` — dataStartBlock/rsrcStartBlock/reserved byte ≠ 0 (Gap 3)

### Struct changes

`CatalogEntry::File` (line 2479): Add fields:

```rust
data_physical_size: u32,   // filPyLen at rec[30..34]
rsrc_physical_size: u32,   // filRPyLen at rec[40..44]
data_start_block: u16,     // filStBlk at rec[24..26]
rsrc_start_block: u16,     // filRStBlk at rec[34..36]
reserved_byte: u8,         // cdrResrv at rec[1]
```

### HFS File Record layout reference

| Offset | Size | Field |
|--------|------|-------|
| 0 | 1 | cdrType (2 = file) |
| 1 | 1 | cdrResrv (MUST be 0) |
| 20..24 | 4 | filFlNum (CNID) |
| 24..26 | 2 | filStBlk (dataStartBlock — MUST be 0) |
| 26..30 | 4 | filLgLen (data logical size) |
| 30..34 | 4 | filPyLen (data physical size) |
| 34..36 | 2 | filRStBlk (rsrcStartBlock — MUST be 0) |
| 36..40 | 4 | filRLgLen (rsrc logical size) |
| 40..44 | 4 | filRPyLen (rsrc physical size) |
| 74..86 | 12 | data extents (3 × 4 bytes) |
| 86..98 | 12 | rsrc extents (3 × 4 bytes) |

### Changes

1. **`parse_catalog_record`** (line 2801, `CATALOG_FILE` arm): Read 5 new fields from
   the record bytes and add them to `CatalogEntry::File`.

2. **`check_catalog_consistency`** (line 2527 loop): Add three checks:

   - **Gap 2 (CNID range)**: `file_id < 16` for files, or `dir_id < 16 && dir_id != 2`
     for directories → `InvalidCnidRange` error.

   - **Gap 1 (LEOF > PEOF)**: `data_size > data_physical_size` or
     `rsrc_size > rsrc_physical_size` → `LeoFExceedsPeoF` error. Uses the already-stored
     physical size fields directly from the catalog record (no extent summation needed).

   - **Gap 3 (Reserved fields)**: `reserved_byte != 0`, `data_start_block != 0`, or
     `rsrc_start_block != 0` → `ReservedFieldNonZero` warning.

### Repairability

- `LeoFExceedsPeoF`: non-repairable (data corruption)
- `InvalidCnidRange`: non-repairable
- `ReservedFieldNonZero`: repairable (can zero them out)

### Tests (7)

- `test_leof_exceeds_peof_data_fork` — data_size > physical_size
- `test_leof_within_peof_ok` — data_size ≤ physical_size, no error
- `test_cnid_below_16_file` — file with CNID 5
- `test_cnid_below_16_dir` — directory with CNID 10 (not 2)
- `test_cnid_2_root_ok` — root dir CNID 2 should not trigger error
- `test_reserved_fields_nonzero` — dataStartBlock = 1, expect warning
- `test_reserved_fields_zero_ok` — all zero, no warning

---

## Stage 3: Catalog Name & Thread Key Validation (Gaps 10, 11)

**Scope**: Validate file/folder names and thread record key names during the catalog scan.

### New `HfsFsckCode` variants

- `InvalidCatalogName` — name empty, > 31 bytes, or contains `:` / null (Gap 10)
- `ThreadNameNotEmpty` — thread key has non-empty name (Gap 11)

### Changes

1. **New helper function**:

   ```rust
   fn validate_hfs_name(name: &[u8]) -> Option<String>
   ```

   Returns `Some(problem_description)` if invalid. Checks:
   - Length must be 1..=31 bytes (0 is invalid for file/dir records)
   - No colon (`:`, byte 0x3A) — HFS path separator, forbidden in names
   - No null bytes (0x00)

2. **`CatalogEntry::DirThread` / `FileThread`** (line 2487): Add `key_name_len: usize`.

3. **`parse_catalog_record`** (thread arms): Store `name_len` as `key_name_len` in the
   thread entry.

4. **`check_catalog_consistency`**: For file/dir entries call `validate_hfs_name` on
   `key.name`; for thread entries check `key_name_len != 0`.

### Repairability

- `InvalidCatalogName`: non-repairable (name corruption)
- `ThreadNameNotEmpty`: repairable (zero the key name length)

### Tests (6)

- `test_catalog_name_too_long` — 32-byte name
- `test_catalog_name_empty` — 0-byte name on a file record
- `test_catalog_name_contains_colon` — name with `:`
- `test_catalog_name_valid` — normal 1–31 byte name, no error
- `test_thread_key_name_zero_ok` — thread with empty key name
- `test_thread_key_name_nonzero` — thread with non-empty key name, expect error

---

## Stage 4: Directory Structure Checks (Gaps 12, 13)

**Scope**: Walk parent chains to detect loops and deep nesting. Operates on the
`dir_records` map already built in `check_catalog_consistency`.

### New `HfsFsckCode` variants

- `DirectoryLoop` — cycle in parent chain (Gap 12)
- `NestingDepthWarning` — nesting > 100 levels (Gap 13)

### Changes

1. **New function**:

   ```rust
   fn check_directory_structure(
       dir_records: &HashMap<u32, (u32, Vec<u8>, u16)>,
       errors: &mut Vec<FsckIssue>,
       warnings: &mut Vec<FsckIssue>,
   )
   ```

   For each directory in `dir_records`, walk the `parent_id` chain toward root
   (CNID 1 or 2):
   - Track visited CNIDs in a `HashSet` per walk; revisit → `DirectoryLoop` error
   - Count depth; > 100 → `NestingDepthWarning` warning
   - Cap traversal at 200 hops as safety valve
   - Cache results: once a directory reaches root at depth N, reuse for children

2. **`check_catalog_consistency`**: Call `check_directory_structure` after the existing
   valence checks (~line 2716), passing `&dir_records`.

### Repairability

Both non-repairable.

### Tests (4)

- `test_directory_loop_detected` — A → B → A cycle
- `test_deep_nesting_warning` — 101-level chain, expect warning
- `test_normal_nesting_ok` — 5-level chain, no warning
- `test_directory_reaches_root` — normal hierarchy to CNID 2, no error

---

## Stage 5: Alternate MDB & Embedded HFS+ Wrapper (Gaps 4, 14)

**Scope**: **API change** to `check_hfs_integrity`. Caller in `hfs.rs` updated to read
the alt MDB sector and pass partition size.

### New `HfsFsckCode` variants

- `AlternateMdbMismatch` — primary vs alt MDB field differs (Gap 4)
- `AlternateMdbMissing` — alt MDB has wrong signature (Gap 4)
- `EmbeddedHfsPlusInvalid` — bad wrapper extents or unknown embedded signature (Gap 14)

### API change

```rust
pub(crate) fn check_hfs_integrity(
    mdb: &HfsMasterDirectoryBlock,
    catalog_data: &[u8],
    bitmap: &[u8],
    extents_data: Option<&[u8]>,
    alt_mdb_sector: Option<&[u8; 512]>,  // NEW
    partition_size: Option<u64>,          // NEW
) -> FsckResult
```

### Changes

1. **`hfs.rs` `fsck()` method** (~line 560): Read the last 512-byte sector of the
   partition as the alternate MDB. Pass it + partition size to `check_hfs_integrity`.

2. **New function** `check_alternate_mdb(mdb, alt_sector, errors, warnings)`:
   - Verify alt signature == 0x4244. If wrong → `AlternateMdbMissing` warning.
   - Cross-check critical fields against primary MDB: `block_size`, `total_blocks`,
     `first_alloc_block`, `volume_bitmap_block`. Mismatch → `AlternateMdbMismatch` error
     per differing field.

3. **New function** `check_embedded_hfs_plus(mdb, partition_size, errors, warnings)`:
   - Check `raw_sector` for embedded signature 0x482B at offset 124.
   - If present: validate `embedded_start_block + embedded_block_count <= total_blocks`.
     Out of range → `EmbeddedHfsPlusInvalid` error.
   - If unknown non-zero embedded signature → `EmbeddedHfsPlusInvalid` error.

4. **Integration**: Call both after Phase 1 in `check_hfs_integrity`.

### Repairability

- `AlternateMdbMismatch`: repairable (copy primary → alt)
- `AlternateMdbMissing`: repairable (write primary as alt)
- `EmbeddedHfsPlusInvalid`: non-repairable

### Tests (6)

- `test_alternate_mdb_matches` — identical primary and alt, no error
- `test_alternate_mdb_block_size_mismatch` — differing block_size, expect error
- `test_alternate_mdb_missing_signature` — alt sector all zeros, expect warning
- `test_embedded_hfs_plus_valid_wrapper` — sig 0x482B within bounds, expect info warning
- `test_embedded_hfs_plus_out_of_range` — start + count > total_blocks, expect error
- `test_embedded_unknown_signature` — sig 0x1234, expect error

---

## Stage 6: Bad Block File Bitmap Cross-Check (Gap 15)

**Scope**: Account for bad block file (CNID 5) extents from the extents overflow B-tree
in the bitmap verification.

### New `HfsFsckCode` variants

- `BadBlockExtentNotInBitmap` — bad block extent marked free in on-disk bitmap (Gap 15)

### Changes

1. **New function**:

   ```rust
   fn collect_bad_block_extents(extents_data: &[u8]) -> Vec<(u32, u32)>
   ```

   Walk the extents overflow B-tree, return only file_id == 5 extents as
   `(start_block, block_count)`.

2. **`check_extents_and_bitmap`** (line 2975): After the bitmap comparison loop
   (line 3076–3105), call `collect_bad_block_extents`. For each bad block extent, verify
   the blocks ARE marked as allocated in the on-disk bitmap. If any are free →
   `BadBlockExtentNotInBitmap` warning.

   Note: bad block extents are already included in `computed_bitmap` via
   `collect_overflow_extents` (which collects all file IDs). This new check specifically
   verifies the on-disk bitmap agrees for bad block extents.

### Repairability

Repairable (set the bitmap bits).

### Tests (3)

- `test_bad_block_extents_in_bitmap_ok` — bad block extents marked allocated, no error
- `test_bad_block_extent_not_in_bitmap` — bad block at block 50 but bitmap free, expect warning
- `test_no_bad_block_file_ok` — no file_id 5 extents, no error

---

## Dependency Graph & Recommended Order

```
Stage 1 (MDB checks)         ─┐
Stage 2 (Record fields)       │── independent, any order
Stage 3 (Name validation)     │
Stage 4 (Directory structure) ─┘
Stage 5 (Alt MDB + wrapper)  ─── API change, do after 1–4
Stage 6 (Bad block bitmap)   ─── logically last, most niche
```

**Recommended**: 1 → 2 → 3 → 4 → 5 → 6

## Totals

| Stage | Gaps | New codes | Tests | Key change |
|-------|------|-----------|-------|------------|
| 1 | 5, 6, 7, 8, 9 | 5 | 7 | `check_mdb` additions |
| 2 | 1, 2, 3 | 3 | 7 | `CatalogEntry::File` expansion + validation |
| 3 | 10, 11 | 2 | 6 | Name validation + thread key check |
| 4 | 12, 13 | 2 | 4 | Parent chain traversal |
| 5 | 4, 14 | 3 | 6 | **API change** + alt MDB + wrapper |
| 6 | 15 | 1 | 3 | Bad block bitmap cross-check |
| **Total** | **15** | **16** | **33** | |

## Key Files

- `src/fs/hfs_fsck.rs` — all checks, repairs, and tests
- `src/fs/hfs.rs` — `HfsMasterDirectoryBlock` struct, `fsck()` caller (Stage 5)
- `src/fs/hfs_common.rs` — `BTreeHeader`, bitmap helpers
- `src/fs/fsck.rs` — `FsckResult`, `FsckIssue`, `FsckStats` shared types
