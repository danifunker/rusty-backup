# Output Formats Module (`src/rbformats/`)

Compress/decompress handlers for backup output formats, plus disk reconstruction for restore and VHD export.

## Architecture

- **`mod.rs`** — Orchestration functions (`compress_partition`, `decompress_to_writer`, `reconstruct_disk_from_backup`) and shared utilities (`SplitWriter`, `write_zeros`, `is_all_zeros`, `output_path`, `file_name`, `CHUNK_SIZE`).
- **`vhd.rs`** — VHD format: Fixed (`build_vhd_footer`, `export_whole_disk_vhd`, `export_partition_vhd`) and Dynamic/sparse (`DynamicVhdReader`, `export_whole_disk_vhd_dynamic`). Shared: `vhd_chs_geometry`, `vhd_checksum`, `VHD_COOKIE`.
- **`sparse.rs`** — Shared sparse-allocation bookkeeping (`SparseAllocator`, `is_zero_unit`) reused by dynamic VHD (and later QCOW2, VMDK sparse).
- **`qcow2.rs`** — QCOW2: `Qcow2Reader` (v2/v3, read + in-place allocate-on-write edit) and `export_qcow2` (writes v3 with `refcount_order = 4`, single-file, uncompressed clusters). Self-referential refcount-block sizing keeps `qemu-img check` clean; the editor grows L2 tables + refcount blocks on demand and reuses free host clusters before extending the file.
- **`vmdk.rs`** — VMDK flat: `VmdkFlatReader` (parses the ASCII descriptor, concatenates `FLAT`/`ZERO` extents from `monolithicFlat` and `twoGbMaxExtentFlat` layouts), `export_vmdk_flat` (writes a single-extent `monolithicFlat`: `<base>.vmdk` descriptor + `<base>-flat.vmdk` raw extent), and `open_flat_extent_for_edit` (resolves the single FLAT extent and returns it `R+W` for in-place edit; rejects multi-extent / ZERO / sparse layouts). Sparse VMDKs (`KDMV` magic) detected by the flat reader are rejected with a precise message.
- **`vmdk_sparse.rs`** — VMDK sparse (`monolithicSparse`): `VmdkSparseReader` parses the binary `KDMV` `SparseExtentHeader`, loads the grain directory into RAM, lazily reads grain tables with a single-entry LRU (mirrors `Qcow2Reader`'s L2 cache and `ChdReader`'s hunk cache), and serves `Read + Seek` over the virtual disk; unallocated grains read as zeros. Compressed / marker (`streamOptimized`) variants are detected and rejected — they're not seekable. Writer + edit land in sessions 4.2 / 4.3.
- **`chd.rs`** — CHD (MAME) format via the in-process `libchdman-rs` crate: `compress_chd`, `compress_chd_dvd`, `ChdReader`, `CdCookedReader`.
- **`zstd.rs`** — Zstd streaming compression: `compress_zstd`.
- **`raw.rs`** — Raw streaming with optional file splitting and sparse zero-skipping: `stream_with_split`.

## Available Formats

| Format | Extension | Compression | Splitting | Notes |
|--------|-----------|-------------|-----------|-------|
| Raw    | `.raw`    | None        | Yes       | Supports sparse zero-skipping |
| VHD (Fixed)   | `.vhd` | None  | No        | Raw data + 512-byte footer |
| VHD (Dynamic) | `.vhd` | None  | No        | Sparse: BAT + per-block bitmap; all-zero blocks omitted |
| QCOW2         | `.qcow2` | None | No       | Read (v2 + v3) + write (v3 uncompressed). Sparse: zero clusters omitted. `qemu-img check` clean. |
| VMDK (Flat)   | `.vmdk` | None | No       | Read (`monolithicFlat` / `twoGbMaxExtentFlat`, FLAT + ZERO extents). Write: `monolithicFlat` — descriptor + sibling `-flat.vmdk` raw extent. |
| VMDK (Sparse) | `.vmdk` | None | No       | Read-only (`monolithicSparse`, `KDMV` magic). Two-level grain map; unallocated grains read as zeros. Writer + edit in Phase 4. |
| Zstd   | `.zst`    | Zstd level 3 | Yes (post-hoc) | Good compression ratio |
| CHD    | `.chd`    | MAME CHD    | Yes (post-hoc) | Native via `libchdman-rs` (no external tool) |

## How to Add a New Output Format

1. Create `src/rbformats/myformat.rs` with a compression function:
   ```rust
   pub(crate) fn compress_myformat(
       reader: &mut impl Read,
       output_base: &Path,
       split_size: Option<u64>,
       progress_cb: &mut impl FnMut(u64),
       cancel_check: &impl Fn() -> bool,
   ) -> Result<Vec<String>>
   ```

2. Add a decompression path in `decompress_to_writer()` in `mod.rs` (match on `compression_type` string from metadata).

3. Add a variant to the `CompressionType` enum in `backup/mod.rs` with `as_str()` and `file_extension()` methods.

4. Add routing case in `compress_partition()` in `mod.rs`.

5. Add `pub mod myformat;` to `rbformats/mod.rs`.

## Conventions

- All compression functions return `Vec<String>` of output filenames (relative to the backup folder).
- All functions accept `progress_cb` (called with bytes read from source) and `cancel_check` (returns `true` to abort).
- Shared utilities: `SplitWriter` for file splitting, `write_zeros` for gap filling, `output_path` for consistent naming, `CHUNK_SIZE` (256 KB) for I/O buffers.
