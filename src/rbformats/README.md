# Output Formats Module (`src/rbformats/`)

Compress/decompress handlers for backup output formats, plus disk reconstruction for restore and VHD export.

## Architecture

- **`mod.rs`** — Orchestration functions (`compress_partition`, `decompress_to_writer`, `reconstruct_disk_from_backup`) and shared utilities (`SplitWriter`, `write_zeros`, `is_all_zeros`, `output_path`, `file_name`, `CHUNK_SIZE`).
- **`vhd.rs`** — VHD (Fixed) format: `build_vhd_footer`, `vhd_chs_geometry`, `export_whole_disk_vhd`, `export_partition_vhd`, `VHD_COOKIE`.
- **`chd.rs`** — CHD (MAME) format via external `chdman` tool: `detect_chdman`, `compress_chd`, `split_file`.
- **`zstd.rs`** — Zstd streaming compression: `compress_zstd`.
- **`raw.rs`** — Raw streaming with optional file splitting and sparse zero-skipping: `stream_with_split`.

## Available Formats

| Format | Extension | Compression | Splitting | Notes |
|--------|-----------|-------------|-----------|-------|
| Raw    | `.raw`    | None        | Yes       | Supports sparse zero-skipping |
| VHD    | `.vhd`    | None        | No        | Fixed VHD with 512-byte footer |
| Zstd   | `.zst`    | Zstd level 3 | Yes (post-hoc) | Good compression ratio |
| CHD    | `.chd`    | MAME CHD    | Yes (post-hoc) | Requires `chdman` on PATH |

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
