# Filesystem Module (`src/fs/`)

Trait-based filesystem abstraction for browsing, compaction, resize, and validation of partition contents.

## Architecture

- **`filesystem.rs`** — `Filesystem` trait and `FilesystemError` enum. The trait defines `root`, `list_directory`, `read_file`, `volume_label`, `fs_type`, `total_size`, `used_size`, and `last_data_byte`.
- **`entry.rs`** — `FileEntry` and `EntryType` structs representing files and directories within a partition.
- **`fat.rs`** — Complete FAT12/16/32 implementation: BPB parsing, directory browsing (with LFN and CP437 support), cluster chain traversal, `CompactFatReader` for smart backup compaction, and in-place resize/validation/BPB patching for restore.
- **`ntfs.rs`** — NTFS implementation: VBR parsing, MFT record parsing with fixup array handling, data run decoding, version detection (1.0–3.1), directory browsing via B+ tree index entries, `CompactNtfsReader` for bitmap-based compaction, and VBR patching resize.
- **`exfat.rs`** — exFAT implementation: VBR parsing, directory entry set parsing (File + Stream Extension + File Name entries), allocation bitmap reading, `CompactExfatReader` for compaction, and full resize with bitmap/FAT/VBR/checksum updates.
- **`mod.rs`** — Factory functions (`open_filesystem`, `compact_partition_reader`, `effective_partition_size`) that route by MBR partition type byte, plus re-exports. Type byte `0x07` is disambiguated by reading the OEM ID magic (`"NTFS    "` vs `"EXFAT   "`).

## Supported Partition Types

**There is deliberately no capability table here.** One lived at this spot and
went stale: it listed five filesystems and six type bytes while the engine had
grown to around forty drivers, and still called ext "planned" years after
`ext.rs`, `ext_format.rs`, `ext_fsck.rs` and `ext_csum.rs` shipped. A hand-kept
table two levels down from the code it describes loses that race every time.

Two live sources instead, neither of which can drift:

- **What a filesystem can do** — the Filesystems table in the top-level
  [`README.md`](../../README.md), which is the user-facing list and is covered
  by the pre-commit documentation sync in CLAUDE.md.
- **Which type byte or DosType routes where** — the dispatch itself in
  [`mod.rs`](mod.rs): `open_filesystem`, `open_editable_filesystem`,
  `compact_partition_reader`, `effective_partition_size`, and the
  `partition_type_string` matchers beside them.

The one routing fact that is not obvious from either and belongs here: type
byte `0x07` covers **both** NTFS and exFAT, and `open_filesystem` disambiguates
by reading the OEM ID magic (`"NTFS    "` vs `"EXFAT   "`) rather than the byte.

## How to Add a New Filesystem

1. Create `src/fs/myfs.rs` implementing the `Filesystem` trait:
   - `root()` — return the root directory entry
   - `list_directory()` — list entries in a directory
   - `read_file()` — read file contents (up to `max_bytes`)
   - `volume_label()`, `fs_type()`, `total_size()`, `used_size()`
   - `last_data_byte()` — minimum bytes from partition start to capture all data (for smart trimming)

2. Optionally implement a `CompactMyfsReader` (implements `Read`) for defragmented streaming backup.

3. Optionally implement `resize_myfs_in_place()` and `validate_myfs_integrity()` for restore/VHD export with partition resizing.

4. Register in `fs/mod.rs`:
   - Add partition type byte matching in `open_filesystem()`
   - Add matching in `compact_partition_reader()` if compaction is supported
   - Add matching in `effective_partition_size()` if trimming is supported

5. Add `pub mod myfs;` to `fs/mod.rs`.

See `fat.rs` as the complete reference implementation showing all capabilities.


## Compact reader sizing model

Compact readers return a `CompactResult` (`fs/mod.rs`) carrying three byte counts that look similar but mean different things. Reading or writing one without remembering the distinction will silently corrupt progress bars or backup metadata.

| Field             | Meaning                                                                  |
|-------------------|--------------------------------------------------------------------------|
| `original_size`   | Partition size on the source disk (unchanged by compaction).             |
| `compacted_size`  | Bytes the reader will emit downstream (the stream's actual length).      |
| `data_size`       | Logical data bytes: `allocated_blocks * block_size` (+ HFS pre-alloc).   |

Two reader styles produce different relations:

- **Packed** (FAT, NTFS, exFAT): boot region + FAT + only used clusters, contiguously. Stream is *smaller* than the original. Invariant: `data_size == compacted_size < original_size`.
- **Layout-preserving** (HFS, HFS+, ext, btrfs, ProDOS): full original byte layout, but free clusters are zero-filled in-memory rather than read from disk. Stream is *the same length* as the original. Invariant: `data_size < compacted_size == original_size`.

In `backup/mod.rs` the orchestrator uses these as:

- `effective_sizes[i] = data_size` — feeds the smart-sizing log line ("X MiB of data in N MiB partition").
- `stream_sizes[i] = compacted_size` — feeds the progress bar (matches actual bytes flowing through the compressor).
- `total_display_bytes = sum(data_sizes)` — total logical data across all partitions.
- `total_stream_bytes = sum(stream_sizes)` — total bytes written to the archive.

When adding a new compact reader, decide which style it is, stamp the result accordingly, and **add a unit test asserting the invariant** so future changes can't drift.
