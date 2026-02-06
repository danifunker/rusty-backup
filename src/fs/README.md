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

| Type Byte(s)                         | Filesystem | Browsing | Compaction | Resize |
|--------------------------------------|-----------|----------|------------|--------|
| `0x01`                               | FAT12     | Yes      | Yes        | Yes    |
| `0x04`, `0x06`, `0x0E`, `0x14`, `0x16`, `0x1E` | FAT16 | Yes | Yes | Yes |
| `0x0B`, `0x0C`, `0x1B`, `0x1C`      | FAT32     | Yes      | Yes        | Yes    |
| `0x07`                               | NTFS      | Yes      | Yes        | Yes (VBR patch) |
| `0x07`                               | exFAT     | Yes      | Yes        | Yes (full bitmap resize) |
| `0x83`                               | ext2/3/4  | No (planned) | No | No |

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
