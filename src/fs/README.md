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


## BFS (BeOS) on-disk offsets

`bfs.rs` and `bfs_write.rs` were written against the BeOS R5 (x86) and
BeOS/PPC fixtures rather than a header file, because the published
descriptions disagree on padding. These are the offsets the real volumes have.
Every multi-byte field is in the volume's own byte order, which `magic1`
decides: an x86 volume stores `BFS1` as `31 53 46 42`, a PPC one as
`42 46 53 31`.

**Superblock** — byte 512 of the volume on x86, byte **0** on PPC and on a
bare volume with no PC boot block. Haiku's `Volume::Identify` probes the same
two places, and so does `BfsFilesystem::open`.

| Offset | Field | Note |
|-------:|-------|------|
| 0 | `name[32]` | volume name |
| 32 | `magic1` | `'BFS1'` — also the byte-order oracle |
| 36 | `fs_byte_order` | always the literal `'BIGE'`, in the volume's order |
| 40 | `block_size`, 44 `block_shift` | must satisfy `1 << shift == size` |
| 48 | `num_blocks` (i64), 56 `used_blocks` (i64) | |
| 64 | `inode_size`, 68 `magic2` (`0xdd121031`) | |
| 72 | `blocks_per_ag`, 76 `ag_shift`, 80 `num_ags` | bitmap geometry |
| 84 | `flags` | `'CLEN'` clean / `'DIRT'` dirty |
| 88 | `log_blocks` (block_run), 96 `log_start`, 104 `log_end` | |
| 112 | `magic3` (`0x15b6830e`) | |
| 116 | `root_dir` (block_run), 124 `indices` (block_run) | |

A `block_run` is `{allocation_group i32, start u16, len u16}` and resolves to
block `(allocation_group << ag_shift) | start`. The layout is: block 0 boot +
superblock, blocks `1 .. 1 + num_ags * blocks_per_ag` the allocation bitmap
(**set bit = allocated**, LSB first inside each 32-bit word), then the log,
then everything else.

**Inode** — one `inode_size` region at the block its `block_run` names. That
block number *is* the file's identity; BFS has no separate inode numbers.

| Offset | Field |
|-------:|-------|
| 0 | `magic1` = `0x3bbe0ad9` |
| 4 | `inode_num` (block_run) |
| 12 `uid`, 16 `gid`, 20 `mode`, 24 `flags` | `mode` carries high bits beyond `S_IFMT`, so mask before switching on the type |
| 28 | `create_time` (i64), 36 `last_modified_time` (i64) — both `seconds << 16` |
| 44 | `parent` (block_run), 52 `attributes` (block_run) |
| 60 | `type`, 64 `inode_size`, 68 `etc` (a stale in-memory pointer) |
| 72 (0x48) | `data_stream`, 144 bytes — **or** the inline symlink target, which is the same bytes under a different name |
| 232 (0xE8) | first `small_data` |

The union at 0x48 is the trap: a rewrite that treats those bytes as a symlink
target on a *directory* silently destroys its data stream. `parse_inode` only
fills `short_symlink` when the mode says link and `INODE_LONG_SYMLINK` is
clear.

A `data_stream` is 12 direct `block_run`s, then `max_direct_range` (i64),
`indirect` (block_run), `max_indirect_range`, `double_indirect` (block_run),
`max_double_indirect_range`, `size`.

A `small_data` is `{type u32, name_size u16, data_size u16}` followed by
`name_size` name bytes, **3 bytes of padding**, `data_size` data bytes, and a
NUL. A `name_size` of zero ends the chain. The file's own name is the entry of
type `'CSTR'` whose one-byte name is `0x13`.

**Directory B+tree** — a directory's data stream is a `bplustree_header`
(magic `0x69f6c2e8`, `node_size`, `max_number_of_levels`, `data_type`, then
i64 `root_node_pointer`, `free_node_pointer`, `maximum_size`; 40 bytes) padded
to `node_size`, followed by nodes at multiples of `node_size`. A node is three
i64 links (`left`, `right`, `overflow`) then `all_key_count` and
`all_key_length` as u16 — 28 bytes. Then:

- keys, concatenated, at offset 28,
- the key-length array (u16 *cumulative end offsets*, not lengths) at
  `align_up(28 + all_key_length, 8)` — note the alignment is computed from the
  **node start**, not from the end of the keys,
- the value array (i64) immediately after, with no further padding.

`overflow == -1` marks a leaf. Leaf values are inode block numbers; internal
values are byte offsets of child nodes within the same stream. Unlike most
Unix filesystems, BFS **does** store `.` and `..` as real keys.

`bfs_write.rs` splits a full leaf in half, promotes its last key as the
separator, and grows a new root when the old root splits. A leaf emptied by
deletion is left linked in place rather than merged — a legal B+tree, and it
keeps deletion O(1) in tree height. Freed nodes are threaded through
`left_link` from `free_node_pointer`.


## BeOS OFS

The pre-BFS filesystem. Its layout is in the `//!` header of `ofs.rs`; the
notes worth repeating here are the two that cost time to establish:

- A version-1 directory block is 4096 bytes (8 sectors) — `64 + 63 * 64`
  exactly. On version 2 and 3 the records occupy 8128 bytes, which rounds up
  to 16 sectors on disk. The 64 trailing bytes `ofs-extractor` skips after the
  63rd entry are padding, not a field.
- `last_alloc_list` means two different things. On a contiguous file (top bit
  of `first_alloc_list` set) it is the run's **length in sectors**; on an
  extent-list file it is the **last list sector** in the chain.
