# Lazy Browse Architecture Plan

## Problem

Browsing a partition from a native zstd backup currently requires building a full
seekable zstd cache before the filesystem can be opened at all. For large partitions
(e.g. a 79.5 GiB HFS+ volume stored as a 17.5 GiB .zst file), this means a one-time
decompress-and-reencode pass that takes 30+ minutes. Only then does the browser open.

The Clonezilla browse path already does something better: it scans only the
filesystem-specific metadata blocks (30–60 seconds), caches them, and reads data
blocks lazily on demand.

---

## Why the Two Paths Differ

**Clonezilla / partclone** embeds a block-level bitmap in its format. The scanner
knows exactly which logical block numbers are occupied and where each one lives in the
compressed stream, making targeted lazy decompression straightforward.

**Native zstd** is a flat sequential stream with no block index. There is no way to
jump to an arbitrary byte position without first decompressing everything before it.
That is why the current code converts the file into `zeekstd` seekable format before
opening anything.

---

## Current Open-Time Load by Filesystem

| Filesystem | Eagerly loaded at `open()` | Typical size | Structures read lazily |
|---|---|---|---|
| FAT | BPB (boot sector) | ~1 KB | FAT table, clusters, file data |
| NTFS | VBR + 2 MFT records | ~10 KB | MFT records, attributes, file data |
| ext2/3/4 | Superblock + all GDT entries | ~1–2 MB | Inode tables, bitmaps, file data |
| btrfs | Superblock + full chunk tree | ~100–500 KB | FS tree nodes, file extents |
| HFS (classic) | MDB + entire catalog file | Varies | Data forks |
| HFS+ / HFSX | Volume header + **entire catalog B-tree** | Could be hundreds of MB | Data forks |

HFS+ is the critical outlier. The catalog file is slurped in full at `open()` via
`read_fork`, and its extents—specified in the volume header—can be located deep in the
volume. There is no guarantee the catalog is near the start of the partition.

FAT, NTFS, ext, and btrfs are well-behaved: `open()` reads a few KB to a few MB from
the very start of the partition, then all further reads are lazy seeks to known offsets.

---

## Proposed Approach: Two Complementary Changes

### Change 1 — Forward-Streaming Reader for FAT / NTFS / ext / btrfs

Implement a `ZstdStreamReader` that:

- Decompresses the zstd file sequentially into a growing in-memory (or on-disk) buffer.
- Services `read(offset, len)` requests immediately if the offset is already in the buffer.
- For a forward seek (offset > current position), continues decompression until it reaches
  the target.
- For a backward seek (offset < already-decompressed position), restarts decompression
  from the start and replays forward. Backward seeks are rare during directory browsing.
- Operates without ever building the full seekable cache file.

This is suitable for FAT, NTFS, ext, and btrfs because:
- Their `open()` reads only the first few KB–MB, which arrives almost instantly.
- Subsequent directory and file reads tend to be forward-progressing.
- The few backward seeks (e.g. revisiting a parent directory) restart from the beginning
  but are short replays for typical partition sizes.

**Expected result**: browser opens within seconds for FAT/NTFS/ext/btrfs even on large
zstd backups.

### Change 2 — Lazy B-tree Reads for HFS+ / HFSX

Redesign `HfsPlusFilesystem` to not load the entire catalog at `open()`. Instead:

- `open()` reads only the 512-byte volume header (already cheap).
- Directory listing (`list_dir`) traverses the B-tree node by node, reading only the
  nodes on the path from root to the target directory.
- Each B-tree node is typically 8 KB; a directory listing at depth D reads at most
  ~D × 8 KB of catalog data.
- Individual B-tree nodes can be cached in a `HashMap<u32, Vec<u8>>` (node number →
  data) so repeated visits to the same directory are free.

With this change, HFS+ `open()` becomes a single 512-byte read. The `ZstdStreamReader`
from Change 1 then handles the per-node seeks, which may require forward-only or short
backward replays.

**Expected result**: HFS+ browser opens in seconds; the first directory listing takes
only as long as it takes to decompress up to the first catalog B-tree node.

---

## What Happens to the Existing Seekable Cache

The seekable cache (`_{partition-N}.seekable.zst`) is still useful for repeated
browsing and for file extraction (arbitrary seeks anywhere in the partition). The plan
is to keep it as an opt-in or background-build option, not as a prerequisite for
opening the browser.

Suggested flow:

1. User clicks Browse.
2. App opens the filesystem immediately via `ZstdStreamReader` (no pre-build step).
3. In the background, the seekable cache continues building.
4. Once the seekable cache is ready, the app transparently switches the reader to it so
   that future seeks (e.g. extracting a file from the end of a large volume) are fast.
5. The cache persists to disk so subsequent sessions skip steps 2–4 entirely.

---

## Affected Files

| File | Change |
|---|---|
| `src/gui/inspect_tab.rs` | Remove blocking seekable-cache creation before browser open; start cache build in background; open browser immediately via streaming reader |
| `src/fs/mod.rs` | Thread streaming reader into `open_filesystem` and `compact_partition_reader` |
| `src/fs/hfsplus.rs` | Replace eager `read_fork` on catalog with lazy per-node B-tree reads; add node cache |
| `src/fs/hfs.rs` | Same assessment needed — check whether catalog is also eagerly loaded |
| `src/clonezilla/block_cache.rs` | Reference implementation for lazy block reading pattern |
| `src/gui/browse_view.rs` | May need reader-swap logic when seekable cache becomes ready |
| (new) `src/fs/zstd_stream.rs` | `ZstdStreamReader` implementation |

---

## Open Questions

1. **Buffer strategy for `ZstdStreamReader`**: keep decompressed data in memory, or
   spill to a temp file? For a 79.5 GiB partition the full decompressed size is too
   large for RAM. A sliding window (keep last N MB, discard older) works if backward
   seeks are always short, but a backward seek past the window forces a full restart.
   A temp file avoids restarts but adds disk I/O.

2. **HFS+ catalog node cache size**: should the node cache be bounded (LRU eviction)?
   For a volume with hundreds of thousands of files the full B-tree node set could be
   large. A cap of e.g. 64 MB is probably sufficient for normal browse patterns.

3. **When to start background seekable-cache build**: immediately on Browse click, or
   only after the user has spent some time in the browser (indicating they'll want file
   extraction too)?

4. **HFS classic**: the existing `HfsFilesystem` also loads its entire catalog at
   `open()`. Needs the same lazy-read treatment, but HFS volumes tend to be small so
   the urgency is lower.

5. **File extraction from streaming reader**: if the user tries to extract a file whose
   data lives before the current stream position, the reader must restart from the
   beginning. Should we prompt the user to wait for the seekable cache before
   extracting, or handle the restart transparently?
