# TODO: btrfs Compact Reader Stream Trimming

## Background

Rusty Backup's compact readers fall into two categories:

- **Packed** (FAT, NTFS, exFAT): allocated clusters emitted contiguously.
  `compacted_size < original_size`. The backup is already small.
- **Layout-preserving** (HFS, HFS+, ext, btrfs): free blocks emitted as zeros
  so that block addresses are preserved. `compacted_size == original_size`.

For HFS, HFS+, and ext, layout-preserving backups are **trimmed to the last
used block** (`last_data_byte()` / `effective_partition_size()`). Only the
used prefix of the partition is written to the backup stream; the free tail is
dropped. Restore zero-fills the tail and then calls the resize function (which
writes any end-of-partition structures such as the HFS+ alternate volume header).

**btrfs is the exception**: `BtrfsFilesystem::last_data_byte()` currently
returns `total_bytes` (the full partition size), so no trimming occurs and the
entire partition — including the zero-filled gaps between chunks — flows through
the compressor. On a partition with, say, 20 GiB used out of 200 GiB, this
means the compressor processes 200 GiB instead of ~20 GiB.

## Why btrfs Can't Trivially Be Trimmed

btrfs does not have a "last used block" in the simple sense that HFS+ or ext
do.  Its allocator places chunks anywhere on the device.  Data chunks, metadata
chunks, and system chunks each occupy distinct physical ranges that can be
interleaved.  There is no monotone "used prefix".

However, the compactor already knows every physical chunk range (from the
superblock `sys_chunk_array` + full chunk tree traversal), so it **does** know
the last physical byte that any chunk occupies.

## What the Compactor Already Knows

`CompactBtrfsReader::new()` (`src/fs/btrfs.rs`) builds `final_ranges: Vec<(u64,
u64)>` — a sorted, merged list of `(start_byte, end_byte)` for every physical
chunk on the device (including the superblock at `0x10000`).  The last entry's
`.1` (end) is the last physical byte used by btrfs.

```rust
// Inside CompactBtrfsReader::new():
let mut final_ranges: Vec<(u64, u64)> = ...;  // sorted, merged physical ranges
// last physical used byte = final_ranges.last().map(|(_, end)| *end)
```

The compact layout already emits a trailing `CompactSection::Zeros(total_bytes -
pos)` for everything after the last chunk.  That trailing zeros section is what
we want to trim.

## Option A — Trim at Last Chunk End (Simpler, Recommended First Step)

**Idea**: set `compacted_size = last_chunk_end` instead of `total_bytes`.
The backing `CompactStreamReader` will still emit the full `total_bytes` stream,
but the `stream_size` in the backup loop wraps the reader with `.take(stream_size)`,
cutting off the trailing zeros before they enter the compressor.

**Changes required**:

1. **`src/fs/btrfs.rs` — `CompactBtrfsReader::new()`**:
   After building `final_ranges`, compute `last_chunk_end`:
   ```rust
   let last_chunk_end = final_ranges
       .last()
       .map(|&(_, end)| end)
       .unwrap_or(0)
       .min(total_bytes);
   ```
   Return in `CompactResult`:
   ```rust
   CompactResult {
       original_size: total_bytes,
       compacted_size: last_chunk_end,   // ← was total_bytes
       data_size,
       clusters_used: total_allocated_sectors as u32,
   }
   ```

2. **`src/fs/btrfs.rs` — `BtrfsFilesystem::last_data_byte()`**:
   Duplicate the superblock + chunk-tree parse to find `last_chunk_end`.
   Or expose it as a method on a shared helper.
   ```rust
   fn last_data_byte(&mut self) -> Result<u64, FilesystemError> {
       // Re-use or cache the chunk map already loaded at open() time.
       let last_end = self.chunk_map
           .iter()
           .map(|c| c.physical + c.length)
           .max()
           .unwrap_or(self.total_bytes);
       Ok(last_end.min(self.total_bytes))
   }
   ```

3. **Restore — `src/restore/mod.rs` step 8**:
   `resize_btrfs_in_place()` must update `total_bytes` and `dev_item.total_bytes`
   in the superblock to match the trimmed size.  Without this the filesystem
   will refuse to mount ("device too small").

   Add to the `compacted_hfs_fixup` analogue for btrfs — or handle inside the
   `if needs_resize` branch since `last_chunk_end < total_bytes` means the
   compacted stream is smaller than original, making `needs_resize` true
   automatically.

**Restore implications**:
- Minimum-size restore: partition is sized to `last_chunk_end`, btrfs resize
  updates `total_bytes` to match.  ✓
- Original-size restore: partition is sized to `total_bytes`, backup stream is
  `last_chunk_end`, restore zero-fills the tail, btrfs resize updates `total_bytes`
  to `total_bytes` (no-op for size, but writes correct superblock checksum).

**Risk**: The superblock appears at a fixed offset (`0x10000`) and at mirror
copies (`0x400_0000`, `0x4000_0000_0000`).  If `last_chunk_end` is less than
the second mirror offset, the second mirror is not included in the backup.
On restore, the filesystem will fall back to the primary superblock — btrfs is
designed for this — but it reduces redundancy.  Only the primary superblock
(`0x10000`) is essential; mirrors are optional.

## Option B — Full Offline btrfs Shrink (Complex, Future Work)

**Idea**: Before compacting, rewrite the chunk tree to pack all data into the
smallest possible physical range — effectively an offline `btrfs filesystem
resize`.

This requires:
- Relocating chunks to fill gaps (analogous to `btrfs balance`).
- Rewriting extent tree and chunk tree B-tree nodes.
- Updating `total_bytes` and `dev_item` in the superblock.
- Updating all back-references in the extent tree.

This is essentially a full offline defragmentation + shrink.  It is complex,
risky (any bug corrupts the filesystem), and slow (relocating data requires
reading and rewriting every allocated extent).

**Not recommended** for Rusty Backup's use case (read-only backup of retro
disks).  Option A gives most of the space savings without any data movement.

## Key Files

| File | Relevance |
|------|-----------|
| `src/fs/btrfs.rs` | `CompactBtrfsReader::new()` — `final_ranges` already computed |
| `src/fs/btrfs.rs` | `BtrfsFilesystem::last_data_byte()` — returns `total_bytes`, needs fixing |
| `src/fs/btrfs.rs` | `resize_btrfs_in_place()` — must handle trimmed-size restore |
| `src/backup/mod.rs` | `stream_sizes[i]` for layout-preserving readers — already `.take(stream_size)` |
| `src/restore/mod.rs` | Step 8 resize loop — `needs_resize` gate will fire automatically |

## Expected Savings (Example)

200 GiB btrfs partition, 20 GiB used, chunks scattered in first 25 GiB:
- Current: 200 GiB flows through compressor (mostly zeros → fast, but CPU-intensive)
- Option A: ~25 GiB flows through compressor
- Compressed output: similar size (compressor handles zeros well), but much faster

The real win is **backup time**, not output size.
