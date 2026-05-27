# GHO File-Aware Open Performance

## Problem

Opening a file-aware GHO for browsing requires building a virtual FAT
image in memory.  `build_virtual_fat_image` creates every directory and
file on a `SparseSink`-backed FAT filesystem so the browse layer sees a
real FAT disk.  For PART-HI.GHO (577 MB compressed, 8 GB partition,
20K files, 278K clusters) this took **~52 seconds**.

## Shipped optimizations (this session)

| Change | File | Effect |
|--------|------|--------|
| `zlib_decode_block` — stop stripping 4-byte prefix | `gho.rs` | Fixes "corrupt deflate stream" on all High-compression GHOs |
| `index_sector_blocks` — 256 KiB buffered reads | `gho.rs` | Sector-mode indexing: eliminates per-block seek+read syscalls |
| CLI `inspect`/`ls`/`get` route through `open_read()` | `resolve.rs`, verbs | GHO/IMZ/CHD now work from the CLI |
| `skip_data_write` on `CreateFileOptions` | `filesystem.rs`, `fat.rs` | Eliminates 2.2M Vec allocs (1.1 GB of zeros) |
| `next_free_hint` on `FatFilesystem` | `fat.rs` | `find_free_clusters` starts from last alloc, not cluster 2 |
| `read_fat_entry_single` | `fat.rs` | Reads one FAT entry instead of loading entire 8 MB table |

**Result: 52 s -> 39 s** (PART-HI.GHO, release build, Apple Silicon)

## Remaining bottleneck analysis

The 39 seconds breaks down roughly as:

1. **~2 s** — `parse_gho_image` + `walk_file_aware_tree` (108K records, I/O-bound, fast)
2. **~37 s** — `build_virtual_fat_image` creating 20K files + 800 directories

Each `create_file` call still does:
- `read_cluster_chain(parent)` — reads parent dir cluster data through SparseSink
- `name_exists_in_dir` — scans all entries for duplicate check
- `collect_existing_sfns` — reads parent dir AGAIN for SFN collision avoidance
- `generate_short_name` — computes 8.3 name
- `add_to_directory` — appends dir entries, may allocate new dir cluster

For large directories (e.g., WINDOWS/SYSTEM32 with hundreds of files),
each `create_file` reads the entire directory cluster chain twice.
This is O(n^2) per directory.

## Optimization ideas to explore

### Tier 1: Quick wins (stay within current architecture)

**A. Cache parent directory data across `create_file` calls**

The current code reads the parent directory from the SparseSink on
every `create_file`.  Since we process children of the same parent
sequentially, we could cache the parent's cluster chain data and
invalidate it only when `add_to_directory` extends the chain.

Estimated impact: ~2x speedup (eliminates redundant reads).

**B. Batch SFN generation / skip duplicate checks**

On a fresh empty FAT, names are guaranteed unique (they come from the
GHO tree which is already deduplicated).  We could add a
`skip_duplicate_check: bool` to `CreateFileOptions` or use a
`create_file_unchecked` variant.

Estimated impact: ~1.5x (eliminates both dir-read passes per file).

**C. BTreeMap -> HashMap for SparseSink**

`SparseSink` uses `BTreeMap<u64, Vec<u8>>` — every sector read/write
is O(log n).  Switching to `HashMap` gives O(1) amortized.  The
ordered property is not needed for the virtual FAT builder.

Estimated impact: ~1.3x (constant-factor improvement on all I/O).

### Tier 2: Bigger refactors

**D. In-memory FAT table cache in `FatFilesystem`**

Load the FAT table once at open, keep it in a `Vec<u8>`, and flush
to the underlying reader only on `sync_metadata`.  All FAT reads
become array lookups; all writes are array stores + dirty flag.

This is the single highest-impact change: `allocate_cluster_chain`
and `next_cluster` would become pure in-memory operations.

Estimated impact: ~3-5x.  But touches core FAT code used by all
filesystems, so needs thorough testing.

**E. Bypass FAT filesystem entirely for file cluster mapping**

Since we're building on a fresh empty FAT with sequential allocation:
- Compute cluster assignments mathematically (no FAT I/O)
- Still create directories through `FatFilesystem` (need . and .. entries)
- For files: write FAT chain entries + dir entries directly to SparseSink
- Build `cluster_to_file` from the computed assignments

This avoids all `create_file` overhead for files (20K calls) but
requires reimplementing FAT directory entry encoding and FAT chain
writing outside of `FatFilesystem`.

Estimated impact: ~10-20x (files become O(1) each).
Risk: high — duplicates FAT logic, fragile if FAT format assumptions change.

**F. Replace SparseSink FAT with a native `Filesystem` over the GHO tree**

Instead of creating a real FAT filesystem, implement `Filesystem` trait
directly backed by the GHO `FileAwareTree`.  Directory listing, file
reads, and metadata all come from the tree + on-demand decompression.

This eliminates `build_virtual_fat_image` entirely.  Open time would
be ~2 seconds (just record parsing + tree walking).

Estimated impact: ~20x.
Risk: medium-high — need to reimplement all Filesystem trait methods,
and the browse view + inspect tab expect partition-table detection to
work (would need a synthetic BPB or bypass).

### Tier 3: Parallelism

**G. Multi-threaded `build_virtual_fat_image`**

The tree walk is inherently sequential (parent dirs must exist before
children).  But we could parallelize within a directory: create all
files in one dir concurrently, then move to subdirs.

However, `FatFilesystem` is not thread-safe (single reader), so this
would require either fine-grained locking or a producer/consumer
model where one thread does FAT I/O and others prepare dir entries.

Estimated impact: unclear — the bottleneck is I/O through SparseSink,
not CPU.  Parallelism helps most when CPU is the bottleneck.

## Recommended path

1. **Ship A+B** (cache parent dir + skip dupe checks) — low risk, ~3x
2. **Then D** (in-memory FAT cache) — medium risk, ~3-5x more
3. **Consider F** only if the above aren't enough — it's the cleanest
   long-term solution but a significant implementation effort

Target: under 5 seconds for PART-HI.GHO.
