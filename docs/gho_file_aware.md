# GHO file-aware mode — record body layouts (in progress)

Status: **exploration phase**. Boot sector + cluster data + directory
entries decoded; directory tree traversal semantics partially mapped;
FAT reconstruction strategy outlined but not implemented.

## Tool

`examples/gho_dump_records.rs` walks the first N records of a file-aware
fixture and hex-dumps each body. Default fixture is
`7.5/PART/PART.GHO` (uncompressed, easiest to read).

Run: `cargo run --release --example gho_dump_records [-- path] [count]`.

## Record body layouts

### `0x0017` (body=512) — boot sector
Verbatim copy of the FAT VBR (BPB at offset 11). Bytes 0..511 of the
backed-up partition. Place at LBA 0 of the reconstructed partition.

### `0x0004` (body=56) — current-directory dir entry
First 32 bytes = one verbatim FAT directory entry (LFN segment, 8.3
entry, dot/dotdot, or empty slot). Last 24 bytes = `[u32 hash][20 zero
bytes]`. Hash is presumably for per-entry integrity.

The walker emits `0x0004` records for entries in the **currently active
directory**. Each subdirectory's `0x0004` stream is terminated by an
all-zero entry (first byte 0x00 = FAT end-of-directory).

### `0x0104` (body=56) — same format as `0x0004`
Identical 32-byte FAT-entry payload + 24-byte trailer. Observed
empirically: `0x0004` entries cluster near the start of the stream
(root dir + first descent), `0x0104` entries appear after the walker
has descended into deeper subdirectories. The two type codes likely
encode tree-position metadata (root vs deeper, or
visited-on-the-way-down vs going-back-up). **Exact semantics not yet
nailed down** — needs more record-walk analysis or a known-tree
reference (start with a Win98 disk whose dir tree we can dump
independently with mtools).

### `0x0002` (body=32768) — full data cluster
One 32 KiB cluster of file content, verbatim. Files are emitted as:
N × `0x0002` records (full clusters) followed by 0 or 1 × `0x0102`
record (tail). Cluster ordering matches the source's FAT chain.

For PART.GHO (uncompressed) the body is the raw cluster data. For
compressed file-aware variants, the body is `[4-byte prefix][zlib
stream]` (per the older 5.5c finding documented in
`src/rbformats/gho.rs` `zlib_decode_block`).

### `0x0102` (body=variable) — file content (or tail)
Body length matches either:
- the source file's exact size (when file ≤ 32 KiB; single record holds
  the whole file), OR
- the remainder `file_size mod 32768` (when file > 32 KiB; appended
  after the file's N `0x0002` chunks).

Examples from PART.GHO (uncompressed, file-aware):
- `CIS.SCP` = 720 bytes → one `0x0102` body=720.
- `MSPAINT.EXE` = 0x54000 (344 064) bytes → ten `0x0002` bodies
  (10 × 32 768 = 327 680) + one `0x0102` body=16 384. Sum = 344 064 ✓.

### `0x0103` (body=20) — per-file metadata
Always 20 bytes. Layout: `[u32 X][u32 X][12 zero bytes]` — first 8
bytes are the same value duplicated. Likely a 32-bit checksum stored
twice for integrity. Appears once per file, immediately after the
file's content stream (`0x0002` and/or `0x0102` records).

## Reconstruction strategy (sketch)

A file-aware-to-raw-FAT rebuilder needs to:
1. **Parse boot sector** (record `0x0017`) for BPB: sectors_per_cluster,
   reserved_sectors, num_FATs, sectors_per_FAT (FAT16) /
   sectors_per_FAT_32 (FAT32), root_dir_entries (FAT12/16) /
   root_dir_cluster (FAT32).
2. **Allocate clusters sequentially** as we walk the record stream.
   We do NOT preserve the source disk's exact FAT chain — we lay out
   files contiguously in fresh cluster numbers, then patch dir entries
   to point at the new cluster numbers. (The source's cluster numbers
   are irrelevant; what matters is the file's content + name.)
3. **Track the active directory** as `0x0004`/`0x0104` records stream
   in. Each subdirectory's entry stream ends at the all-zero "end of
   dir" marker; the next directory in the tree begins at the next
   record (with a soft tree-traversal indicator that needs reverse
   engineering — see `0x0004` vs `0x0104` open question).
4. **Emit raw FAT image** with:
   - LBA 0 = boot sector
   - LBA `reserved_sectors` = FAT #1 (computed from cluster chains)
   - LBA `reserved_sectors + sectors_per_FAT` = FAT #2 (= FAT #1)
   - LBA `data_region_start` = clusters in allocation order

## Open questions for the next session

1. **`0x0004` vs `0x0104` semantics.** Both are dir entries with the
   same 56-byte layout. We need a known-tree reference to determine
   the boundary — mount PART.GHO independently (e.g. via Ghost
   Explorer on Win98) and correlate the entry stream with the actual
   directory traversal order.
2. **Directory tree depth tracking.** When a directory's entries end,
   how do we know whether the next entry belongs to a *child* or a
   *sibling*? Possibilities to check:
   - The 4-byte trailer of `0x0004`/`0x0104` carries the
     parent-cluster ID (so we can rebuild parent→child links).
   - There's an unobserved "begin/end directory" record type that we
     missed in the first 60 records (look further in the stream).
   - The walker is depth-first with implicit termination at the
     all-zero entry, and the directory tree is rebuilt by counting
     dot/dotdot entries' first_cluster references.
3. **FAT16/12 fixtures.** PART.GHO is FAT32. Need to verify the
   record layout is identical for FAT16/FAT12 fixtures, or whether
   record bodies/lengths differ.
4. **`0x0103` checksum algorithm.** Reverse-engineer the 32-bit
   per-file checksum so reconstructions can write it back (probably
   CRC-32 or Adler-32; check by computing both for known files).

## What's NOT here

- No FAT entries are stored in the record stream. The original FAT is
  discarded; the rebuilder constructs a fresh FAT from the (file,
  cluster_count) pairs it sees.
- No partition table info. File-aware backups of an
  `image_type=0x00` (single-partition) source. Multi-partition
  full-disk backups (FULLDISK.GHO) presumably interleave one record
  stream per partition with a top-level "begin partition" marker —
  not yet observed.
