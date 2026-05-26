# GHO file-aware reconstruction

Status (2026-05-26):
- **Slice A** — typed body parsers + record-type predicates: **done**
  (commit `e8847d0`, validated against PART.GHO).
- **Slice B** — directory tree walker: **done** (commit `28d9ab1`,
  validated against PART.GHO: 22,219 entries / 20,761 files / 1,458
  dirs walked cleanly, structural invariants checked on MYDOCU~1,
  MYPICT~1, PROGRA~1, WORDPAD.EXE).
- **Slice C** — FAT image emitter: not started.
- **Slice D** — GUI wiring: not started.

Goal: turn a file-aware GHO into a mountable raw FAT partition image,
sitting alongside the SECTOR-mode `GhoReader` for the file-aware case.

## Exploration tools

- `examples/gho_dump_records.rs` — hex-dump first N records of a GHO.
- `examples/gho_record_histogram.rs` — type/marker histogram, used to
  characterise unfamiliar fixtures.
- `examples/gho_dump_tree.rs` — decodes each dir entry inline with
  name/attr/cluster/size; used to reverse-engineer the directory walker.

## Record types

| type   | body_len  | meaning                                                     |
|--------|-----------|-------------------------------------------------------------|
| `0x0002` | 32768   | full 32 KiB cluster of file content                         |
| `0x0017` | 512     | boot sector (Ghost 7.5)                                     |
| `0x0717` | 512     | boot sector (Ghost 11.5)                                    |
| `0x0004` | 56      | dir entry — Ghost 7.5 "header section"                      |
| `0x0704` | 56      | dir entry — Ghost 11.5 "header section" (appears once)      |
| `0x0104` | 56      | dir entry — all later entries on both 7.5 and 11.5          |
| `0x0102` | variable| file content (whole small file OR tail fragment)            |
| `0x0103` | 20      | per-file checksum: `[u32 cksum][u32 cksum_dup][12 zeros]`   |

Markers observed: `0x0000` (default), `0x95FD` (~10% of records on
Ghost 11.5 — purpose TBD), `0xC01E` (first 3 records on 11.5
file-aware fixtures — header-section flag).

The `0x07xx`-series codes are NOT filesystem-specific. They're the
Ghost 11.5 stream-version equivalents of the `0x00xx` codes used by
Ghost 7.5 for the same body shapes. XP_SP2FU.GHO (which the filename
suggests is NTFS) is actually a FAT32 partition — "MSWIN4.1" + "FAT32"
signature in the boot sector. We have no real NTFS fixtures in the
corpus; all four file-aware fixtures are FAT12/16/32.

### `0x0017` / `0x0717` boot sector
Verbatim copy of the FAT VBR. Bytes 0..511 of the backed-up partition.
For FAT32, BPB_RootClus is at offset 44 (4 bytes LE). Confirm FAT32 by
the `"FAT32"` signature at offsets 82..87.

### `0x0004` / `0x0104` / `0x0704` directory entry
First 32 bytes = verbatim FAT directory entry (LFN segment, 8.3 entry,
dot/dotdot, or empty slot). Bytes 32..36 = `u32 entry_hash`. Bytes
36..56 = 20 reserved zero bytes.

The `0x0004` → `0x0104` transition is a writer-side stream-state flag
unrelated to filesystem semantics. The walker treats all three type
codes identically.

### `0x0002` / `0x0102` file content
Files are emitted as N × `0x0002` (32 KiB each, full clusters) +
optional `0x0102` tail (file_size mod 32768). Verified against
MSPAINT.EXE (344 064 bytes = 10 × 32 768 + 16 384, exact match).

Uncompressed fixtures: body bytes are the raw content.
Compressed fixtures: body is `[4-byte prefix][zlib stream]` (per the
older 5.5c finding documented in `src/rbformats/gho.rs`
`zlib_decode_block`).

### `0x0103` per-file checksum
Duplicated `u32` followed by 12 zero bytes. Algorithm is not yet
reverse-engineered. Probably CRC-32 or Adler-32 over the file content
— compute both for a known file (e.g. `desktop.ini` 125 bytes from
PART.GHO record #12) and compare to the value stored at record #13
(`0x1d78fce5`). Useful for integrity checks; not blocking
reconstruction.

## Directory walker algorithm

Reverse-engineered from PART.GHO records 0-200 and implemented in
`walk_file_aware_tree(reader, image) -> GhoFileAwareTree`. Validated
against the full 22,219-entry tree.

The writer does depth-first **pre-order** descent: emit a dir's entries
inline, recurse into each subdir IMMEDIATELY after seeing its 8.3
entry, return when the subdir's entries are exhausted, continue the
parent dir.

The walker mirrors this with a simpler reader-side model that doesn't
need an explicit pop stack:

1. **`current_dir_cluster`** is initialised from the boot sector's
   `BPB_RootClus` (FAT32) or `0` (FAT12/16 fixed root area).
2. **`.` entry** sets `current_dir_cluster` to the entry's
   first_cluster. The writer always emits `.` right after descending
   into a directory.
3. **`..` entry** reveals the PARENT cluster of the directory we just
   descended into. We back-fill `parent_cluster` on the most-recent
   pending DIR entry.
4. **FILE 8.3 entry** uses `current_dir_cluster` as its parent.
5. **DIR 8.3 entry** is pending; `parent_cluster` filled when the
   dir's own `..` entry shows up.
6. **LFN slots** (`attr == 0x0F`) buffer up before each 8.3 record
   and attach as `long_name` to the next 8.3.
7. **Empty / deleted slots** (first byte `0x00` or `0xE5`) are FAT
   source padding artifacts — ignored.

### Key correctness check

`PROGRA~1` (Program Files) appears in the stream while
`current_dir_cluster` still points at MyPics (cluster 8) — i.e. between
MyPics' content end and the next descent. If we naively used
`current_dir` as the dir's parent, Program Files would be wrongly
placed inside MyPics. But its OWN `..` entry (record #18) says
`cluster=0`, which we normalise to `root_cluster` per FAT convention
(direct children of root carry `..` cluster=0, not the actual root
cluster). So Program Files correctly lands at root.

This is the load-bearing test for the algorithm; the fixture-gated
test in `src/rbformats/gho.rs` (`walk_file_aware_tree_against_real_part_gho`)
asserts it explicitly.

### Known limitation

For FILE entries we use `current_dir_cluster` as the parent. If
Ghost's writer ascends through multiple levels between a `.` entry
and the next 8.3 FILE entry (the "implicit pop" scenario), files in
the intervening levels would be attributed to the deepest level
visited. Directory entries are NOT affected — they get their true
parent via their own `..` entry.

The scope of this concern is bounded by the corpus we have:
- PART.GHO's WORDPAD.EXE / MSPAINT.EXE / etc. all appear in
  HyperTerminal's `current_dir` window. They may genuinely belong to
  HyperTerminal (custom install) or they may be in Accessories one
  level up. We don't have an independent ground-truth dump of PART.GHO
  to disambiguate.
- A future validation step (Ghost Explorer external extract, or mount
  the reconstructed image and compare) would resolve this. Not
  blocking slice C: the walker output is internally consistent and the
  reconstructed image will be byte-equivalent up to file placement.

## Slice C — FAT image emitter (planned)

Given `GhoFileAwareTree` + `content_record_offsets` per file, the
emitter constructs a fresh FAT partition image:

1. **Use the source boot sector verbatim** at LBA 0. Reuses
   sectors_per_cluster, reserved_sectors, num_FATs, sectors_per_FAT,
   media descriptor, etc.
2. **Format a blank FAT image** matching the source BPB params and
   open it as `EditableFilesystem` from `src/fs/fat.rs`. This reuses
   the existing FAT writer (cluster allocator, dir entry encoder,
   LFN encoder, FSInfo updater).
3. **Walk the tree** in topological order (root → subdirs depth-first
   or BFS — either works since `EditableFilesystem::create_directory`
   takes the parent path). For each entry:
   - DIR: `create_directory(parent_path, name, opts)`
   - FILE: `create_file(parent_path, name, content_reader, opts)` —
     stream the content from `content_record_offsets` via a custom
     `Read` wrapper that decompresses each `0x0002` / `0x0102` body
     on demand (None / Fast / High per container compression).
4. **Sync metadata** once at the end; the resulting image is a
   ready-to-mount raw FAT partition.

The output is **byte-equivalent up to cluster allocation order**: we
choose fresh cluster numbers (sequential as we create), so the source
disk's FAT chain is not preserved. File content + names + tree
structure ARE preserved.

Open work items for slice C:
- Decide the output API: does the emitter take a `Write+Seek` sink, or
  produce a tempfile path, or directly stream to a `Vec<u8>`?
  (Sectors-aligned `Write+Seek` is most flexible.)
- Validate by mounting the reconstructed image and comparing the
  directory tree to what we computed in slice B.
- Compute `0x0103` checksums on emit (once the algorithm is known) so
  round-trip GHO-out → GHO-in is possible.

## Slice D — GUI wiring (planned)

- Inspect tab: when picking a file-aware `.gho`, route through the
  reconstructor → mountable image. Show a progress bar over the
  record walk + emit phases.
- Auto-detect at file-pick time (we already have `image_type` in
  the container header). SECTOR-mode keeps using `GhoReader`;
  file-aware switches to a "decode to temp image" flow that wraps
  walker + emitter.

## What's NOT in scope here

- **No FAT entries are stored in the record stream.** Ghost's
  file-aware mode discards the source's FAT entirely; the rebuilder
  constructs a fresh FAT from observed (file, cluster_count) pairs.
- **No partition table info.** File-aware GHOs are
  `image_type=0x00` (single partition). Multi-partition full-disk
  backups (e.g. FULLDISK.GHO) presumably interleave one record stream
  per partition with a top-level "begin partition" marker — not yet
  observed in our trace. SECTOR-mode multi-partition is handled
  separately via the existing block-stream path.
- **No NTFS.** All four file-aware fixtures we have are FAT. If a real
  NTFS fixture turns up (recognisable by `"NTFS    "` OEM signature in
  the boot sector + record types we haven't seen yet), it's a separate
  reconstructor with a different downstream emitter.
- **Password-protected file-aware GHOs.** Blocked on the cipher
  reverse-engineering work tracked in `docs/gho_password.md`. All our
  encrypted fixtures are file-aware mode, so neither problem unblocks
  the other.

## Open questions (still)

1. **The implicit-pop ambiguity for FILE parent attribution.** Could
   be resolved with an independent extract of PART.GHO (Ghost
   Explorer, or compare against a mount of the reconstructed image).
2. **`0x0103` checksum algorithm.** CRC-32 vs Adler-32 vs custom.
   Brute-force compute both over `desktop.ini` (125 bytes, checksum
   `0x1d78fce5`) and compare.
3. **`0x95FD` and `0xC01E` marker semantics.** `0xC01E` flags the
   first 3 records on Ghost 11.5 (header section). `0x95FD` appears
   on ~10% of records throughout. Not blocking, but unexplained.
4. **Multi-partition full-disk reconstruction.** FULLDISK.GHO likely
   has multiple boot-sector records (one per partition). The walker
   currently handles only the first partition; adding multi-partition
   support is a separate effort, not blocking slice C for the
   common single-partition case.
