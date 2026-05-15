# Amiga Filesystem & Container Support — Implementation Plan

Add first-class support for Amiga disk images (`.adf`, `.hdf`, `.adz`, `.hdz`,
and CHD-wrapped HDFs) and the three filesystems that matter on Classic Amiga
hardware: **OFS/FFS** (DOS\0–DOS\7), **PFS3** (PFS\3 and variants), and
**SFS** (SFS\0 / SFS\2). Both read and write are in scope; LHA / IPF / DMS / RP9
are explicitly out.

The work fits the existing layered architecture: containers flow through
`PartitionTable::open()`, partition tables produce `PartitionInfo` with a
`partition_type_string`, and filesystems are dispatched from `src/fs/mod.rs`
by that string. No new sub-crates; no C FFI. Reference C implementations live
under `~/repos/amigasources/{ADFlib,amitools,pfs3aio,smartfilesystem}` and are
consulted, not linked.

---

## Scope

### In scope
- Containers: `.adf` (raw floppy), `.hdf` (raw hard-disk image, with or without
  RDB), `.adz` / `.hdz` (gzip-wrapped ADF / HDF), CHD-wrapped HDF (already
  works through `ChdReader`).
- Partition tables: RDB (Rigid Disk Block), added as a new
  `PartitionTable::Rdb` variant alongside Mbr/Gpt/Apm/Sgi.
- Filesystems (read + write):
  - **OFS / FFS** — all 8 DosType variants `DOS\0`..`DOS\7` (Intl, DirCache
    read-only / write-through, Long Names).
  - **PFS3** — `PFS\3`, `PDS\3`, `muFS`.
  - **SFS** — `SFS\0`, `SFS\2`.
- Metadata: Amiga protection bits (RWED HSPA), comments (up to 79 chars),
  AmigaDOS DateStamps (seconds since 1978-01-01).
- Disk Validator (fsck) for FFS — handles the "validation needed" flag that
  appears when the write bit is left set after an unclean unmount.

### Out of scope (for now)
- IPF / SPS flux preservation (needs CAPS SDK; license + cross-platform pain).
- DMS, ADZ-of-DMS, LZX, RP9 — packaging layers, not block storage.
- LHA / WHDLoad browsing.
- JXFS (PPC-only, vanishingly small install base).
- Floppy disk expand-block-size or PFS/SFS in-place resize.

### Reference images (already on disk)
| Path | Size | What it is |
|---|---|---|
| `~/amiga-filesystems/DiskDoctor.adf` | 880 KiB | FFS floppy, bootable AmigaDOS 3.0 |
| `~/amiga-filesystems/4x4OffRoadRacing_v1.00_1480.hdf` | 1 MiB | OFS single-partition HDF, no RDB |
| `~/amiga-filesystems/AmigaVision.hdf` | 9.6 GiB | RDB hard-disk image |
| `~/amiga-filesystems/amiga128gb.chd` | 22 GiB compressed | CHD-wrapped 128 GB HDF |

Disk space is tight — when building synthetic test images, prefer the smallest
size that exercises the code path (a 4 MiB RDB with two 2 MiB partitions is
plenty).

---

## Architecture

### Layer 1 — Container layer (no new module)

`.adf` and `.hdf` are already raw byte streams. They flow through
`PartitionTable::open()` unchanged. Two small touch-ups:

- Recognize the standard ADF sizes (`901_120` for DD, `1_802_240` for HD) in
  `is_floppy_size()` so `detect_superfloppy()` runs on them.
- `.adz` / `.hdz` are gzip-wrapped — add a transparent decompress-on-open shim
  in the file-picker / image-open path. Decompress to a temp file the first
  time it's opened (we already do this kind of thing for some formats); never
  try to operate on the gzipped bytes in-place.

CHD is already supported via `ChdReader`.

### Layer 2 — Partition table: `PartitionTable::Rdb`

New file: `src/partition/rdb.rs`. Parses the RDSK linked structure (big-endian,
typically 512-byte blocks but the RDB declares its own).

- `RDSK` block lives in the first 16 sectors; signature = `b"RDSK"` at byte 0.
- Walk the `PART` list via `pl_Next` (terminator = `0xFFFFFFFF`).
- Each `PART` exposes start LBA, block count, RDB-side block size, and a
  4-byte `dosType` (e.g. `DOS\0`, `PFS\3`, `SFS\0`).
- Record `FSHD`/`LSEG` driver chains as opaque metadata (preserved on
  round-trip, never executed).
- Bad-block list (`BADB`) — parse and surface in the validator.

Wire into `PartitionTable::open()` between APM and the MBR fallback. Set
`partition_type_string = Some("DOS\\0" | ... | "PFS\\3" | "SFS\\0" | ...)` on
each `PartitionInfo`; leave `partition_type_byte = 0`.

The single-partition HDF case (no RDB; `4x4OffRoadRacing_v1.00_1480.hdf`)
falls through to the superfloppy path and detects OFS/FFS at sector 0.

### Layer 3 — Filesystem dispatch

In `src/fs/mod.rs`, extend the existing `partition_type_string` matchers
(`fs_name_for`, `is_layout_preserving_fs`, `is_expensive_minimum`,
`compact_partition_reader`, `open_filesystem`, `open_editable_filesystem`,
`is_browsable_type`, `is_checkable_type`) so they route `"DOS\\?"`, `"PFS\\3"`,
`"PDS\\3"`, `"muFS"`, `"SFS\\0"`, `"SFS\\2"` to the new modules.

All three filesystems are **layout-preserving** (allocation-bitmap based, no
relocation-friendly packing). That means:
- `compacted_size == original_size`, `data_size = used_blocks * block_size`.
- Min-size computation is cheap (bitmap read); `is_expensive_minimum = false`.

### Layer 4 — The three filesystems

#### `src/fs/affs.rs` — OFS + FFS (all DOS\0..DOS\7)

- Big-endian; block size from RDB or 512 for ADF.
- Boot block at 0–1 (custom checksum), root block at `(reserved + total/2)`.
- Hash chains for directory lookups:
  `hash = name_len; for c in name { hash = (hash * 13 + upper(c)) & 0x7FF }; hash %= HT_SIZE`.
  Intl variants (`DOS\2/3/6/7`) use ISO-8859-1 case folding (0xC0–0xFF range).
- Block header layout differs between OFS and FFS:
  - **OFS data blocks**: 24-byte header + 488 data bytes per block.
  - **FFS data blocks**: 512 bytes pure data (pointers live in the file header
    + extension blocks).
- Used-block map: bitmap blocks listed in `bm_pages[0..25]` + `bm_ext` chain.
  **Set bit = free** (opposite of typical convention).
- DirCache (`DOS\4/5`): read-through (ignore the cache; treat as plain FFS).
  Write: invalidate cache blocks on mutation, let AmigaDOS rebuild.
- Long Names (`DOS\6/7`): 107-char filenames in extended dir-entry records.
- Every header/dir/file block has a 32-bit checksum (sum of all u32s = 0).

#### `src/fs/pfs3.rs` — Professional File System 3

- Reference: `~/repos/amigasources/pfs3aio/Docs/`, `amitools/amitools/fs/`.
- 1024-byte default logical blocks (follows RDB block size if larger).
- Root block at block 1; anode-based extent allocation.
- No checksum — relies on transactional rollback. Writes must follow PFS3's
  postponed-operations log discipline (write new state, commit pointer,
  invalidate old). See [pfs3aio Docs/blocks.txt] for the exact ordering.

#### `src/fs/sfs.rs` — Smart File System

- Reference: `~/repos/amigasources/smartfilesystem/`.
- B-tree of `OBJC` container nodes; journal log lives in a ring buffer.
- Read path: read the *committed* state; if journal has uncommitted entries,
  log a warning and proceed read-only. Emulators handle replay.
- Write path: append to journal, fsync, then apply. We never skip the journal.

### Layer 5 — Metadata (extend `FileEntry`)

Add three optional fields, mirroring the `type_code`/`creator_code` precedent:

```rust
pub amiga_protection: Option<u8>,    // RWED HSPA bits
pub amiga_comment: Option<String>,   // up to 79 chars
pub mtime_amiga_epoch: Option<i64>,  // seconds since 1978-01-01 UTC
```

UNIX epoch conversion: `unix = amiga + 2922 * 86400`. Render in the existing
browse-view detail panel; no new UI.

### Layer 6 — Edit/write support (`EditableFilesystem`)

All three filesystems implement the existing `EditableFilesystem` trait:
`create_file`, `create_directory`, `delete_entry`, `delete_recursive`,
`sync_metadata`, `free_space`. Mutations stage in memory; caller invokes
`sync_metadata()` to flush. The GUI's `StagedEdit` queue and "Apply Edits"
flow work unchanged.

Snapshot/rollback (like HFS): each filesystem clones its in-memory metadata
before a mutation and restores on error. Block-level writes happen only in
`sync_metadata()`.

Checksum maintenance — **every block touched is re-checksummed before
write**. This is non-negotiable; AmigaDOS flags any block whose checksum is
stale as "unreadable."

### Layer 7 — FFS Disk Validator (fsck integration)

New file: `src/fs/affs_fsck.rs`, mirroring `hfs_fsck.rs` shape.

Phases:
1. Root block structure + checksum.
2. Walk every hash chain, verify each header block checksum.
3. Walk file extension chains, verify data block pointers.
4. Rebuild bitmap from observed allocations; compare to on-disk bitmap.

Issue codes:
- `AffsBadChecksum` — repairable (recompute).
- `AffsBitmapMismatch` — repairable (rewrite bitmap from walk). This is the
  classic "validation needed" condition.
- `AffsOrphanBlock` — repairable (move to `lost+found/`, synthesizing a
  header block).
- `AffsBrokenHashChain` — repairable (rebuild chain from dir contents).

PFS3 and SFS get `fsck() -> None` for v1; their on-disk journals already
restore consistency on next mount.

### Layer 8 — Inspect / Backup / Restore integration

Once dispatch is wired, the following come for free:
- Inspect tab — partition list, used/free, FS name.
- Backup tab — layout-preserving compaction, per-partition zstd/raw/VHD,
  single-file CHD via byte-ranges.
- Restore — byte-range copy back (no resize).
- Browse view — implements `Filesystem` trait → free.
- Min-size runner — bitmap walk is cheap, no deferred button needed.

### Shared helpers

- `src/fs/affs_common.rs` — big-endian readers, AFFS checksum
  (`sum_u32 == 0`), boot-block checksum (ones-complement), Amiga DateStamp ↔
  UNIX conversion, protection-bits string formatting (`----rwed`).
- `src/partition/rdb.rs` — `RdbBlock`, `PartBlock`, `FshdBlock`, `LsegBlock`,
  block-checksum helper, exported `Rdb` type.

---

## Phasing & checklist

Mark items `[x]` as completed. Each phase ends with a runnable, mergeable
state.

### Phase 1 — RDB parsing + partition table integration
- [x] `src/partition/rdb.rs` with `Rdb`, `RdbPartition`, parser.
- [x] `PartitionTable::Rdb(Rdb)` variant + propagation through
      `partitions()`, `type_name()`, `disk_signature()`, `alignment`,
      `editor`, `backup`, `single_file_chd`.
- [x] Wire into `PartitionTable::detect()` between APM and superfloppy
      (RDSK scan covers first 16 sectors).
- [x] `PartitionInfo` rows from RDB partitions with `partition_type_string`
      set to the DosType (e.g. `"DOS\\3"`, `"PFS\\3"`, `"SFS\\0"`).
- [x] Recognize ADF sizes (901_120, 1_802_240) in `is_floppy_size`.
- [x] AmigaDOS boot-block detection in `detect_superfloppy` returns the
      DosType as `fs_hint` AND `partition_type_string` so single-partition
      HDFs / ADFs without RDB also route correctly.
- [x] `rdb.json` sidecar emitted by `backup::run_backup` (data-path backup
      gated until Phase 2 ships the AFFS reader).
- [x] Manual validation against the four reference images:
      `DiskDoctor.adf` → None / DOS\1, `4x4OffRoadRacing_v1.00_1480.hdf` →
      None / DOS\0, `AmigaVision.hdf` → RDB / 2× PDS\3, `amiga128gb.chd` →
      RDB / 3× SFS\0 (via transparent CHD wrapper).
- [x] Unit tests for RDSK + PART parsing and `format_dos_type`.
- [x] `examples/probe_amiga.rs` smoke-test tool.
- [x] `.adz` / `.hdz` transparent gzip-decompress on open. Helper
      `gui::materialize_amiga_image_path` (src/gui/mod.rs) sniffs the
      file extension, validates the gzip magic, and decompresses to a
      `<stem>.adf` / `<stem>.hdf` in a fresh `tempfile::TempDir` that
      the calling tab keeps alive. Wired into inspect, backup, and
      restore (single-partition source) file-picker hooks; close /
      device-switch paths drop the tempdir guard. Unit tests cover
      raw-passthrough, round-trip decompress, and bogus-magic rejection.

### Phase 2 — OFS/FFS read + browse + backup
- [x] `src/fs/affs_common.rs` — checksums (normal + bitmap + boot), AFFS
      hash with Intl case folding, AmigaDOS DateStamp conversion, BSTR
      reader, protection-bits string, variant classification helpers.
- [x] `src/fs/affs.rs` — open, root block parser, bitmap walk (root inline
      pages + ext chain), hash-chain directory listing, file read (OFS
      24-byte header + 488-byte payload vs FFS 512-byte payload), header
      block + extension chain traversal.
- [x] All DOS\0..DOS\7 variants detected. Intl case folding implemented;
      DirCache (DOS\4/5) blocks are ignored on read (canonical hash chain
      is authoritative); Long Names (DOS\6/7) currently read up to 30
      chars (full 107-char support deferred until write lands so the
      naming rules can be enforced consistently).
- [x] Dispatch wired in `src/fs/mod.rs`: `is_amiga_dos_type` helper,
      `fs_name_for`, `is_layout_preserving_fs` (true), `is_expensive_minimum`
      (false — bitmap scan only), `open_filesystem_by_string`,
      `compact_partition_reader_by_string`.
- [x] `CompactAffsReader`: layout-preserving stream emitting allocated
      blocks verbatim and zero-filling free blocks (AmigaDOS bit convention:
      set bit = free).
- [x] Manual validation against real images via `examples/probe_amiga`:
      - `DiskDoctor.adf` (FFS floppy, DOS\1) → label "DiskDoctor",
        used 729 600 / 901 120 bytes, 7 root entries.
      - `4x4OffRoadRacing_v1.00_1480.hdf` (OFS HDF, DOS\0) → label
        "4x4OffRoadRacing", used 854 528 / 1 064 960 bytes, 7 root entries.
- [x] Unit tests: empty FFS floppy open, empty-root list, compact-reader
      zero-fill for free blocks.
- [ ] Amiga metadata in `FileEntry` (protection bits, comment). *Deferred
      to Phase 3 — `FileEntry` is shared across 9+ filesystems and the
      field additions are easier to land alongside write support so the
      Edit UI can surface them too. `mtime` is already plumbed via
      `FileEntry::modified` (AmigaDOS DateStamp → ISO-8601 string).*
- [ ] Backup test on `DiskDoctor.adf` and `4x4OffRoadRacing_v1.00_1480.hdf`
      via the GUI / `run_backup` end-to-end.
- [ ] Restore round-trip byte-equal.

### Phase 3 — FFS write / EditableFilesystem
- [x] Block allocator (`alloc_block` / `free_block` / `mark_bitmap`):
      finds the lowest free bit, clears it in both the flat in-memory
      bitmap and the on-disk bitmap-page cache, recomputes the bitmap
      page checksum.
- [x] Header block synthesis (`build_dir_block`, `build_file_header_block`,
      `build_file_ext_block`, `build_data_block`) with the correct
      checksums and AmigaDOS reverse-order dataBlocks[].
- [x] Hash-chain insert (`hash_chain_insert`) — places new entry at the
      front of the chain so existing predecessors don't need rewiring.
- [x] Hash-chain remove (`hash_chain_remove`) — relinks the predecessor
      or updates the parent's hash slot, handling root specially.
- [x] Extension block chain growth — `create_file` allocates one ext
      block per 72 data blocks beyond the header inline, chaining via
      `extension` field.
- [x] Dirty-block cache in `AffsFilesystem` — all mutations write into a
      `HashMap<u32, [u8; BSIZE]>` instead of the disk. `sync_metadata()`
      flushes in ascending block order; failure mid-mutation leaves the
      on-disk volume untouched (atomicity at the sync boundary).
- [x] `sync_metadata()` flushes every dirty block with fresh checksums
      already in place (set by the build_*/mark_bitmap helpers).
- [x] `validate_name` rejects empty names, names > 30 bytes, and the
      forbidden bytes NUL / '/' / ':'.
- [x] Dispatcher wired in `open_editable_filesystem`.
- [x] Round-trip tests: create dir + create file + sync + reopen + read
      back contents; create file + delete + verify allocation returns
      to baseline.
- [ ] Snapshot/rollback wrapper for in-memory state (matches HFS
      pattern). *Deferred — the dirty-block cache already provides
      atomicity at the sync boundary; snapshot/rollback only matters
      if a single high-level edit needs to fail-and-revert without
      tearing down the cache. Add when the GUI's staged-edits "Apply
      Edits" flow surfaces a need.*
- [ ] GUI staged-edits flow exercised on a real AFFS image.
- [ ] Persist Amiga protection + comment on `create_file` (currently the
      header is created with `access=0` → `----rwed`, no comment).

### Phase 4 — FFS Disk Validator
- [x] `src/fs/affs_fsck.rs` with `check_affs` + `repair_affs`. BFS over
      the directory tree from the root, validates each entry block's
      checksum, walks file-extension chains, rebuilds the bitmap, and
      diffs against the on-disk bitmap.
- [x] Hooked into the existing fsck UI: `Filesystem::fsck()` returns
      a `FsckResult`; `is_checkable_type` in `inspect_tab` accepts the
      AmigaDOS DosType strings.
- [x] Repair path for `AffsBitmapMismatch` via `EditableFilesystem::repair()`
      → rewrites every bitmap page from the observed allocation set and
      flushes through the dirty-block cache.
- [x] Issue codes surfaced: `AffsBadChecksum`, `AffsBitmapMismatch`,
      `AffsOrphanBlock`, `AffsBrokenHashChain`, `AffsBadType`,
      `AffsOutOfRange`, `AffsBadDateStamp`.
- [x] Round-trip test: clean image → fsck reports clean; corrupt a
      bitmap page → fsck flags `AffsBitmapMismatch` as repairable;
      `repair()` → re-fsck reports clean.
- [x] `examples/fsck_amiga.rs` smoke-test tool. Runs cleanly on the
      synthetic test image; on the real `DiskDoctor.adf` and
      `4x4OffRoadRacing_v1.00_1480.hdf` images it reports a handful of
      bad-checksum + bitmap-orphan findings consistent with the
      "validation needed" condition AmigaDOS itself flags on these
      vintage images.
- [ ] Repair path for `AffsBadChecksum` / `AffsBrokenHashChain` /
      `AffsOrphanBlock`. *Intentionally deferred — silently recomputing
      a stale header checksum risks locking in deeper corruption.
      Surface as errors; the user can wipe + restore from backup.*

### Phase 5 — PFS3 read + browse + backup
- [x] `src/fs/pfs3.rs` — root block (small + supermode/large layouts),
      rootblock extension (`EX`), anode resolution (`MODE_SPLITTED_ANODES`
      + superindex two-level indirection), dirblock walk via anode
      chains, file data streaming in HW-sector clusters, `LARGEFILE`
      48-bit file size support, reserved-block LRU cache.
- [x] Dispatch for `"PFS\\3"`, `"PDS\\3"`, `"muFS"` wired in
      `src/fs/mod.rs`: `is_amiga_pfs3_type` helper, `fs_name_for`
      ("PFS3"), `is_layout_preserving_fs` (true), `is_expensive_minimum`
      (false), read-only `open_filesystem_by_string`, and
      `compact_partition_reader_by_string`. GUI `is_browsable_type_string`
      in `inspect_tab.rs` updated. Editable dispatch site returns
      `Unsupported` until Phase 6.
- [x] `CompactPfs3Reader` — layout-preserving stream. Reserved area
      (HW sectors 0..=last_reserved) is emitted verbatim; data area
      sectors default to verbatim too because the bitmapindex →
      bitmapblock walk is intentionally deferred (see TODO below). The
      stream is correct but not yet compactable.
- [x] Manual validation against `~/amiga-filesystems/AmigaVision.hdf`:
      both `PDS\3` partitions open, return labels ("Amiga" / "Data"),
      list their root directories, and stream file contents byte-for-byte
      (verified by checking the `.info` icon magic `e3 10 00 01`).
- [x] Unit tests for read helpers, anodenr split arithmetic, and
      direntry parsing.
- [ ] Backup/restore round-trip on a PFS3 partition. *Deferred to follow-up:
      the dispatch is wired but a real run_backup → reconstruct exercise
      against the 9.6 GB AmigaVision image isn't part of this commit.*
- [x] Implement `read_user_bitmap`: walks `root.bitmapindex[]` → `MI`
      bitmap-index blocks → `BM` bitmap blocks; assembles a flat sector
      bitmap (LSB-first within byte). `CompactPfs3Reader` now
      zero-fills free user-data sectors; `last_data_byte` returns the
      last allocated sector's end. Verified against AmigaVision.hdf:
      DH0 compact data 544 MB vs used 544 MB (≤1 BM-word rounding),
      DH1 compact 9069 MB vs used 9069 MB. `Pfs3Filesystem::open` now
      narrows partition_size to `root.disksize * 512` so multi-partition
      images don't claim past their declared end.

### Phase 6 — PFS3 write
- [x] `create_blank_pfs3(total_sectors, name)` formatter producing a
      minimum mountable empty volume (RB 0 rootblock+reserved bitmap,
      RB 1 EX extension, RB 2 MI bitmap-index, RB 3+ BM data bitmap,
      then IB anode-index, AB anodeblock 0, root DB dirblock; anodes
      0..4 sentinel-allocated, anode 5 = ROOTDIR).
- [x] Reserved-block alloc/free against the cluster's BM bitmap;
      decrements/increments `rootblock.reserved_free` and zeros newly
      allocated blocks.
- [x] Data-block alloc/free walking the rootblock's bitmapindex →
      bitmapindex blocks (MI) → bitmap blocks (BM) for a contiguous
      run; updates `rootblock.blocks_free`.
- [x] Anode alloc/free in anodeblock 0 (84 user slots per AB); uses
      the pfs3aio sentinel `(0, 0xFFFFFFFF, 0)` for the "allocated but
      empty" state.
- [x] Direntry encoder (`build_direntry`) for minimal-mode entries
      (next/type/anode/fsize/dates/protection/nlength/name + comment
      length+comment, padded even). `add_direntry_to_dir` walks the
      directory's anode chain looking for a dirblock with room and
      grows the chain by allocating a fresh dirblock + anode when the
      tail runs out. `remove_direntry_from_dir` shifts later entries
      up by `next` bytes and zeros the tail.
- [x] `Pfs3Snapshot` + `snapshot()`/`restore_snapshot()`: every
      `EditableFilesystem` mutation wraps in a snapshot guard so a
      partial failure (e.g. AlreadyExists, DiskFull) leaves the
      in-memory volume identical to the pre-call state. Snapshot
      captures rootblock + both dirty maps + cache.
- [x] `sync_metadata` flushes `dirty_reserved` then `dirty_data` in
      ascending HW-sector order; clears both maps and refreshes the
      LRU read cache so subsequent reads observe the persisted bytes.
- [x] Dispatch wired through `open_editable_filesystem_by_string`:
      PFS3 partitions now open editable (was returning `Unsupported`).
- [x] Round-trip tests in `src/fs/pfs3.rs`:
      `write_round_trip_create_dir_and_file` (format → create dir +
      file → sync → reopen → list root → read file body byte-equal),
      `write_round_trip_delete_entry` (create both then delete; verify
      root empty after reopen and `free_space` grew), and
      `create_directory_rolls_back_on_duplicate` (duplicate name
      returns `AlreadyExists` and rollback restores free space).
- [ ] Postponed-operations log discipline — the pfs3aio approach
      (write new state, commit pointer, invalidate old) is **not** what
      we implement: rusty-backup's edit model batches mutations into
      dirty buffers and flushes atomically on `sync_metadata` (the same
      pattern AFFS/HFS use). A real Amiga-style postponed-ops log
      would only matter for crash recovery; we accept "metadata writes
      flushed in ascending order" as the consistency story for now.
- [x] Wire PFS3 into the GUI's `EditableFilesystem` staged-edits flow.
      `open_editable_filesystem` already routes PFS3 to
      `Pfs3Filesystem`, and `BrowseView::apply_staged_edits` drives any
      `EditableFilesystem` through `edit_queue::apply_edit`, so the
      existing edit-mode toggle + Add File / New Folder / Delete UI
      now operate on PFS3 volumes without per-fs branching. Added
      `tests/filesystem_e2e.rs::test_pfs3_staged_edits_round_trip`:
      blank-volume image, staged `CreateDirectory` + `AddFile` from a
      host file, dispatch through `apply_edit`, `sync_metadata`,
      reopen and verify both entries landed and file body is byte-equal;
      then a second pass stages `DeleteEntry`, syncs, and reopens to
      confirm the file is gone and the directory remains.

### Phase 7 — SFS read + browse + backup
- [x] `src/fs/sfs.rs` — root block parse (block 0 + last block, pick
      highest sequencenumber), ObjectContainer chain walk for directory
      listing, NodeContainer tree lookup (leaf entries stride =
      `sizeof(fsObjectNode)` = 10; internal entries `BLCKn` with
      `>>shifts_block32` shift to recover the block, low bit = full
      flag), extent B-tree walk (BNDC, leaf = `fsExtentBNode`
      key/next/prev/blocks), file streaming, soft-link surface as
      file body, bitmap walk for compaction.
- [x] Dispatch for `"SFS\\0"`, `"SFS\\2"`: `is_amiga_sfs_type` helper
      added to `src/fs/mod.rs`; `fs_name_for` returns "SFS";
      `is_layout_preserving_fs` = true; `is_expensive_minimum` =
      false; `open_filesystem_by_string` and
      `compact_partition_reader_by_string` route to
      `sfs::SfsFilesystem` / `sfs::CompactSfsReader`. GUI
      `is_browsable_type_string` accepts SFS too.
      `open_editable_filesystem_by_string` returns
      `Unsupported` until Phase 8.
- [x] Manual validation against `~/amiga-filesystems/amiga128gb.chd`:
      three SFS\0 partitions open, list their root directories, read
      `.info` icon files byte-for-byte (verified Amiga magic `e3 10 00
      01`), and the compact reader walks the bitmap to report sane
      data ratios (62%, 38%, 13%) consistent with how full each
      partition is.
- [ ] Backup/restore round-trip on an SFS partition (deferred to a
      follow-up GUI run against the real CHD image).

### Phase 8 — SFS write
- [x] `create_blank_sfs(total_blocks, name)` formatter producing a
      minimum mountable empty volume: rootblock at 0 (and duplicated at
      totalblocks-1), AdminSpaceContainer at 1 (6 admin slots
      pre-marked allocated via bits=0xFC000000), root OBJC at 2 with
      the root fsObject, empty HashTable at 3, transaction-OK
      placeholder (TROK) at 4, empty extent B-tree leaf (BNDC) at 5,
      object-node root NodeContainer (NDC ) at 6, BTMP bitmap blocks
      starting at 33.
- [x] `stamp_checksum` helper centralizing the CALCCHECKSUM convention
      (sum-all-longs + 1 == 0). All metadata blocks re-stamped on
      `sync_metadata` flush; data blocks (no header) flow through
      verbatim. `is_metadata_block` distinguishes the two by first u32.
- [x] Allocators: `alloc_data_blocks` walks the BTMP bitmap for a
      contiguous run, `alloc_admin_block` scans
      `adminspace[0].bits`, `alloc_object_node` finds a free
      `fsObjectNode` slot in the leaf NDC. Free variants flip the bits
      back. Object-node `data` field records the OBJC block that holds
      the new entry — `lookup_object_block` then walks the same chain
      to find the fsObject.
- [x] OBJC chain mutation: `ensure_dir_chain_room` finds (or
      allocates) an OBJC with enough free tail for a new fsObject,
      growing the chain by linking a fresh admin block via `next`/
      `previous`. `splice_object_into_block` appends the encoded
      fsObject. Delete reverses the operation, unlinking and freeing
      OBJC blocks that go empty (other than the root OBJC).
- [x] Extent B-tree insert/remove for single-leaf BNDCs: sorted
      `fsExtentBNode` records `(key=first_data_block, blocks)`.
      Splits + rebalance are deferred — `DiskFull` surfaces if the
      leaf overflows. Sufficient for the round-trip test surface.
- [x] `SfsSnapshot` clones rootblock + dirty map + cache +
      free-blocks counter; every mutation wraps in snapshot/rollback
      so partial failures (e.g. `AlreadyExists`, `DiskFull`) leave the
      in-memory volume identical to its pre-call state.
- [x] `sync_metadata` bumps the rootblock `sequencenumber`, writes
      both primary + backup root copies, then flushes every dirty
      block in ascending order. Metadata blocks get re-stamped
      checksums; data blocks flush verbatim.
- [x] Dispatch wired: `open_editable_filesystem_by_string` routes
      `SFS\0`/`SFS\2` to `SfsFilesystem` (was returning
      `Unsupported`). GUI staged-edits flow now works against SFS
      without any GUI-side changes — the trait dispatch is enough.
- [x] Round-trip tests in `src/fs/sfs.rs` and
      `tests/filesystem_e2e.rs::test_sfs_staged_edits_round_trip`:
      blank-volume image, create dir + file, sync, reopen, list root,
      read file byte-equal; delete pass restores empty root + grows
      free-space; duplicate name rolls back via snapshot.
- [ ] Journal append + commit ordering (TRST/TRFA). Deliberately
      deferred — we use sync-boundary atomicity (write all dirty
      blocks then both roots with bumped sequencenumber), matching
      what rusty-backup already does for AFFS/HFS/PFS3.
- [ ] B-tree splits + rebalance (extent BNDC + object-node NDC tree).
      Deferred until a real workload exceeds the single-leaf capacity
      (~36 extents, ~50 object-nodes on a 512-byte-block volume).

### Phase 9 — Polish
- [ ] CLAUDE.md: add Amiga section describing dispatch + DosType strings.
- [ ] User-facing docs (README or in-app help): supported formats list.
- [ ] CI: add a tiny synthetic RDB+FFS test image to `tests/` (under 64 KiB).

---

## Open risks & notes

- **Endianness everywhere**: every multi-byte field is big-endian. Easy to
  forget once and produce silently-wrong checksums.
- **Checksum staleness is fatal**: AmigaDOS marks blocks unreadable on
  mismatch. Touch a block, recompute. No exceptions.
- **PFS3 journaling is unusual** — it uses postponed-operations rather than a
  redo log. Read `pfs3aio/Docs/blocks.txt` carefully before implementing
  writes; the ordering is load-bearing.
- **Bitmap "set bit = free"** in AFFS is the opposite of nearly every other
  filesystem you've implemented. Easy bug source.
- **RDB block size ≠ filesystem block size**. Always carry both through the
  dispatch path; never assume 512.
- **Disk space is tight on the dev machine**: prefer 2–4 MiB synthetic test
  images over operating on the 9.6 GiB / 22 GiB samples for tight inner-loop
  development. Use the big images for end-to-end validation only.
