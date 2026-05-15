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
- [ ] `.adz` / `.hdz` transparent gzip-decompress on open. *(Deferred to
      Phase 2 — needs to hook the GUI file-open paths in inspect/backup/
      restore tabs rather than the parser, since `PartitionTable::detect`
      already works on any `Read + Seek` stream.)*

### Phase 2 — OFS/FFS read + browse + backup
- [ ] `src/fs/affs_common.rs` — checksums, big-endian helpers, DateStamp.
- [ ] `src/fs/affs.rs` — open, root block, bitmap walk, hash-chain
      directory listing, file read (OFS 24-byte header vs FFS 512-byte data).
- [ ] All DOS\0..DOS\7 variants detected; Intl case folding correct.
- [ ] Amiga metadata in `FileEntry` (protection, comment, mtime).
- [ ] Dispatch wired in `src/fs/mod.rs`: name, layout-preserving=true,
      expensive-min=false, browsable, compactor.
- [ ] Compaction reader: zero-stream unused blocks (layout-preserving).
- [ ] Backup test on `DiskDoctor.adf` (FFS floppy) and
      `4x4OffRoadRacing_v1.00_1480.hdf` (OFS single-partition HDF).
- [ ] Restore round-trip byte-equal.

### Phase 3 — FFS write / EditableFilesystem
- [ ] Block allocator (find free bit in bitmap, mark, recompute bitmap
      checksum).
- [ ] Header block synthesis (file/dir) with correct hash insertion.
- [ ] Hash-chain delete (relink predecessor).
- [ ] Extension block chain growth/shrink.
- [ ] Snapshot/rollback wrapper around every mutation.
- [ ] `sync_metadata()` flushes all dirty blocks with fresh checksums.
- [ ] GUI staged-edits flow works on an AFFS image.
- [ ] Round-trip test: add file → close → reopen → file is there with
      correct contents, protection, comment, mtime.

### Phase 4 — FFS Disk Validator
- [ ] `src/fs/affs_fsck.rs` with four phases.
- [ ] Hook into existing fsck UI (per-partition Check button).
- [ ] Repair path for `AffsBitmapMismatch`, `AffsBadChecksum`,
      `AffsBrokenHashChain`, `AffsOrphanBlock`.
- [ ] Test on a deliberately-corrupted copy of `DiskDoctor.adf`.

### Phase 5 — PFS3 read + browse + backup
- [ ] `src/fs/pfs3.rs` — root block, anode B-tree, dir block reading,
      file extent reading, bitmap.
- [ ] Dispatch for `"PFS\\3"`, `"PDS\\3"`, `"muFS"`.
- [ ] Backup/restore round-trip on a PFS3 partition inside
      `AmigaVision.hdf` (or `amiga128gb.chd` if present there).

### Phase 6 — PFS3 write
- [ ] Anode allocation + free-list management.
- [ ] Postponed-operations log discipline (write new, commit, invalidate old).
- [ ] Snapshot/rollback.
- [ ] Round-trip add/delete file test.

### Phase 7 — SFS read + browse + backup
- [ ] `src/fs/sfs.rs` — `OBJC` B-tree, journal recognition (read-only on
      uncommitted journal with a warning).
- [ ] Dispatch for `"SFS\\0"`, `"SFS\\2"`.
- [ ] Backup/restore round-trip on an SFS partition.

### Phase 8 — SFS write
- [ ] Journal append + commit ordering.
- [ ] B-tree insert/delete with rebalance.
- [ ] Snapshot/rollback.
- [ ] Round-trip test.

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
