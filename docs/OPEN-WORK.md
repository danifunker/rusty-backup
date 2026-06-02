# Open Work — Master Plan

The single source of truth for what's left to do on Rusty Backup. Each
section carries enough design detail to be actionable without external
references. The MiSTer plan
([`mister_filesystem_implementation_plan.md`](mister_filesystem_implementation_plan.md))
is the one other living plan and is treated as a single line item here;
everything else has been consolidated into this file.

Last reconciled against the code: 2026-06-02.

When an item lands, remove its block. When new work surfaces, add a block
here.

---

## 1. Filesystem engine — Unix priority track

The three Unix filesystems we want soon. ReiserFS is on the kernel removal
track and should land before it disappears from upstream distros; UFS and
JFS unblock BSD/Solaris/OS-2 workflows. **Sequence: ReiserFS → UFS → JFS**.
Within each, **Tier A** (detect + size + compact, the bitmap-only floor) is
the high-value slice that lights up backup + restore + inspect even without
browse; **Tier B** (inode + dir + file walk) follows for `rb-cli ls` / `get`
and the GUI browse tab.

Magic dispatch order (`probe_0x83_fs_type` in `src/fs/mod.rs`) should remain
**content-magic first, partition-type-byte second**; all four (ext, btrfs,
xfs, reiserfs/ufs/jfs) share MBR type `0x83` on Linux.

### 1.1 ReiserFS

- **Scope**: v3.5 (`ReIsErFs` magic) + v3.6 (`ReIsEr2Fs` magic). Reject
  reiser4 with a clear error — wholly different on-disk format, extinct,
  partimage never handled it either. **Read-only** — no edit, no fsck, no
  resize (the filesystem is leaving the kernel; investment caps at "read
  what exists").
- **References**: `partimage-0.6.9/src/client/fs/fs_reiser.{cpp,h}` for Tier
  A (port directly); `reiserfsprogs` (`debugreiserfs`, `reiserfsck`) for
  Tier B's S+tree walker; Linux kernel `fs/reiserfs/reiserfs_fs.h` for
  authoritative on-disk structs.
- **Bitmap polarity**: confirm against a real image before committing — most
  Linux FS use set = allocated, but CLAUDE.md notes the Amiga / EFS "set =
  free" trap; ReiserFS is presumed Linux-standard but verify.

| # | Tier | Deliverable |
|---|---|---|
| ~~R.1~~ | A | **Shipped.** `src/fs/reiserfs.rs` parses the superblock at byte 65536 with magic dispatch (v3.5 / v3.6 / reject-reiser4), reads `s_block_count` / `s_free_blocks` / `s_blocksize` / `s_bmap_nr` / `s_label`, and is wired through `detect_filesystem_type`, `probe_0x83_fs_type` ("ReiserFS"), and both `open_filesystem` dispatch arms (type 0x83 + auto-detect). Inspect tab shows the right type + sizes. 9 unit tests cover both magics, the reiser4 reject path, block-size validation, label parsing, partition-offset threading, and `probe_0x83_fs_type` routing. Gated test against a real `mkfs.reiserfs` fixture still pending. |
| ~~R.2~~ | A | **Shipped.** `src/fs/reiserfs.rs` adds `read_bitmap_block` + `bitmap_valid_bits` helpers (bitmap 0 at sb_block+1, bitmap i≥1 at i*blocks_per_bitmap), `Filesystem::last_data_byte` override (scans bitmaps from end via `highest_set_bit`), and a layout-preserving `CompactReiserFsReader` that coalesces same-state bitmap runs into `MappedBlocks` / `Zeros` sections. Bitmap polarity is **set = allocated** (standard Linux). Wired into `compact_partition_reader` for both auto-detect and 0x83 dispatch arms; re-exported as `crate::fs::CompactReiserFsReader`. 6 new tests: `last_data_byte` with extras / reserved-only / empty-bitmap / multi-bitmap-block, plus a layout-preserving stream check and a round-trip-through-parser test. Gated test against a real `mkfs.reiserfs` fixture still pending. |
| R.3a | B | **Shipped.** On-disk S+tree node parsers in `src/fs/reiserfs.rs`: `BlockHead` (level + nr_item + free_space + right_delim_key, 24 bytes); `Key` (16-byte raw + `KeyFormat`-aware `dir_id` / `objectid` / `offset` / `item_type` accessors handling both v1 sentinel-u32 and v2 packed-u64 layouts); `ItemHead` (24-byte leaf header with `version` → `key_format`); `DiskChild` (8-byte internal-node child pointer). `parse_leaf_item_heads` / `parse_internal_keys_and_children` walk a whole block and assert the B-tree N+1 invariant. Superblock `root_block` is now parsed and stored. 13 new tests cover both key formats, both item-type decode tables, BlockHead leaf/internal, ItemHead, DiskChild, leaf-of-3 + internal-with-2-keys round trips, and the leaf/internal mismatch rejections. |
| R.3b | B | Tree walker: `find_leaf_for_key(key)` descends from `root_block` by binary-searching internal nodes; `range_scan(min, max)` iterates items in sorted order. Reads tree nodes via the existing partition reader. |
| R.3c | B | `list_directory`: starting from inode 2 (root dir), walk `DirEntry` items, parse the entry-array + name-area structure, return `FileEntry`s. |
| R.3d | B | `read_file`: walk `StatData` for size, then `Indirect`/`Direct` items for file body, including **tail packing** — small files (<= a few KB) keep their tail in a `Direct` item co-located with the stat data. |

### 1.2 UFS

- **Scope**: UFS1 (4.2BSD/FFS — SunOS 4, Solaris 2, NetBSD, OpenBSD, FreeBSD
  ≤4) and UFS2 (FreeBSD 5+). Read + size on first pass; edit + fsck gated
  on real demand. **Reject** softupdate-journaled and SU+J **dirty**
  volumes with a clear message rather than risk corrupting recovery state.
- **References**: `partimage-0.6.9/src/client/fs/fs_ufs.cpp` for Tier A;
  FreeBSD `sys/ufs/ffs/fs.h` + `sys/ufs/ufs/dinode.h` for on-disk structs;
  `ufstools` (`ufstool`, `fsck_ffs`) as oracle.
- **Internal precedent**: `src/fs/efs.rs` + `src/fs/unix_common/` — EFS is
  the same cylinder-group + inode-bitmap + classic-Unix-inode shape, and
  the unix_common scaffolding lifts to UFS with little change.

| # | Tier | Deliverable |
|---|---|---|
| U.1 | A | Superblock parser handling UFS1 (`MAGIC=0x011954` at byte 8192) and UFS2 (`MAGIC=0x19540119` at byte 65536). `fs_name_for` returns "UFS1"/"UFS2". Port from `fs_ufs.cpp` + `fs.h`. Inspect tab shows the right type + total/used/free. Unit test against hand-built superblocks; gated test against a `newfs` fixture (FreeBSD VM or `mkfs.ufs` package on Linux). |
| U.2 | A | Walk cylinder groups: each CG header carries pointers to its block bitmap. Sum free + record highest allocated block for `last_data_byte`. `is_expensive_minimum` = false. Implement `compact_partition_reader_by_string`. Round-trip compaction test. |
| U.3 | B | Walk inode table per CG; parse `ufs1_dinode` / `ufs2_dinode` (mode, uid, gid, size, mtime, block pointers). Directory entries are `direct struct` (32-bit ino + 16-bit reclen + 8-bit type + 8-bit namlen + name). `unix_common::unix_entry_from_inode` handles the mapping. Indirect / double-indirect / triple-indirect block walks (familiar pattern from ext2 inode block addressing). Read-only. |
| U.4 | B+ | **Optional follow-up** — fsck + edit (alloc/free + dir insert/remove + snapshot/rollback, mirrors EFS shape). Cylinder-group rotation makes contiguous allocation slightly different from EFS's single-CG case. Defer until U.1-U.3 prove demand. |

### 1.3 JFS

- **Scope**: JFS2 only (the only on-disk version Linux ever shipped; AIX's
  original JFS1 is a different format-id — reject with a clear message).
  Read + size first; edit out of scope until U.4 lands as the warm-up.
- **References**: `partimage-0.6.9/src/client/fs/fs_jfs.cpp` (688 lines) for
  Tier A; `jfsutils` (`jfs_debugfs`, `jfs_fsck`) for B+tree walkers; Linux
  `fs/jfs/jfs_dinode.h`, `jfs_dtree.h`, `jfs_xtree.h`.
- **Note**: even Tier A needs a basic B+tree walker because the Block
  Allocation Map (BMAP) is itself a B+tree of allocation control pages, not
  a flat bitmap.

| # | Tier | Deliverable |
|---|---|---|
| J.1 | A | Parse the Primary Aggregate Superblock at byte 32768 (`s_magic = "JFS1"`, `s_version = 2`; reject v1). Pull `s_size`, `s_bsize`, `s_l2bsize`. Wire dispatch (`0x83` precedence after ext/btrfs/xfs/reiserfs). Inspect tab shows JFS + total size. |
| J.2 | A | Walk the BMAP tree to compute used/free block counts. Port the bitmap traversal from `fs_jfs.cpp`. Implement `compact_partition_reader_by_string` + `last_data_byte`. `is_expensive_minimum` = true on first pass (BMAP walk is heavier than a flat bitmap). |
| J.3 | B | Walk the Fileset Allocation Map (FSAM) to find inode tables, then walk **dtrees** (directory B+trees) and **xtrees** (extent B+trees) per inode. Reference `jfsutils/libfs/`. Read-only. Multi-session. |
| J.4 | B+ | **Parked** — fsck + edit. JFS edit means atomically rewriting three B+trees (BMAP, dtree, xtree) against the journal — comparable scope to a btrfs writer. Revisit only after J.3 ships and there's real demand. |

### Estimated total

~8 sessions ReiserFS Tier A+B, ~6 UFS Tier A+B, ~10-12 JFS Tier A+B. Each
Tier A slice is small (1-2 sessions); fixture generation per FS is a few
minutes in a Linux VM.

---

## 2. Filesystem engine — multi-session existing tracks

### 2.1 XFS — close the v4-edit holes and add v5 write

XFS read + edit + fsck shipped on the `xfs-efs-fsck` track (~10k lines).
Oracle: `xfs_repair -n` clean on every write. Two oracle Docker images
already documented elsewhere: `rusty-xfs-oracle` (xfsprogs 4.9.0, v4, v2
inodes) and `rusty-xfs-oracle-v1` (xfsprogs 3.1.9, V1-inode IRIX disks).
For v5 you need a third image with recent xfsprogs (e.g. ubuntu:22.04).

Tools: `scripts/xfs-oracle.sh`; `examples/xfs_check.rs` (`--repair`),
`xfs_mkdir.rs`, `xfs_mkfile.rs`; and `rb-cli mkdir|put|rm|ls|get IMG[@N]`.
Real disk to test against: `~/Documents/scsi2.raw` (SGI, V1 inodes, XFS at
byte 2097152 / `@1`, partition slot 7). Always operate on a copy.

Open holes in v4 edit (suggested order — easiest/most reusable first):

- **(A) new-chunk inode allocation** — `alloc_inode_slot` currently returns
  `DiskFull` when every inobt chunk is full. Allocate `blocks_per_chunk`
  contiguous blocks (alloc_blocks, respecting `sb_inoalignmt`), initialize
  all 64 dinodes (magic `0x494e`, version from a sibling, `di_mode=0`,
  `di_next_unlinked=NULLAGINO`), add the chunk to the inobt (startino,
  freecount 64, free=all-ones). Reuse the R3 multi-level REBUILD
  (`build_sblock_btree`): gather all chunks + the new one, rebuild the
  inobt, update AGI root/level/count/freecount and `sb_icount`/`sb_ifree`.
  Ref `libxfs/xfs_ialloc.c (xfs_ialloc_ag_alloc)`. Oracle: fill every free
  inode slot, then create one more file/dir → clean.

- **(B) block → short-form dir re-compaction** — cosmetic. After
  `block_remove_entry`, if surviving entries fit the inode literal area,
  convert back to short-form: build the sf fork, free the dir data block
  (`free_blocks`), set `di_format=Local`, `di_size=sf_len`, `nblocks=0`,
  `nextents=0`. Ref `xfs_dir2_block_to_sf`. Oracle: grow a dir to block
  form, delete down to a few entries → expect short-form again, clean.

- **(C) leaf/node (multi-block) directories** — the big directory item.
  Today `dir_insert_entry` returns `DiskFull` when a single-block dir
  overflows one block. Implement block → LEAF conversion: spread entries
  across multiple XD2D data blocks (each with header+bestfree+tag entries)
  plus a leaf index block at file offset `XFS_DIR2_LEAF_OFFSET` (32
  GiB/blocksize) holding the sorted hash-table itself overflows (free-
  index + da-btree node blocks). Update the inode extent map accordingly.
  Reader already parses leaf/node data blocks (`walk_dir2_data_blocks`);
  verify exhaustive listing, then add write support. Ref
  `libxfs/xfs_dir2_block.c`, `xfs_dir2_leaf.c`, `xfs_dir2_node.c`.
  Oracle: add 100s of entries to one directory → list all back, read a
  sample, clean.

- **(D) bmap-btree file forks** — `create_file` caps at one 2²¹-block
  extent and ~9 inline extents (`alloc_extents`). When a file needs more
  extents or >2²¹ blocks, convert the data fork to btree format
  (`di_format=3`): allocate bmbt block(s), write extent records into the
  leaf chain, put a bmbt root in the inode fork, include the bmbt blocks
  in `di_nblocks`. Reader already supports bmbt READ (`walk_bmbt`). Ref
  `libxfs/xfs_bmap_btree.c`. Bonus: once we can enumerate bmbt blocks for
  ownership, relax the R2/R3 "abort on `di_format==3`" gate so repair
  handles fragmented files. Oracle: write a file with >9 extents (heavily
  fragment free space first) → clean, data byte-matches.

- **(E) v5/CRC editing** — largest item. Every write path is v4-only
  today (`is_v5() → Unsupported/skip`) because v5 blocks carry crc32c +
  uuid + owner + blkno/lsn. Add a crc32c (Castagnoli) routine and at every
  write site set CRC (computed over the block with the crc field zeroed) +
  uuid (=sb_uuid) + owner + blkno on: v3 inode core (176-byte core,
  `di_crc`), dir3 data/leaf/node blocks (64-byte hdr), short-form-block
  btrees (`XFS_BTREE_SBLOCK_CRC_LEN=56`), bmbt crc blocks, AGF/AGI, the
  superblock. Thread `is_v5` through `edit.rs` / `repair` (header
  lengths, `fork_offset(true)=176`) instead of hardcoding v4. Open up the
  v5 open-time rejection for editing. Ref `libxfs/xfs_cksum.h` and the
  per-structure crc offsets. Multi-slice; land v5 reads/writes
  structure-by-structure, oracle-checking each.

### 2.2 XFS shrink via clone-into-fresh

Edit shipped, so the prerequisites for a shrink path are in tree. Same
shape as the HFS+ defrag-on-backup pipeline (`hfsplus_defrag.rs` /
`hfsplus_clone.rs`) and the proposed btrfs scratch-recreate.

```
source XFS ──► read every file + xattr via XfsFilesystem
            ──► create_blank_xfs(target_size, label) → fresh mountable v4 image
            ──► replay dirs/files into it via edit.rs
            ──► stream the target image into the compressor
            ──► metadata.json records shrunk=true so restore knows to grow back
```

Restore: write cloned bytes into the (larger) target partition, then run an
in-tree `xfs_growfs`-equivalent (or document the manual `xfs_growfs`
post-restore step the way today's "Add free space" flow already does).

Each slice oracle-validated against `xfs_repair -n`:

| # | Slice |
|---|---|
| XR.0 | **Decision**: v4-only-first vs gate behind hole (E) v5 editing. v4 is enough to land the feature end-to-end on IRIX disks and old Linux; v5 follows when (E) does. |
| XR.1 | `create_blank_xfs(size_bytes, label, sector_size, ag_count) -> Vec<u8>` — emit a minimum-sized fresh v4 XFS that mounts clean and passes `xfs_repair -n`. Mirrors `create_blank_hfsplus` / `create_blank_efs`. |
| XR.2 | `clone_xfs_volume(source, target_size) -> Result<Vec<u8>>` — BFS-replay dirs + files (xattrs once that edit primitive lands; ignore for now or refuse with a clear error). Reuses `edit.rs` write primitives. |
| XR.3 | `stream_xfs_shrunk_to_writer` — peer of `stream_defragmented_hfsplus`: clone into a temp/in-memory target, then stream into the compressor with no double-disk-pass. |
| XR.4 | Pre-flight refusal: v5 sources (until XR.0+E), realtime volumes, multi-device XFS, images with xattrs we can't emit. |
| XR.5 | Backup-engine `shrink_to_minimum` flag wiring for XFS partitions, GUI checkbox + tooltip. Mirrors HFS+ Step 22f-i. |
| XR.6 | End-to-end backup → restore round-trip; restore-side post-write `xfs_growfs`-equivalent or document the manual step. |

Until XR.0 is decided, XFS backups continue through the existing
layout-preserving compactor (output size is fine — zeros compress
trivially; only wall-clock CPU is hurt).

### 2.3 HFS+ journal — Step 27 transactional writes

Journal parse + replay + dirty-volume recovery shipped (Steps 24-26 of the
HFS+ enhancements track). The Step 4 "refuse journaled" relaxation also
landed — `prepare_for_edit` now accepts a journaled volume when its journal
is clean (empty, `start == end`) and still refuses dirty ones.

**What's still open**: route every `do_sync_metadata` block write through
`TransactionBuilder.record_*` so edit-mode mutations land *inside* the
journal rather than writing directly to disk, plus the
`replay_dirty_journal`-on-open hook for `open_editable_filesystem`. The
recorder (`TransactionBuilder` in `src/fs/hfsplus_journal.rs:847`) and
`replay_journal` are in tree from steps 25/26; the plumbing below hasn't
been picked up.

Touch sites in `src/fs/hfsplus.rs`:
- `do_sync_metadata` becomes the natural transaction boundary. Every
  block-level write inside it (`write_fork_data`, B-tree node writes,
  bitmap writes, VH + alt-VH) goes through `pending_tx.record_*` instead
  of writing to disk directly.
- After in-memory mutations succeed, `pending_tx.commit(...)` runs once at
  the end of `do_sync_metadata`. A failure rolls back via the Step 5
  snapshot AND skips the commit, so the journal stays untouched.
- Touch-date stamping (Step 8) and counter updates ride inside the
  transaction.
- `open_editable_filesystem` for HFS+: when `vh.attributes &
  kHFSVolumeJournaledBit != 0`:
  - Clean (`kHFSVolumeUnmountedBit` set) — proceed; transactional writes
    preserve journaling.
  - Dirty + `replay_dirty_journal` on — call `replay_journal` first, then
    proceed.
  - Dirty + not authorized — keep refusing with the Step 4 message.
- Multi-megabyte mutations (defrag-clone in place) stay routed through
  `clone_hfsplus_volume` writing to a **fresh** target with no journal;
  only edit-mode mutations get journaled.

Tests:
- End-to-end edit on a clean journaled HFS+ image: create a file,
  `do_sync_metadata`, reopen with macOS-style replay, assert the file is
  present and the journal is empty.
- End-to-end edit on a dirty journaled image with `replay_dirty_journal=
  true`: replay applies pending transaction, then our own transaction
  lands on top, both visible after a second replay-and-reopen.
- Snapshot rollback during a journaled mutation: forced `DiskFull` →
  assert the journal is byte-identical to before the mutation (no partial
  transaction written).

### 2.4 btrfs — scratch-recreate (gated on writer)

The earlier "trim at last chunk end" idea was abandoned: real-world
layouts have metadata/system chunks placed mid-device, so last-chunk-end
is often close to `total_bytes` even on mostly-free volumes; trimming
past the 64 MiB / 256 GiB superblock mirrors drops redundancy; and
restore-side `resize_btrfs_in_place` would corrupt the volume if any
chunk lies past the trim point (chunk tree still references original
physical offsets).

**Recommended path**: clone the live source into a freshly formatted,
sized-to-fit target btrfs during backup. Same pattern as the HFS+
defrag-on-backup pipeline.

**Hard prerequisite**: a pure-Rust btrfs writer. We have none today —
`src/fs/btrfs.rs` is read-only: superblock + chunk tree + root tree + FS
tree + extents → `Filesystem` reads. No `EditableFilesystem`, no
allocator, no B-tree insert, no chunk tree builder, no CRC stamping for
tree blocks.

Building one is genuinely large (pessimistically larger than the HFS+
edit-mode work) because btrfs is COW with cross-linked B-trees:
chunk tree, extent tree, FS tree, root tree, free-space tree (post-v4.5
default), plus crc32c on every metadata block, plus subvolume layout at
minimum. Multi-month. **No alternative ships it faster** — shelling out
to `mkfs.btrfs` + `btrfs send/receive` is Linux-only and requires root,
losing the Windows/macOS coverage the project's "pure Rust, no platform
cfg" rule (CLAUDE.md) protects.

| # | Slice |
|---|---|
| B.0 | **Decision** — do we want to invest in a btrfs writer at all, or close as "wontfix — back up btrfs verbatim and let users shrink in-OS"? **Needs user input before any code work.** |
| B.1 | `create_blank_btrfs(size, label) -> Vec<u8>` — emit a fresh mountable btrfs. Bootstrap chunk array, system chunk, single data + metadata chunks, empty root/extent/fs trees, free-space tree. Validate with `btrfs check`. |
| B.2 | Single-file write: given a blank target and a `(path, bytes)`, allocate a data extent, write the file, insert INODE_ITEM/DIR_ITEM/DIR_INDEX/EXTENT_DATA, update extent tree back-refs, CRC everything. |
| B.3 | Recursive directory replay: walk source via `BtrfsFilesystem::list_directory` → emit into target. Handle nested dirs + many-file directories (B-tree split). |
| B.4 | `clone_into_blank_btrfs(source, target_size) -> Vec<u8>` + `stream_btrfs_to_target` for streaming into the compressor with no multi-GB temp file. |
| B.5 | Pre-flight refusal for subvolumes / snapshots / compressed extents we can't yet emit, with a clear error. |
| B.6 | Backup engine `shrink_to_minimum` wiring for btrfs partitions (parallel to HFS+ flag); GUI checkbox + tooltip + log polish. |
| B.7 | Restore-side: post-resize `resize_btrfs_in_place` already shipped; verify end-to-end. |

Until B.0 is decided, btrfs backups continue through the existing
layout-preserving compactor.

---

## 3. Filesystem engine — small to medium

### 3.1 ProDOS access-bit setter

ProDOS directory entries carry an *access byte* at offset 30 (read /
write / destroy / rename / backup-needed flags; `$C3` unlocked, `$21`
locked). The codebase reads and writes this byte
(`src/fs/prodos.rs:602, 637, 664`) but the `EditableFilesystem` trait
offers no way to change it after creation.

Add a ProDOS-specific method (mirrors `set_prodos_type`):
```rust
fn set_prodos_access(&mut self, entry: &FileEntry, access: u8)
    -> Result<(), FilesystemError>
```
or a higher-level `set_locked(entry, bool)` if a single toggle covers
the GUI case. **GUI hookup**: new `StagedEdit::SetProdosAccess` variant
in `src/gui/browse_view.rs` (parallel to `SetProdosType`); button gated
on `fs_type == "ProDOS"`. ~150-200 LOC total.

`set_permissions(mode: u32)` is Unix-shaped and only ext implements it;
ProDOS access bits are an 8-bit format with different semantics, so they
can't piggy-back on `set_permissions`.

### 3.2 NTFS file-aware GHO — compressed path

Uncompressed NTFS file-aware Ghost backups work end-to-end via
`src/rbformats/gho.rs` (the `NtfsFileAware` `GhoReaderMode` variant
threads a cluster-run index + inline-MFT fallback through `GhoReader`).
Originally landed in commits `63a57af`, `84e3d26`, `7e29fda`, `b9f8924`
plus visibility changes to `src/fs/ntfs.rs`.

The compressed path bails with `"compressed NTFS not yet supported"`
in `GhoReader::open` (around line 1102, the branch where
`find_inner_stream_start` fails AND `compression != None`). Picking it
up needs:

1. A real compressed NTFS file-aware `.GHO` fixture from a Ghost dump
   (the format is reverse-engineered from uncompressed samples; the
   compressed wrapper format isn't observed yet — likely the same
   Fast-LZ chunking as compressed FAT GHO, but verify).
2. Plumb the compressed-block reader (already shipped for FAT
   file-aware) into the NTFS index builder. The on-disk cluster-run
   format documented in the original NTFS file-aware work is the same;
   only the surrounding container framing changes when compression is
   on.
3. Round-trip test against a known-good Ghost-produced compressed
   image.

The `docs/gho_file_aware.md` reference doc covers the binary format
(kept).

### 3.3 HFS — extending a raw partition image

Investigate how to extend `~/Documents/partition-0.img` (a raw single-
partition HFS image with no APM wrapper). The current expand-HFS path
runs through `emit_apm_disk_with_hfs` in `src/fs/hfs_clone.rs`, which
assumes an APM source.

Step 1: reproduce, confirm whether raw-image expand ever worked or has
always been APM-only. Step 2: either add a non-APM emit path (just
write the cloned HFS image to the output) or document the APM-only
constraint and offer to wrap the source first. The HFV path
(`src/fs/hfv.rs::clone_into_hfv`) is the analogous "bare HFS, no
wrapper" pattern and likely the cleanest reference for what a non-APM
extend would look like.

---

## 4. Clonezilla import — remaining gaps

GPT support shipped. The remaining gaps are LVM and RAID. Both are real
but rare in the project's use case (vintage hardware retro restores);
the motivating workflows are CF/SD cards and single-disk vintage
drives, not modern Linux servers. Open because a future user might hit
them; they sit behind everything in the MiSTer plan and §1-§3 above.

### 4.1 LVM (Logical Volume Manager)

Clonezilla preserves LVM2 metadata in `lvm_*.conf` / `lvm_logv.list` /
`lvm_vg_dev.list` sidecar files plus a `<disk>-<part>.<fs>-ptcl-img.*`
partclone image of the **physical volume** (the partition that backs
the PV header). Today we ignore the LVM sidecars entirely; the
partclone image is treated as raw bytes — restore writes the PV back
verbatim. The LV layout inside the PV is invisible to inspect/browse,
and restore-time resize leaves the VG metadata stale.

Work:
1. Parse `lvm_*.conf` (text format — UUIDs, PV/VG/LV map, extent
   counts).
2. Map each `lv_*` payload partclone image to its parent VG/LV.
3. Surface LVs as logical partitions in inspect (one row per LV inside
   the PV partition).
4. On restore, rewrite PV header `pe_count` and the VG metadata text
   to match the new partition size when the user resizes.

Reference: Clonezilla `ocs-functions` LVM block + `lvmdump` output
format.

### 4.2 RAID (mdadm / Linux software RAID)

Clonezilla images of Linux software RAID arrays write the array members
as separate partitions with type byte `0xFD` (Linux RAID Autodetect) and
a metadata sidecar describing the array shape. Today we detect the
`0xFD` partition type byte in `src/partition/mbr.rs:107` for display
("Linux RAID") but do nothing with the RAID metadata. Restore writes
each member partition back verbatim — fine for RAID-1, meaningless for
RAID-0/5/6 without reassembling the array first.

Work:
1. Parse the RAID sidecar (level, chunk size, member UUIDs, member
   order).
2. Refuse single-member restore for RAID-0/5/6 with a clear "this is
   part of an array; restore needs all N members at once" message.
3. Optional: synthesize an mdadm superblock on restore so the OS
   re-assembles the array on boot.

Reference: Linux `md/raid*.c` superblock format; Clonezilla
`ocs-functions` mdadm dump block.

---

## 5. MiSTer computer-core filesystems

The MiSTer FS plan lives in its own file because of its size (Wave 1 +
Wave 2 + Wave 3, ~100-150 sessions estimated):
[`mister_filesystem_implementation_plan.md`](mister_filesystem_implementation_plan.md).
Companion status survey:
[`full_MiSTer_support_status.md`](full_MiSTer_support_status.md).

Treat as a single tracked initiative here; progress is on that doc's
own per-format spine.

Gaps to reconcile back into the plan when it gets picked up:
- **BBCMicro / AcornElectron** — Acorn DFS (the flat catalog FS) not in
  the plan; only ADFS is covered via Archie.
- **AcornAtom** (Atom DOS) — niche, omitted from §1 + §5 of the plan.
- **SAM-Coupe** (SAM DOS / MasterDOS) — omitted.
- **ZX-Spectrum native FS** (TR-DOS / G+DOS) — only FAT + +3DOS are
  covered. The native floppy FS isn't in scope yet.
- **PET2001 D80/D82** — included in CBM Wave 3 but the `cbm` crate
  doesn't cover those formats; needs an extra slice.
- **§0 vs §4 reconciliation** — §0 says "Add/Delete is the target for
  every format" but §4 marks ~9 floppy cores as "write deferred". Decide
  which is right (only ColecoAdam EOS has a legitimate read-only reason
  — the core itself is RO).

---

## 6. CLI / GUI

### 6.1 `rb-cli fsck --format json|yaml`

Last unstructured query verb. Every other read-only verb (`inspect`,
`show *`, `locate`) accepts `--format json|yaml` and emits the standard
`schema_version`/`status`/`result` envelope. `fsck` writes
human-readable lines via `out_stdout` / `log_stderr` today
(`src/cli/verbs/fsck.rs:133`); serialize the existing `FsckResult`
(which already uses the shared `FsckIssue` / `FsckStats` types from
`src/fs/fsck.rs`) into the same envelope. Scripts currently work around
it by redirecting stdout and branching on `$?`.

### 6.2 `rb-cli get` globbing

Globs already work for `ls` / `rm` / `put` (bash-equivalent: `*` `?`
`[abc]` `**` `{a,b}`, `--include` / `--exclude` with exclude-wins).
`get` was deferred to the recursive-extract design.

Shape: when the source argument expands to multiple files (shell glob
or `--glob` flag), build an implicit batch in memory, run preflight,
apply as one operation — same code path as `rb-cli batch`. The
trailing-slash rule and conflict-handling flags (`--force` /
`--skip-existing`) carry over unchanged.

### 6.3 Native Mac archives — remaining GUI / parity items

Reader / writer for BinHex 4.0 (.hqx), StuffIt classic (SIT! and SIT5),
and .sea are in tree (Phase 1-2-3 of the original plan plus SEA
detection via `find_sea_archive`). CLI verbs `put-binhex`, `get-binhex`,
`sit list`, `sit extract`, `sit create`, `sit create OUT.sit.hqx`
shipped. Archives tab in the GUI ships.

Still open:
- **GUI `.hqx` import**: currently export-only. Need an "Import .hqx"
  flow in edit mode that decodes → `create_file` on the open
  filesystem.
- **Auto-unwrap hook**: when a decoded `.hqx` payload sniffs as a
  DiskCopy 4.2 (via `dc42::detect_dc42`) or raw HFS, route it into the
  image pipeline instead of the loose-file path.
- **SIT writer folder emission**: currently flat-file only. Add nested-
  directory support so a tree of files round-trips through `sit create`.
- **File-picker extensions** for `.hqx` / `.sit` / `.sea` — add to the
  inspect-tab / restore-tab / backup-tab pickers (single source of
  truth in `src/model/file_types.rs`).
- **GUI archive browse**: open `.hqx` / `.sit` / `.sea` as a read-only
  archive view, with the auto-unwrap hook routing disk-image payloads
  to the image pipeline.
- **ASCII-only audit**: confirm no Unicode glyphs leaked into the
  native-mac-archives surface (CLAUDE.md rule).

---

## 7. User-side verification (not coding)

- **HFV in BasiliskII / MAME** — boot/mount our blank + cloned HFVs
  against the bootable samples (`Mac OS 8.1.HFV`, `Starterdisk.hfv`).
  Our blank/cloned volumes fsck clean and round-trip byte-identically,
  but in-emulator mount is unverified.
- **HFV restore filename default** — optional nicety: default the
  restore output filename to `.hfv` when the backup was a `None`-table
  HFS. Round-trip works without it.
- **CD CHD ISO9660 browse** — open a CD CHD (game ISO ripped to CHD) in
  Browse, navigate the ISO9660 tree, extract a file, SHA256-match
  against the same file extracted via mounting the source ISO.
  Implementation is in tree (`CdCookedReader` in `src/rbformats/chd.rs`,
  `ImageFormat::ChdCdCooked` routing in `src/rbformats/mod.rs`); just a
  user-side sanity check.

---

## 8. Parked (could revisit; *not* deferred)

Items that have a real shape but no schedule. Surface them here so they
aren't lost.

- (None right now.)

---

## 9. Explicitly excluded

Out, not parked. Listed so the question doesn't get re-litigated.

- **StuffIt X / `.sitx`** — licensing.
- **SIT methods 6 / 8 / 14** — XADMaster itself flags as "interesting /
  partial".
- **SIT method 13 compressor (writer)** — stored / RLE90 covers
  preservation; no need for the compressor side.
- **Resume support for partial backups/restores** — explicitly deferred.
- **Partition-scoped device restore** (`/dev/disk3s2` style) —
  explicitly deferred.
- **Disk-expansion v1 out-of-scope**: shrinking via partition-table
  edits, shifting non-trailing partitions, RDB expand, per-partition
  free space.
- **In-place XFS / btrfs shrink** — `xfs_repair`-scale risk; the
  clone-into-fresh paths replace them.
- **23 N/A MiSTer cores** — tape / cartridge / ROM-only; no filesystem
  to support. See `full_MiSTer_support_status.md`.
- **`a2kit` / `cbm` / `fluxfox` as dependencies** — see the MiSTer plan
  §2 for the per-crate rationale; all are reference ports, not crates.
- **In-place btrfs balance / shrink** — replaced by §2.4
  scratch-recreate (gated on B.0 decision).
- **External `chdman`** — replaced by the in-tree MAME CHD core via
  `libchdman-rs`. The Windows-host elevation rework removed the
  separate `chdman.exe` dependency.

---

## 10. Recently closed (2026-06-02 sweep)

Audit trail. Each was either shipped, closed-by-design, or moved into
the structure above before its source plan doc was deleted in the
docs-consolidation pass.

- **Windows install + self-update** — Phases 1-9 + elevation A-H all
  done (Setup.exe via Inno, in-app `self_update` + `self_replace`,
  ARP `DisplayVersion` refresh, per-user file-association registrar,
  asInvoker elevation rework, Windows-VM runtime validated).
- **BasiliskII HFV support** — Phases 1-8 done; 4.1/4.2 standalone
  "New blank HFV" dialog closed-by-design (folded into Expand /
  Export-to-HFV).
- **HFS+ editable surface** — `impl EditableFilesystem for
  HfsPlusFilesystem` at `src/fs/hfsplus.rs:3703` with full
  create/delete/sync_metadata + Finder info + resource fork + blessed
  folder + xattr B-tree + hardlinks + HFSX case-sensitivity + journal
  parse + replay + history view + defrag-on-backup.
- **Superfloppy → partitioned-disk export** — `src/restore/
  superfloppy_wrap.rs` ships MBR + GPT synthesis with alignment
  variants and VBR `hidden_sectors` patching.
- **Clonezilla GPT parsing** — `<disk>-gpt-1st` + `<disk>-gpt-2nd`
  raw-sector readers wired through `src/restore/mod.rs:2048-2080` via
  the same CRC-validated `Gpt::parse` path native GPT restores use.
- **Earlier btrfs "trim at last chunk end" plan** — abandoned (real-
  world layouts don't have a useful last-chunk-end; superblock mirrors
  matter; restore-side resize would corrupt). Replaced by §2.4.
- **XFS gap-fill** — v5 read shipped, fsck shipped (R1-R8 pipeline +
  multi-level inobt rebuild + freespace btree rebuild), edit shipped
  (v4 + snapshot/rollback + wired into `open_editable_filesystem`),
  AGF walk shipped via `freespace_rebuild.rs`. Open: holes A-E in §2.1
  and shrink in §2.2.
- **JFS + UFS** — promoted from parked (`additional_nix_filesystems.md`
  "JFS / UFS parked, noted for completeness") to scheduled in §1.
- **`PLAN-ext-btrfs.md` Task 16** — `tests/filesystem_e2e.rs` ships 89
  ext2/ext4/btrfs browse + read + symlink + permissions + nested-dir +
  compaction round-trip + resize + detection-routing tests, with
  committed fixtures.
- **Disk expansion** — Phases 0-5 all shipped (PartitionBar widget, XFS
  recognizer, Mode A free-space, partition-table writers for
  MBR/GPT/APM/SGI, Mode B last-partition extend, Add-Partition flow).
- **Amiga Phase 10** — `resize_affs_in_place` / `resize_sfs_in_place` /
  `resize_pfs3_in_place` + their validators + RDB PART-entry fixup all
  shipped. Standard "restore an Amiga RDB image to a larger CF card
  and grow to fill" works end-to-end.
- **HFS+ defrag-on-backup Steps 22g + 23** — `shrink_to_minimum` wired
  via the "Resize partitions to minimum size" GUI checkbox and the
  per-partition Defrag toggle; `"Defrag emit: ..."` log line in
  `src/backup/mod.rs:1109-1121`.
- **HFS+ journal Steps 24-26 + 28-29** — parse + replay + transaction
  recorder + history viewer + fsck journal phase. §2.3 covers what's
  left.
- **SEA detection** — `find_sea_archive` + extract.rs routing classifies
  `.sea` as SIT-over-Mac-app data fork.
- **Native Mac archives reader/writer** — BinHex 4.0 + StuffIt classic
  + SIT5 + SEA, CLI verbs, Archives tab in GUI, all shipped. §6.3
  covers the GUI import / auto-unwrap / folder-emit / file-picker
  remainder.
- **`docs/codecleanup.md`** — every actionable item closed; survey
  retired.
- **`docs/virtualization-formats.md`** — Phases 1-5 closed; modern-VM
  containers (VHD dynamic / QCOW2 / VMDK flat + sparse) and legacy
  imports (GHO + IMZ with password decryption + file-aware FAT+NTFS
  reconstruction) all shipped.
- **`docs/efs_resize_and_edit.md`** — EFS read + edit + fsck + in-place
  grow + conservative shrink + aggressive shrink all shipped.
- **`docs/chdman_replacement.md`** — Stages 1-10 shipped; no external
  `chdman` binary required. The user-side CD CHD browse verify in §7
  is the only loose end.
