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

**Closed 2026-06-02.** ReiserFS (v3.5 + v3.6), UFS (UFS1 + UFS2), and JFS
(JFS2) all read end-to-end through the trait: inspect, layout-preserving
backup compactor, browse, file read, symlinks, recursive directory
descent. See §10 for the per-FS one-liners. Optional Tier B+ follow-ups
(UFS U.4 fsck + edit; JFS J.4 fsck + edit, plus the J.3 multi-page B+tree
walkers for >32 GiB / >4k-inode / non-inline-dtree JFS volumes) are
parked in §8 behind real demand. ReiserFS is intentionally read-only
forever (filesystem leaving the kernel; investment caps at "read what
exists").

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

- **(A) new-chunk inode allocation** — **shipped 2026-06-02** (single-leaf
  + multi-level grow; see §10).

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

### 3.1 HFS — extending a raw partition image

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

(Section currently empty — Mac archives GUI polish closed 2026-06-02,
see §10. Reopen when new CLI / GUI work surfaces.)


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

- **UFS Tier B+ (U.4) — fsck + edit.** Read shipped (see §10); on-disk
  ground knowledge is already in tree via `src/fs/ufs.rs` + EFS precedent
  (`src/fs/efs.rs` shares the cylinder-group + classic-Unix-inode shape).
  Real-world demand has been zero so far; revisit if someone needs to
  edit / repair a UFS image.
- **JFS Tier B+ (J.4) — fsck + edit.** Read shipped (see §10), but every
  edit path runs through xtree / dtree / dmapctl B+tree writes that
  J.1–J.3 deliberately walked only at the inline-root level. Picking up
  edit means the multi-page B+tree walkers + reverse-direction writers
  for all three trees.
- **JFS multi-page B+tree walkers (read-side).** `walk_bmap` refuses
  aggregates ≥ 2²³ blocks (32 GiB at bsize=4096) — a multi-level `dmapctl`
  walker is the unblocker. `read_fileset_iag` refuses multi-IAG filesets
  (> 4096 inodes). `parse_inline_dtree` refuses non-inline dtrees
  (`di_size > 4096`); `read_file_data` refuses xtrees with internal
  nodes. All four are out of scope for vintage hardware (the actual
  motivating workload) but would block any larger-than-vintage JFS
  read.

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

## 10. Recently closed (2026-06-02 sweep + follow-ups)

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
  + SIT5 + SEA, CLI verbs, Archives tab in GUI, all shipped. §6.1
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
- **Unix-FS read track (§1 ReiserFS / UFS / JFS)** — three filesystems
  shipped end-to-end. **ReiserFS** (v3.5 `ReIsErFs` + v3.6 `ReIsEr2Fs`,
  reiser4 rejected): superblock at byte 65536 + bitmap-driven
  layout-preserving `CompactReiserFsReader` (set bit = allocated) + S+tree
  walker (`collect_leaf_block_numbers` DFS with cycle / level / child=0
  guards) + `read_statdata` / `read_symlink_target` / `list_directory`
  (DIR_ENTRY decode, hidden-filter) / `read_file` (SD/IND/DRCT
  stitching with sparse + tail-pad handling). Fixture
  `tests/fixtures/test_reiserfs_v3_6.img.zst`. **UFS** (UFS1 4.2BSD/FFS +
  UFS2 — SunOS / Solaris / *BSD): superblock probe at 65536 then 8192
  with both endians, SU+J dirty refused, cylinder-group bitmap walker
  (set = FREE — opposite polarity of every other Linux FS, matching
  primitive `BitmapReader::highest_clear_bit`), `CompactUfsReader`,
  `UfsInode` (UFS1 128 B / UFS2 256 B), `resolve_logical_block` (direct
  → single → double → triple indirect with sparse handling), DIRENT2
  decode with `d_reclen` alignment validation, inline-symlink decode at
  the version-specific cutoffs. **JFS** (JFS2 only — AIX JFS1 rejected
  via the `"JFS1"` ASCII magic gate paired with version `1..=2`):
  Aggregate Superblock at byte 32768, AIT bootstrap at
  `AIMAP_OFF + 2 * SIZE_OF_MAP_PAGE = 0xB000`, BMAP B+tree walker that
  tightens `last_data_byte` to the pmap-derived highest allocated block
  (still maxed against `logpxd`/`fsckpxd` so the inline log rides out
  verbatim), `CompactJfsReader`, IAG-based dinode locator with pmap
  sanity check, inline-dtree walker (stbl iteration through slots 1..8 +
  dtslot continuation chain for names > 11 chars), inline-xtree XAD walker
  with sparse gap-fill. Parked Tier B+ follow-ups (UFS U.4 / JFS J.4
  fsck+edit, JFS J.3 multi-page B+tree walkers) live in §8.
- **ProDOS access-bit setter** —
  `EditableFilesystem::set_prodos_access(entry, access)` patches the
  access byte at directory-entry offset 30 in place. **Lock** ($21 = read
  + backup) / **Unlock** ($C3 = read + write + destroy + rename +
  backup) buttons on the browse-view edit toolbar; `StagedEdit::
  SetProdosAccess { entry, access }` queues through the same
  replace-prior + pending-query helpers as the rest of the staged-edit
  pipeline.
- **NTFS file-aware GHO — compressed path** — `NtfsCompressedState`
  in `src/rbformats/gho.rs` carries per-block zlib metadata + a lazy
  scan cursor; `open_ntfs_compressed_mft_only` decompresses just the
  $MFT run to bootstrap the index, then `extend_ntfs_compressed_index`
  decompresses the rest on demand as `GhoReader` reads progress. The
  scan result rides through `ntfs_scan_cache_store` so re-opening the
  same `.GHO` skips the multi-second decompress pass. Compressed +
  uncompressed file-aware NTFS GHO backups now round-trip identically.
- **`rb-cli fsck --format json|yaml`** — `FsckResult` / `FsckIssue`
  / `FsckStats` / `OrphanedEntry` / `RepairReport` all carry
  `#[derive(Serialize)]`. The verb mirrors the structured-output flag
  every other read-only verb already has, with a `clean: bool` top-level
  envelope for scripted branching and a `repair: { applied / failed /
  unrepairable }` sub-object when `--repair` runs. Unsupported
  filesystems emit `status.error: true, status.code: GENERIC_FAILURE,
  result: null`. Process exit code stays issue-driven so `$?` branching
  works in either text or structured mode.
- **`rb-cli get` globbing** — accepts the same glob /
  `--exclude` / `--ignore-case` / `--case-sensitive` syntax as `ls` /
  `rm`. Adds `-r` / `--recursive` (literal-directory source without it
  errors with a clear pointer to the flag; glob-matched directories
  without it skip with a warning), `--force` (overwrite existing host
  files), `--skip-existing` (skip silently). Destination semantics
  follow cp/rsync: literal-file source treats DST as the target path
  (or `DST/<basename>` when DST exists as a directory or has a trailing
  separator); literal-directory + `-r` mirrors the tree under
  `DST/<source-basename>/`; glob lays matches out relative to the
  longest non-glob prefix of the pattern. Symlinks ride out as plain
  text files containing the target (lossy but cross-platform safe);
  specials skip with a one-line note.
- **XFS hole (A) — new-chunk inode allocation (single-leaf + multi-level
  grow)** — `alloc_inode_slot` lifted into
  `try_claim_slot_from_existing_chunks`; the new `alloc_new_inode_chunk`
  reads `di_version` from the root inode, carves `blocks_per_chunk`
  contiguous fsblocks aligned to `sb_inoalignmt` (or `blocks_per_chunk`
  when ALIGN is off) via the new `alloc_blocks_aligned` +
  `carve_aligned_from_largest` primitives, writes 64 free dinodes (magic
  + version + `di_mode = 0` + `di_next_unlinked = NULLAGINO`), then
  dispatches between two splice strategies. **Single-leaf splice**
  (`splice_inobt_single_leaf_record`) — when the AGI root is a one-block
  level-1 leaf with at least one open slot, sort + rewrite the leaf in
  place; AGI root/level unchanged. **Multi-level grow**
  (`grow_inobt_with_new_record`) — when the existing tree is multi-level
  or its only leaf is full, gather every existing record from every
  leaf, add the new all-free chunk, key-sort, allocate `blocks_needed_for`
  contiguous AG-local blocks for a fresh tree AND reclaim the old tree
  blocks in one freespace rebuild via the new `swap_inobt_blocks_in_ag`
  helper (alloc + free fused; the old blocks coalesce into the AG's
  current-full-free set, then `n` blocks carve from the largest extent,
  and `rebuild_ag_freespace` writes one set of new bno/cnt trees),
  re-build the inobt via R3's `build_sblock_btree`, write every new tree
  block, then relocate AGI root + level. AGI `count`/`freecount` + sb
  `sb_icount`/`sb_ifree` bump by 64 once after dispatch — both paths
  share the same counter update. Slot search re-runs and claims slot 0
  of the new chunk via the existing -1 accounting. v4 only (v5/CRC is
  hole E). New helpers: `collect_inobt_all_blocks` in `inobt_repair.rs`
  (shared `walk_inobt` traversal returns leaves + all blocks).
  Tests: 5 alignment-carve unit tests +
  `alloc_new_inode_chunk_extends_inobt_and_keeps_volume_clean` end-to-end
  against the v4 fixture (counter deltas, sorted record + all-free
  shape, every dinode initialized, our verifier stays clean) +
  `alloc_new_inode_chunk_promotes_full_leaf_to_multi_level` (forges 254
  phantom records to fill the root leaf, then verifies the new tree is
  level >= 2, AGI root moved, all max_leaf+1 records survive in
  per-leaf-sorted form, sb counters bumped). Oracle helper:
  `examples/xfs_fill_chunks.rs`.
- **Native Mac archives GUI polish (Workflows A / B / C / D / E)** —
  full sweep. Detection is now magic-driven via
  `macarchive::detect::detect_mac_archive` (six `MacArchiveKind`
  variants — BinHexSingleFile, BinHexOverSit, BinHexOverSea, Sit, Sit5,
  Sea); filename extension is hint-only. `MacArchiveKind::label()` keeps
  user-visible strings ASCII-only (CLAUDE.md egui-font rule).
  `detect_mountable_image` sniffs DiskCopy 4.2 / raw HFS / raw HFS+ for
  the auto-unwrap "Mount in new Inspect tab" affordance.
  **D.4**: inspect / restore / backup tab pickers gained a "Mac
  archives" filter group alongside Disk Images. **D.1**:
  `extract::open_bytes` routes through `detect_mac_archive` and
  synthesizes a one-entry archive (with proper `crc16_arc` stamping) for
  loose BinHex single-file inputs, so the Archives tab and every
  downstream extract path handle `.hqx` end-to-end without any special
  case. **D.2**: Archives tab caches a `MountablePayload` on load when
  the archive has exactly one entry whose data fork sniffs as a disk
  image; "Mount in new Inspect tab (<kind>)" button writes the bytes to
  a tempfile, hands path + tempdir guard to the Inspect tab via the new
  `InspectTab::load_image_with_tempdir`, and the RustyBackupApp update
  loop drains `take_pending_inspect_open` + switches `active_tab`.
  **D.3**: per-entry checkbox column on the Archives tab grid; new
  `extract::extract_filtered(keep: impl FnMut(usize) -> bool)`
  (folder markers always extract); "Extract Selected... (N)" button
  alongside Extract All. **A**: browse-view `stage_host_file` intercepts
  any picked file that sniffs as a Mac archive when the target is HFS /
  HFS+; queues a `PendingArchiveImport`; modal pops with the per-kind
  three-action set ("Convert" / "Expand" / "Convert and expand" +
  "Convert only" for HQX-over / "Add as-is" + Cancel). Extract path
  dumps to an AppleDouble tree under a BrowseView-owned
  `archive_import_tempdir`, then re-stages the resulting children
  through the existing `stage_host_directory` / `stage_host_file` flow
  so resource forks come back through `detect_resource_fork`. Tempdir
  cleaned up on Apply / Discard / close. **B**: three new "Export Mac
  archive" buttons under the existing Extract row — BinHex (single file
  only), StuffIt (any selection — folders walk recursively), and
  StuffIt-over-BinHex (any selection). `walk_fs_to_input_nodes` reads
  data + resource fork + type/creator from the FileEntry / Filesystem
  trait; `build_archive_tree` + optional `binhex::build_binhex` wrap
  produces the output bytes. **C**: a "Save as Mac archive..." button on
  selected files inside the disk image sniffs the bytes and pops a
  Save-as-is / Decode-and-save modal with an inline ForkFormat dropdown
  (BinHex / MacBinary / AppleDouble / Raw) for the decode path. **E**:
  a "Browse archive..." button opens a floating read-only viewer window
  with the same shape as the standalone Archives tab — entry list with
  per-entry checkboxes, fork-format dropdown, Extract All / Extract
  Selected. Edit operations inside the archive (would need repacking)
  are deferred. Modal wording lives in
  `docs/mac-archives-gui-wording.md` (kept as the spec source of truth).
  Tests: 17 detect-tests (6 kinds + 3 mountable + edge cases) + 4
  extract-tests (open_bytes synth, extract_filtered subset).
