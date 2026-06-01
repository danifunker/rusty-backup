# XFS — Consistency Checker (fsck) + Conservative Repair

Add a read-only integrity checker for SGI / IRIX XFS volumes, surfaced
through `Filesystem::fsck`, followed by a deliberately small repair path
limited to the safe, non-structural fixes. This is the XFS counterpart to
the EFS work in `docs/efs_resize_and_edit.md` (Phase 2 there) — but XFS is a
B+tree filesystem, so the verifier is broader and the repair story is
narrower.

The read side already exists: `src/fs/xfs/` parses the superblock, inodes,
dir1/dir2 directories, the bmap btree, and symlinks (v4 only — v5 returns a
clear "not supported" error at open time). This document covers the
check/repair side only; it does **not** add editing, grow, or shrink (XFS
resize is clone-via-rebuild and is tracked separately in
`docs/SGI_Filesystems.md`).

---

## Why a checker first, and why repair is narrow

EFS could get a full in-place repair because it has no btrees, no checksums,
and a flat per-CG bitmap. XFS has none of that simplicity:

- **Free space and inode allocation are B+trees**, not bitmaps. The AGF
  carries two free-space btrees (by-block-number `bno`, by-count `cnt`); the
  AGI carries the inode-allocation btree. Rewriting these to "fix" an
  allocation is a structural edit with wide blast radius.
- **Inode numbers are packed**: `ino = (agno << (agblklog + inopblog)) |
  (agblock << inopblog) | offset`. A misplaced inode can't simply be moved.
- **AG-relative pointers** mean every free/inode btree node is interpreted
  relative to its allocation group — corruption in one AG header invalidates
  a whole AG's interpretation.
- v4 has **no CRCs** (one thing in our favour — no checksum re-stamping when
  we do write), but it also means less self-describing metadata to validate
  against.

So the strategy mirrors EFS's split:

1. **Verifier (Phases 1–3)** — read-only, ships first, answers the user's
   actual question ("is my IRIX disk corrupt, and where?"). Low risk, high
   value. This is the bulk of the work.
2. **Conservative repair (Phase 4)** — only the fixes that are provably safe
   without touching the allocation btrees: adopt unreachable-but-valid inodes
   into `lost+found/`, and reconcile secondary-superblock / AG-header
   summary fields that disagree with the primary. Everything structural is
   surfaced `repairable = false` for diagnosis, same as EFS does for geometry
   damage.

---

## Background — on-disk layout (cribbed from `src/fs/xfs/` + xfs-modern refs)

```
AG 0                                   AG 1 ... AG (agcount-1)
+------------------+
| sector 0  super  |  primary superblock (XFSB) — AG 0 only carries the
|                  |  authoritative copy; every AG starts with a sb replica
| sector 1  AGF    |  free-space btree roots (bno_root, cnt_root), freeblks
| sector 2  AGI    |  inode btree root, count, freecount
| sector 3  AGFL   |  free list (not needed by the checker)
| ...  inode btree |
| ...  free btrees |
| ...  inodes      |  di_format: 1=local 2=extents 3=bmap-btree
| ...  data/dirs   |
+------------------+
```

Geometry from the superblock (`src/fs/xfs/sb.rs`, all big-endian):
`blocksize`, `dblocks` (total fs blocks), `agblocks`, `agcount`, `rootino`,
`inodesize`, `inopblock`, `agblklog`. The AG headers are already parsed by
`src/fs/xfs/ag.rs` (`XfsAgf`, `XfsAgi`) — they were written "for completeness"
during the read work specifically so the checker can plug straight in.

Inode walking entry points already present in `mod.rs`:
`read_inode(ino)` / `read_inode_buf(ino)`, `walk_bmbt(...)` (bmap btree
walker), `walk_dir2_data_blocks(...)`, and `read_file(...)`.

References (same set the reader cites):
- `~/xfs-efs/refs/xfs-modern/xfs/libxfs/xfs_format.h`
- `~/xfs-efs/refs/xfs-modern/xfs/libxfs/xfs_alloc_btree.{c,h}` (AGF free btrees)
- `~/xfs-efs/refs/xfs-modern/xfs/libxfs/xfs_ialloc_btree.{c,h}` (AGI inode btree)
- "XFS Algorithms & Data Structures", 3rd ed. (SGI).
- `docs/SGI_Filesystems.md`, `docs/fsck.md` (shared fsck types).

---

## Shared types (no new infrastructure)

Reuse `src/fs/fsck.rs` exactly as EFS and HFS do: `FsckResult`,
`FsckIssue { code: String, repairable: bool }`, `FsckStats { extra: Vec<..> }`,
`OrphanedEntry { id: u64 }`, `RepairReport`. The GUI already renders these
generically — no XFS-specific display code is needed. Put the checker in a new
`src/fs/xfs/fsck.rs` (sibling to the existing modules) and the private code
enum + `is_repairable_code()` mapping there, exactly the shape of
`src/fs/efs_fsck.rs`.

---

## Phase 1 — Superblock + AG header verifier

Read-only. The cheap, high-signal checks that catch the most common "won't
mount" corruption.

1.1 **Primary superblock sanity** — magic `XFSB`; `blocksize` a power of two
    in [512, 65536]; `agcount >= 1`; `agblocks * agcount >= dblocks` and
    `(agcount-1) * agblocks < dblocks` (last AG may be short); `rootino`
    inside AG 0; `inopblock == blocksize / inodesize`. Codes: `BadMagic`,
    `BadBlocksize`, `BadAgGeometry`, `RootInoOutOfRange` (all
    `repairable = false`).

1.2 **Per-AG superblock replica** — every AG starts with a sb copy. Compare
    the resize-relevant / geometry fields against the primary (`dblocks`,
    `agblocks`, `agcount`, `blocksize`, `inodesize`, `rootino`, feature
    bits). Mismatch → `ReplicaSbMismatch` (**repairable** — see Phase 4).

1.3 **AGF / AGI parse + summary sanity** — for each AG, parse `XfsAgf` and
    `XfsAgi` (already implemented). Check `seqno == agno`, `length <=
    agblocks`, btree root pointers (`bno_root`, `cnt_root`, AGI `root`) land
    inside the AG, `freeblks <= length`, `freecount <= count`. Header magic
    or root-out-of-AG → `BadAgHeader` (`repairable = false`); a `freeblks` /
    `freecount` summary that disagrees with the btree walk (Phase 2) →
    `AgSummaryMismatch` (**repairable**).

Output: `FsckStats.extra` lines for AG count, total/free blocks, inode count.

---

## Phase 2 — Allocation & inode btree walk

Read-only. Walk the on-disk btrees and cross-check.

2.1 **Free-space btrees** — for each AG, walk the `bno` btree from
    `agf.bno_root`. Validate node magics (`XFS_ABTB_MAGIC` / `XFS_ABTC_MAGIC`
    for v4), level monotonicity, key ordering, and that every free extent
    `[startblock, startblock+blockcount)` lies inside the AG. Sum the free
    extents and compare with `agf.freeblks` → `AgSummaryMismatch` if off.
    Record every free extent in a per-AG free set (a `Vec<(u32,u32)>` runs or
    a bitset) for the double-allocation cross-check. Codes: `FreeBtreeMagic`,
    `FreeExtentOutOfAg`, `FreeBtreeUnordered`.

2.2 **Inode allocation btree** — walk the AGI `inobt` from `agi.root`
    (`XFS_IBT_MAGIC`). Each record covers a chunk of 64 inodes with a
    free-mask bitmap; collect the set of *allocated* inode numbers. Compare
    the allocated count with `agi.count - agi.freecount`. Codes:
    `InodeBtreeMagic`, `InodeChunkOutOfAg`, `AgInodeCountMismatch`.

2.3 **Inode table sweep** — for every allocated inode from 2.2, call
    `read_inode_buf`. Validate `di_magic == IN`, `di_format` is a value the
    reader understands per fork, `di_nextents` consistent with the fork, and
    decode the data fork's extents (via the existing extent / `walk_bmbt`
    code). Every block in every extent must lie inside the fs and must **not**
    fall in any AG's free set from 2.1. Codes: `InodeMagic`,
    `InodeReadFailed`, `ExtentPastVolume`, `DoubleAllocation`,
    `ExtentInFreeSpace` (all `repairable = false`).

This phase is where most real corruption surfaces. Keep it streaming — never
materialize a full-volume bitmap larger than necessary; a per-AG `Vec<bool>`
of `agblocks` bits is the natural granularity.

---

## Phase 3 — Connectivity (orphan detection)

Read-only. BFS from `rootino`, following directory entries via the existing
dir1/dir2 walkers, collecting every reachable inode number. Any allocated
inode (from Phase 2.2) **not** in the reachable set with `di_mode != 0` is an
orphan: emit `OrphanInode` (**repairable**) and push an
`OrphanedEntry { id: ino }` so Phase 4 can adopt it. Also flag directory
entries whose target inode is unallocated or unreadable → `DanglingEntry`
(`repairable = false`; needs a structural dir edit we're not doing yet).

Mirror EFS's `parse_dir_block` BFS exactly — same shape, different dir
format.

---

## Phase 4 — Conservative repair

Implement `EditableFilesystem::repair()` for XFS, restricted to fixes that do
**not** touch the allocation or inode btrees:

4.1 **`ReplicaSbMismatch`** — overwrite the offending AG's sb replica fields
    from the authoritative primary. Pure field copy, no structural change.

4.2 **`AgSummaryMismatch`** — rewrite `agf.freeblks` / `agi.freecount` /
    `agi.count` from the values the Phase-2 walk computed. The btrees
    themselves are already self-consistent; only the cached summary is wrong.

4.3 **`OrphanInode`** — adopt into `lost+found/` under root, the same pattern
    as `repair_efs`: ensure a `lost+found` directory exists, add a
    `(ino, "inode_<n>")` entry. This is a directory-data edit (shortform or
    a data block), **not** a btree edit — keep it to inodes whose own extents
    are already valid (no overlap with Phase 2 structural codes).

Everything else (`BadAgHeader`, `DoubleAllocation`, `ExtentPastVolume`,
`FreeBtree*`, `InodeBtree*`, `DanglingEntry`, any geometry damage) is emitted
`repairable = false`. The GUI greys out "Repair" when no finding is
actionable — already handled by the shared types.

> **Snapshot/rollback:** wrap every repair mutation in a snapshot guard like
> HFS Step 2 / EFS does, so a mid-repair failure rolls back cleanly. Re-run
> the Phase 1–3 verifier *after* repair and refuse to seal if it still errors.

---

## Non-goals (this document)

- **v5 / dir3 / CRC-stamped XFS.** Reader is v4-only; the checker inherits
  that. v5 support is a separate, larger effort (CRC validation everywhere).
- **Rebuilding allocation or inode btrees.** Structural damage is diagnosed,
  not repaired. That is `xfs_repair`-class work and out of scope.
- **Editing / grow / shrink.** Tracked in `docs/SGI_Filesystems.md`. The
  `lost+found` adoption in 4.3 is the only directory write we add.
- **Realtime device, quota inodes, reflink.** Not present on vintage IRIX
  disks; skip.

---

## Wiring & tests

- New module `src/fs/xfs/fsck.rs`; declare `pub mod fsck;` in
  `src/fs/xfs/mod.rs`.
- Implement `fn fsck()` on the existing `impl Filesystem for XfsFilesystem`
  (currently inherits the `None` default at `src/fs/filesystem.rs:112`).
- Add `impl EditableFilesystem for XfsFilesystem` (Phase 4 only) — requires
  the generic to widen to `R: Read + Write + Seek + Send`, matching EFS.
- Route through the existing dispatch in `src/fs/mod.rs` — XFS already opens
  via `xfs::XfsFilesystem::open` for MBR `0x83` (with XFSB magic), synthetic
  `0xA0` (SGI), and the GPT Linux-filesystem GUID. No new routing needed; the
  `fsck()`/`repair()` calls flow through the same boxed trait object.
- CLI/GUI parity: `is_checkable_type()` / Check button already dispatch on
  the trait, so XFS lights up automatically once `fsck()` returns `Some`.
- **Fixtures:** build small v4 XFS images with `mkfs.xfs -m crc=0` (forces
  v4) under a Linux/CI helper, then craft corrupted variants by poking bytes
  (bad replica sb field, wrong AGF `freeblks`, an unlinked-but-allocated
  inode) — mirror the EFS fsck tests in `src/fs/efs.rs`
  (`repair_fixes_missing_bitmap_bit`, `repair_adopts_orphans_into_lost_found`).
  Keep an `examples/xfs_poke.rs` for crafting dirty fixtures, like the HFS+
  journal poke tools.

---

## Implementation order

1. **Phase 1** — superblock + AG header verifier (2 days). Independent,
   immediately useful.
2. **Phase 2** — btree walk + double-alloc cross-check (3–4 days). The core.
3. **Phase 3** — connectivity / orphans (1–2 days; reuses dir walkers).
4. **Phase 4** — conservative repair (2–3 days; reuses EFS snapshot pattern).

Phases 1–3 ship together as the verifier PR (this alone covers the user's
"is it corrupt?" need). Phase 4 is a separate PR. v5 support, if ever, is its
own track.
