# XFS — Editable Filesystem + Full fsck Repair (v4 / IRIX)

> **Implementation status (2026-06-01).** Verifier (Phases 1–3 + block-ownership
> map) and the repair phases **R4** (secondary-superblock geometry), **R4b**
> (AGF/AGI summary counters), **R2** (free-space btree rebuild,
> `src/fs/xfs/freespace_rebuild.rs` + `btree_build.rs`), and **R3** (inobt
> free-mask/freecount repair + single-level structure rebuild,
> `src/fs/xfs/inobt_repair.rs`) are **shipped and oracle-validated** — including
> against a real V1-inode IRIX disk (`xfsprogs` 3.1.9 oracle). `repair()` runs
> them in order R4 → R4b → R3 → R2. Not yet done: R3 multi-level structure
> rebuild, **R5** (inode core), **R6** (directory), **R7** (orphan reconnection
> — note the verifier already flags `OrphanInode` as repairable, which won't be
> true until R7 lands), and the §3/§4 edit write-primitives. See the auto-memory
> `xfs_fsck_repair.md` for the running status and the Docker oracle recipe.

Extend the existing read-only XFS support (`src/fs/xfs/`) all the way to:

1. **Full fsck repair** — not just the conservative field-fixes in
   `docs/xfs_fsck.md`, but `xfs_repair`-class reconstruction: rebuild the
   free-space and inode-allocation btrees from a full-scan block-ownership
   map, reconnect orphans, and rewrite damaged AG headers. **This is the user-
   facing goal ("my IRIX disks get corrupt") and ships first** — see §8 /
   Appendix A.4 for why repair precedes edit.
2. **Edit** — create/delete files and directories from the browse view
   (`EditableFilesystem`), the same surface AFFS/PFS3/SFS/EFS expose. Built
   on the §3 write primitives, which repair does **not** need.

This is a large, multi-week effort. The full reference set is now local under
`~/efs-xfs-refs` — most importantly **`xfsprogs` pinned to v3.1.11**, the last
v4-only release, whose
offline `xfs_repair` is almost exactly the program we are reimplementing.
This document maps those references to concrete phases and data structures
(Appendix A is code-ready), and calls out what the references **don't** give
us so the scope stays honest.

> **Relationship to the other XFS docs:** `docs/SGI_Filesystems.md` is the
> read-side plan (done). `docs/xfs_fsck.md` is the read-only **verifier** +
> *conservative* repair (replica-sb / AG-summary / orphan-adoption only).
> This document is the superset: it assumes the verifier from `xfs_fsck.md`
> exists, adds write primitives, then builds edit and full repair on top.

---

## 0. Reality check — is this actually feasible, and how big?

Yes, feasible — but it is the single largest filesystem feature in the
project, bigger than the EFS edit+resize work, because **XFS is a btree
filesystem and we currently have zero XFS write code.** Everything — the
allocator, inode allocation, directory mutation, btree insert/split — has to
be built from scratch against the on-disk format.

Two facts make it *more* tractable than rewriting modern `xfs_repair`:

- **Target is v4 only.** Every IRIX disk we care about is `XFS_SB_VERSION_4`
  (e.g. the on-hand `irix65.chd` is `versionnum=0x30b4` = v4 + ATTR + NLINK +
  ALIGN + EXTFLG + DIRV2). The reader already refuses v5 at open time. v4
  has **no CRCs, no rmap btree, no finobt, no reflink/refcount btree, no
  parent pointers, no realtime metadir.** That removes ~70% of what modern
  `xfs_repair`/`scrub` has to handle.
- **We control the whole pipeline.** Because rusty-backup operates on image
  files offline, we can use the **offline reconstruction model** (full scan →
  in-memory maps → rebuild), which is far simpler than the kernel's online,
  transactional, rmap-driven repair.

Two facts make it *harder* than it looks:

- **The modern `scrub/` repair engine does not apply to v4.** See §2.
- **No journal replay.** v4 disks have an external/internal log; a disk that
  was not cleanly unmounted may have committed metadata only in the log. We
  do **not** implement log replay (huge). Mitigation: detect a dirty log
  (`sb_logstart` region non-empty / log head != tail) and **refuse to
  edit**, exactly as HFS+ refuses a dirty journal. Repair on a dirty-log
  volume is allowed but warns that pre-crash in-flight changes are lost —
  which is also what `xfs_repair -L` (zap the log) does.

Rough effort: **6–9 weeks** of focused work, phased so each phase is
independently shippable and useful.

---

## 1. Reference map — what `~/efs-xfs-refs` gives us

```
~/efs-xfs-refs/
  xfsprogs/                   <- *** PRIMARY REFERENCE *** offline xfs_repair + libxfs + db + mkfs
                                 PINNED to tag v3.1.11 (2013-05-08) — the LAST release before
                                 v5/CRC existed. Verified: zero XFS_SB_VERSION_5 in libxfs, no
                                 mkfs crc= option, no rmap.c, no xfs_btree_staging.c. 100% v4.
  xfs-modern/xfs/libxfs/      <- modern on-disk format primitives (cross-check only; v5-laden)
  xfs-modern/xfs/scrub/       <- kernel online repair engine (rmap-based; does NOT apply — see §2)
  xfs-dir1-2.4.2-sgi/xfs/     <- IRIX-era v4 source: the ONLY readable dir1 reference
  xfuse/src/libxfuse/         <- independent Rust XFS *reader* (v5/dir3) — field-level cross-check
  efs-linux-5.15/efs/         <- EFS driver (already ported; reference only)
  xfs_filesystem_structure.pdf        <- "XFS Algorithms & Data Structures" 3rd ed (format bible)
  File System Forensic Analysis.pdf   <- Carrier; prose-level XFS chapter
  xfs-online-fsck-design.html         <- Darrick Wong design doc (v5/rmap; shape only)
```

> **Why v3.1.11 specifically.** XFS v5/CRC made its mainline debut in **Linux
> 3.12 + xfsprogs 3.2.0** (2013/2014). v3.1.11 (2013-05-08) is the **last
> release before v5 existed**, so *every line of its code manipulates exactly
> the on-disk format our reader handles* — no `xfs_sb_version_hascrc()`
> branches to mentally skip, no self-describing block headers, 1994-style
> inode cores throughout. It also predates the 2016+ machinery (rmap btree,
> online `scrub/`, `xfs_btree_staging.c`), so its offline `repair/phase5.c`
> rebuilds the AG btrees with a hand-rolled "incore slice" builder and **no
> rmap dependency** — exactly the shape we want. **This tree, not
> `xfs-modern`, is the porting target.** Use `xfs-modern/libxfs` only when a
> field layout is genuinely clearer there (its v5 branches are then the ones
> to ignore). The repair algorithm is identical to later releases — v5 just
> adds parallel branches — so nothing is lost by pinning here.

### 1a. `libxfs/` — the write-primitive bible (use heavily)

These are the portable routines shared by mkfs/repair/kernel. Port the
*algorithms*, not the code (it's GPL kernel C; we reimplement in Rust):

| Need | libxfs file(s) |
|------|----------------|
| Superblock geometry, version predicates, AG count math | `xfs_sb.c`, `xfs_format.h`, `xfs_types.c` |
| Generic B+tree insert/delete/split/merge/lookup | `xfs_btree.c`, `xfs_btree.h` |
| **Bulk btree load (rebuild from sorted records)** | `xfs_btree_staging.c/.h` — the staging-cursor "fake root" loader repair uses to rebuild a whole btree at once |
| Free-space (`bnobt`/`cntbt`) records + geometry | `xfs_alloc_btree.c`, `xfs_alloc.c` |
| Inode-allocation (`inobt`) records + chunk alloc | `xfs_ialloc_btree.c`, `xfs_ialloc.c` |
| Data-fork mapping (`bmbt`) insert/extend | `xfs_bmap_btree.c`, `xfs_bmap.c` |
| Inode core encode/decode, fork formats | `xfs_inode_buf.c`, `xfs_inode_fork.c` |
| dir2 mutation (sf/block/leaf/node add/remove) | `xfs_dir2_sf.c`, `xfs_dir2_block.c`, `xfs_dir2_leaf.c`, `xfs_dir2_node.c`, `xfs_dir2_data.c` |
| dir/attr da-btree (shared with dir2 node) | `xfs_da_btree.c` |
| bit helpers (inobt free masks, agfl) | `xfs_bit.c` |

> **Ignore in libxfs for v4:** `xfs_rmap*`, `xfs_refcount*`, `xfs_rt*`,
> `xfs_parent.c`, `xfs_metadir.c`, `xfs_metafile.c`, `xfs_exchmaps.c`,
> `xfs_attr_remote.c`, CRC paths (`xfs_cksum.h`). None exist on v4 IRIX disks.

### 1b. `xfs-dir1-2.4.2-sgi/` — the v4 ground truth (primary reference)

This is the **actual IRIX-era source** and the closest match to the disks the
user is repairing. Critically it still contains **dir1** (`xfs_dir.c`,
`xfs_dir_leaf.c`, `xfs_dir_sf.h`) which the modern kernel deleted — needed if
any target disk predates dir2. It also has the v4-shaped `xfs_alloc_btree.c` /
`xfs_ialloc_btree.c` / `xfs_btree.c` without any of the later rmap/CRC
plumbing, so it's *cleaner* to port for our purposes than the modern tree.
Use modern `libxfs/` when a routine is clearer there, but cross-check
on-disk field layout against this tree.

### 1c. `scrub/` — algorithm shape only, NOT a port target (see §2)

### 1d. Reference inventory (all now local under `~/efs-xfs-refs`)

- **`xfsprogs/repair/`** (offline `xfs_repair`, pinned at v3.1.11, v4-only) —
  **the primary
  blueprint.** Its `phase1.c`..`phase7.c` map one-to-one onto our R-phases
  (§5), and the in-memory data model in `incore.h` / `incore_ino.h` /
  `incore_ext.c` is exactly the ownership reconstruction we need (detailed in
  Appendix A). Read order: `xfs_repair.c` (driver) → `incore.h` +
  `incore_ino.h` (data model) → `phase2.c` → `dino_chunks.c` + `dinode.c` +
  `scan.c` (P3/P4) → `phase5.c` (btree rebuild) → `phase6.c` + `dir2.c` (dir
  connectivity) → `phase7.c` (nlinks).
- **`xfsprogs/libxfs/`** — the write primitives (same files as the table in
  §1a, but the v3.1.11 v4-only versions — prefer these). Plus `xfsprogs/db/` is the
  source of `xfs_db` (our fixture-poker / oracle), `xfsprogs/mkfs/` shows
  blank-volume construction (useful for `create_blank_xfs` tests).
- **`xfuse/src/libxfuse/`** — independent Rust XFS reader; modules
  (`sb.rs`, `dinode.rs`, `bmbt_rec.rs`, `da_btree.rs`, `dir3_*.rs`,
  `btree.rs`, `file_btree.rs`) are a field-level second opinion for our
  parser. Note it targets v5/dir3, so it confirms shared structures but is
  not a v4 authority.
- **`xfsprogs/repair/dir2.c` + `xfs-dir1-2.4.2-sgi/xfs/xfs_dir.c`** — dir2 and
  dir1 repair/format respectively (dir1 only exists in the SGI tree).
- **`xfs_db`** + **`xfs_repair -n`** (build from `xfsprogs/`) — the
  fixture-poker and the differential **oracle** for every test (see §6).
- **`xfs_filesystem_structure.pdf`** ("XFS Algorithms & Data Structures",
  3rd ed.) — the on-disk format bible (SB/AGF/AGFL/AGI, btree layouts, dir
  formats). **`File System Forensic Analysis.pdf`** (Carrier) — prose XFS
  chapter for when the spec is terse.
- **`xfs-online-fsck-design.html`** (Darrick Wong) — v5/rmap-centric, so
  *shape only*; the "rebuild btrees by regenerating all records in memory
  then writing fully-formed blocks" technique informs R2/R3.

---

## 2. Why the modern `scrub/` engine does NOT port to v4

The kernel's online repair (`scrub/*_repair.c`, `newbt.c`, `reap.c`) is
elegant but built on a foundation v4 lacks. From `scrub/repair.h`:

```c
struct xrep_find_ag_btree {
    uint64_t  rmap_owner;   /* find the btree by asking the rmap who owns it */
    ...
};
```

Every modern repair rebuilds a structure by **querying the reverse-mapping
(rmap) btree** for "which blocks are owned by this btree / this inode," then
bulk-loads a replacement. **v4 has no rmapbt.** Without it, you cannot ask
the disk "who owns block X" — you must *reconstruct* that ownership by
scanning every inode and every btree yourself. That is exactly what offline
`xfs_repair` does and what the kernel scrub does **not** do.

So the porting strategy is:

- **Take from `scrub/`:** the *shape* of orphan reconnection
  (`orphanage.c`), the staging/bulk-load pattern (`newbt.c` +
  `libxfs/xfs_btree_staging.c`), and the per-structure check ordering
  (`agheader.c`, `alloc.c`, `ialloc.c`, `inode.c`, `bmap.c`, `dir.c`).
- **Take from `xfsprogs/repair/` (to be cloned):** the in-memory
  block-ownership map and the full-scan reconstruction driver.
- **Build ownership ourselves** by a full inode walk (we already enumerate
  inodes for the verifier — §`xfs_fsck.md` Phase 2/3).

---

## 3. Prerequisite — XFS write primitives (`src/fs/xfs/write/`)

Nothing can be edited or repaired until these exist. Build and unit-test them
in isolation against round-trip-readable images **before** wiring any
user-facing operation. New module group `src/fs/xfs/write/` (or extend the
existing per-area modules):

3.1 **Block I/O + sb writeback.** Sector-aligned write of a single fs block;
    update primary sb and **all per-AG sb replicas**; recompute lazy sb
    counters (`sb_fdblocks`, `sb_ifree`, `sb_icount`) — v4 may use
    `LAZYSBCOUNTBIT`, in which case the authoritative counts live in the AGF/
    AGI and the sb copy is advisory. Ref: `libxfs/xfs_sb.c`.

3.2 **Free-space allocator.** Allocate/free extents by editing the `bnobt`
    + `cntbt` pair and the AGFL, keeping `agf.freeblks` in sync. Start with
    the simplest correct policy (first-fit from `bnobt`); no need to match
    XFS's real allocator heuristics. Ref: `libxfs/xfs_alloc*.c`,
    `xfs-dir1-2.4.2-sgi/xfs/xfs_alloc_btree.c`.

3.3 **Generic btree mutation.** Insert/delete a record with node split/merge
    and root grow/shrink, parameterized over key/record/pointer sizes so the
    same engine serves `bnobt`, `cntbt`, `inobt`, and `bmbt`. This is the
    hardest core piece — model on `libxfs/xfs_btree.c`. We already have a
    *read* btree walker (`walk_bmbt`); this is the write counterpart.

3.4 **Inode allocation.** Allocate an inode: find/extend an inode chunk via
    `inobt`, flip the free-mask bit, init the dinode core. Free the reverse.
    Maintain `agi.count`/`agi.freecount`. Ref: `libxfs/xfs_ialloc*.c`.

3.5 **Inode core + fork writeback.** Encode `XfsDinodeCore` back to disk;
    grow a data fork local→extents→btree as it outgrows each format; append
    extents via `bmbt`. Ref: `libxfs/xfs_inode_buf.c`, `xfs_bmap.c`.

3.6 **Directory mutation.** Add/remove `(name, ino)` in dir2 across all four
    formats (sf → block → leaf → node) with the format upgrades, plus dir1
    for pre-dir2 disks. Maintain the hash leaf. Ref: `libxfs/xfs_dir2_*.c`;
    dir1 from the SGI tree.

3.7 **Snapshot/rollback guard.** Same pattern as HFS Step 2 / EFS: capture
    touched sectors (or the whole AG header region + dirty blocks) before a
    mutation, restore on error. Cheap because operations are AG-local.

> **No CRCs, no journal.** v4 means none of these recompute a checksum or
> write a log record — a major simplification vs the kernel code, which
> interleaves both everywhere.

---

## 4. Phase plan — EDIT

Depends entirely on §3. Ships as its own PR series after the write primitives
land and pass round-trip tests.

- **E1 — `EditableFilesystem` skeleton.** Widen the impl to
  `R: Read + Write + Seek + Send`; route through `open_editable_filesystem`
  in `src/fs/mod.rs` (XFS branch). Implement `free_space()` from AGF
  summaries.
- **E2 — create_file / create_directory.** Compose 3.4 (alloc inode) + 3.5
  (init core/forks) + 3.6 (link into parent dir) + 3.2 (alloc data blocks
  for dir/file content). Honor `CreateFileOptions`. Bump parent nlink for
  directories.
- **E3 — delete_entry / delete_recursive.** Reverse of E2: unlink from
  parent, free forks (3.2), free inode (3.4). Decrement nlink; handle the
  `xfs_iunlink` unlinked list only insofar as we zero it (offline, no
  open-file semantics).
- **E4 — sync_metadata.** Flush dirty sb/AGF/AGI; reconcile counters. Caller
  responsibility per the project's edit-mode convention (memory: mutation
  methods modify in-memory state only).
- **E5 — Browse-view + staged-edit integration.** Reuse the existing
  `StagedEdit` queue, projected-free-space banner, virtual overlay. No XFS
  specifics in the GUI — it dispatches on the trait.
- **E6 — Dirty-log guard.** Refuse edit mode on a volume with a non-clean
  log (detect via log head/tail); surface a clear message. (Mirrors HFS+.)
- **E7 — CLI parity.** Wire into `rb-cli` browse-edit, per CLAUDE.md
  GUI/CLI-parity rule.

---

## 5. Phase plan — FULL REPAIR

Builds on §3 primitives + the §`xfs_fsck.md` verifier (which already
enumerates inodes, walks btrees, and detects orphans). Offline,
full-scan, `xfs_repair`-style. Each phase mirrors an `xfs_repair` phase.

> **Model citations below point at `xfsprogs/repair/` @ v3.1.11 (v4-only),
> not the modern `scrub/`.** This version has **no `rmap.c`** (the ownership
> map *is* `incore.c` + `incore_ext.c`) and **no staging loader** (`phase5.c`
> hand-rolls the btree build). The exact file/struct mapping is in
> **Appendix A**; the prose below is the conceptual shape.

- **R1 — In-memory block-ownership map (the rmap substitute).** Full inode
  scan: for every allocated inode, mark every block its forks claim in a
  per-AG block-state map (Appendix A.1: the `XR_E_*` states). Pre-seed the
  static metadata blocks (sb/AGF/AGI/AGFL/btree-root blocks) as `INUSE_FS` /
  `FS_MAP`. A block claimed twice flips to `MULT` (= cross-linked/duplicate)
  — double-allocation detection falls out for free. Model:
  `repair/incore.h` + `repair/incore_ext.c` (the map), `repair/scan.c` +
  `repair/dino_chunks.c` (the inode/block walk that fills it). This is
  `xfs_repair` **phase 3 + phase 4**.

- **R2 — Rebuild free-space btrees (`bnobt` + `cntbt`).** Invert the R1 map:
  every block left `UNKNOWN`/`FREE` is free. Coalesce into extents, sort, and
  build fresh `bnobt`/`cntbt` bottom-up (fill leaves to the btree's fill
  factor, then build parent levels). Rewrite `agf` roots/levels/`freeblks`.
  Model: `repair/phase5.c` (`build_freespace_tree` / the incore-slice
  builder); records via `libxfs/xfs_alloc_btree.c`.

- **R3 — Rebuild inode-allocation btree (`inobt`).** From the R1 inode tree,
  emit one record per 64-inode chunk with its free mask, build the `inobt`
  bottom-up like R2; rewrite `agi.root/level/count/freecount`. Model:
  `repair/phase5.c` (`build_ino_tree`); records via
  `libxfs/xfs_ialloc_btree.c`.

- **R4 — Repair AG headers + AGFL.** Rewrite AGF/AGI summary fields, the
  AGFL, and the per-AG sb replica from R1–R3 results; zero stale
  `agi_unlinked` buckets. Model: `repair/phase5.c` (`build_agf_agfl` /
  `sync_sb`) and `repair/agheader.c` (`secondary_sb_whack`).

- **R5 — Inode core repair.** Per inode: validate/repair mode, di_size vs
  fork extent total, timestamps, clear impossible flags; truncate forks whose
  extents R1 flagged out-of-bounds or `MULT`. Model: `repair/dinode.c`
  (`process_dinode`, `process_inode_data_fork`). (nlink correction is R7.)

- **R6 — Directory + connectivity repair.** BFS from root following dir
  entries; verify `.`/`..`; drop entries pointing at free/invalid inodes
  (`DanglingEntry`); rebuild a dir's leaf/free index when data blocks are
  intact but the index is not. Tally references into `counted_nlinks` and set
  `ino_reached`. Model: `repair/phase6.c`, `repair/dir2.c`
  (dir2) + `xfs-dir1-2.4.2-sgi/xfs/xfs_dir.c` (dir1).

- **R7 — Orphan reconnection + nlink correction.** Any allocated, valid inode
  with `ino_reached == 0` after R6 is linked into `lost+found/` (created
  under root if absent), named by inode number; then rewrite every inode's
  `di_nlink` from the R6 `counted_nlinks`. Model: `repair/phase6.c`
  (`mk_orphanage` / `mv_orphanage`) + `repair/phase7.c` (`set_nlinks`).

- **R8 — Final verify + seal.** Re-run the full `xfs_fsck.md` verifier; refuse
  to commit if it still reports structural errors. Snapshot-guard the whole
  repair so a mid-run failure restores the original image (offline images
  make this affordable — optionally operate on a temp copy and swap on
  success, like the destructive-re-encode path).

**Repairability tiering** (surfaced via the shared `FsckIssue.repairable`):
once R1–R7 exist, the codes that `xfs_fsck.md` marked `repairable = false`
(`BadAgHeader`, `FreeBtree*`, `InodeBtree*`, `AgSummaryMismatch`,
`DoubleAllocation`, `OrphanInode`, `DanglingEntry`) become repairable. What
stays unrepairable: a corrupt **primary superblock geometry** with no usable
replica (can't even establish AG layout), and a **dirty log** we won't replay
(offer "zap log and repair" = `xfs_repair -L` semantics, with a clear data-
loss warning).

---

## 6. Testing & validation strategy

The strongest asset here is that **`xfsprogs` is the oracle**:

- **Fixture generation (Linux/CI helper):** `mkfs.xfs -m crc=0 ...` forces
  v4. Build small images with known directory trees. For dir1 coverage, use
  the historical mkfs or hand-craft from the SGI tree.
- **Corruption crafting:** `xfs_db` write commands to poke specific damage
  (zero an AGF root, flip an inode free mask, unlink an inode's dir entry,
  corrupt a `bnobt` record). Keep an `examples/xfs_poke.rs` mirroring the
  HFS+ journal poke tools for in-repo crafting.
- **Differential oracle:** after our repair, run `xfs_repair -n` (no-modify)
  on the same image — it must report **no remaining errors**. Run
  `xfs_db -c check`. Mount under Linux loopback and `diff -r` the tree
  against the pre-corruption fixture. This is the gold standard: "our repair
  agrees with xfs_repair."
- **Round-trip edit tests:** create N files/dirs via our edit API, then
  validate with `xfs_db`/`xfs_repair -n` and by mounting + reading back.
- **On-hand real images:** `~/Downloads/IRIX6.5/irix65.chd` (v4, dir2) and
  `~/xfs-efs/ULTRA64_2GIG_SCSI_IRIX53_confirmed2.img` from
  `docs/SGI_Filesystems.md` — use read-only first, then copies for
  edit/repair smoke tests.

---

## 7. Wiring (same trait surface as every other FS)

- `impl EditableFilesystem for XfsFilesystem` (edit) and `fn repair()` →
  `RepairReport` (full repair), both via shared `src/fs/fsck.rs` types —
  no GUI changes (memory: GUI uses shared types only).
- Dispatch through `open_editable_filesystem_by_string` / the `0xA0` /
  `0x83`+XFSB / GPT-Linux-GUID branches already in `src/fs/mod.rs`.
- `is_checkable_type()` / Check + Repair buttons light up automatically once
  `fsck()`/`repair()` return `Some`.
- CLI: add to `rb-cli` per the GUI/CLI-parity rule (`docs/cli-todo.md`).

---

## 8. Recommended order & shippable milestones

**Revised order (repair-first — see Appendix A.4).** Full repair rebuilds
whole btrees *bottom-up* and therefore does **not** depend on the §3
incremental allocator / generic btree-mutation primitives; **edit** does. So
for the user's actual goal ("my IRIX disks get corrupt"), repair is both the
higher priority and the *earlier* deliverable:

1. **Verifier** (`docs/xfs_fsck.md` Phases 1–3) — read-only, ship first.
   Independent of everything below.
2. **In-memory maps + R1 reporting** (Appendix A.1/A.2 + A.4 #1–2) — pure
   in-memory, driven by the existing read-side inode walker. Adds
   double-allocation / stray-block / orphan *reporting* with **zero write
   code**. Days, not weeks.
3. **Bottom-up btree builders** (A.4 #3) — the first write; the riskiest
   piece. Gate hard on read-back + `xfs_db` + `xfs_repair -n` clean.
4. **Full repair** (§5 R2→R7) — wire builders into AGF/AGI rewrite, then
   inode/dir/orphan/nlink fixes. Each R-phase independently oracle-tested.
5. **Write primitives** (§3) — only now, because they're a prerequisite for
   *edit*, not repair.
6. **Edit** (§4 E1–E7) — last, built on §3.

Conservative repair (`docs/xfs_fsck.md` Phase 4) is subsumed once R2–R7
exist; it ships earlier only as a stepping stone if full repair slips.

Each numbered item is its own PR series. Do **not** land any write step
before it is independently proven against `xfs_db` / `xfs_repair -n` —
writing XFS btrees incorrectly silently corrupts volumes, and the whole
value proposition here is *not* making the user's IRIX disks worse.

---

## 9. Hard constraints / non-goals

- **v4 only.** v5 (CRC/rmap/reflink/finobt/parent-ptr) remains rejected at
  open. A v5 repair is a separate, much larger project and modern `scrub/`
  *would* be the reference for it.
- **No log replay.** Dirty-log volumes: refuse edit; allow repair only with
  an explicit "discard log" acknowledgement (= `xfs_repair -L`).
- **No realtime device, quota, attr-fork repair** in v1. Attr forks are
  preserved if intact, dropped if damaged (note it in the report).
- **Allocator is correctness-first, not XFS-faithful.** We don't need XFS's
  real allocation policy, alignment, or AG-rotation heuristics — only to
  produce a structurally valid, `xfs_repair`-clean result.

---

## Appendix A — Code-ready data model (translated from `xfsprogs/repair/`)

This is the concrete plan to start coding the repair side. It is a direct
Rust translation of the 2013 `xfs_repair` in-core model. New module group:
`src/fs/xfs/repair/`.

### A.1 Block-state map — `repair/incore.h` → `src/fs/xfs/repair/blockmap.rs`

`xfs_repair` tracks every fs block's state in a per-AG map. Translate the
`XR_E_*` enum verbatim (the names are load-bearing — match them so the C is
greppable next to our code):

```rust
#[derive(Clone, Copy, PartialEq, Eq)]
pub enum BlockState {            // repair/incore.h XR_E_*
    Unknown,   // XR_E_UNKNOWN  0  not yet seen
    Free1,     // XR_E_FREE1    1  free per ONE of the two space btrees
    Free,      // XR_E_FREE     2  free per BOTH space btrees
    InUse,     // XR_E_INUSE    3  claimed by a file/dir data or metadata
    InUseFs,   // XR_E_INUSE_FS 4  AG header / log (immovable)
    Mult,      // XR_E_MULT     5  multiply claimed -> cross-link to resolve
    Ino,       // XR_E_INO      6  inode chunk block
    FsMap,     // XR_E_FS_MAP   7  space/inode btree block
}
```

- Storage: one `Vec<BlockState>` of length `agblocks` per AG (the C packs 4
  bits/block; a byte/block `Vec` is fine at our image sizes and far simpler).
- The transition that matters: setting a block already `InUse`/`Ino`/`FsMap`
  to in-use again → `Mult`. That single rule is the whole double-allocation
  detector (R1). Mirror `set_bmap_ext` / `get_bmap_ext`.
- Free-extent inversion for R2: walk the map, emit `(agbno, len)` runs of
  `Unknown`/`Free` — that is the free-space record set, no separate bno/bcnt
  AVL trees needed (the C keeps them only for allocation during repair; we
  rebuild bottom-up so a sorted `Vec` suffices).

### A.2 Inode map — `repair/incore_ino.h` `ino_tree_node_t` → `src/fs/xfs/repair/inomap.rs`

Per-AG, keyed by 64-inode chunk start (`INOS_PER_IREC = 64`). Per chunk:

```rust
pub struct InoChunk {                 // ino_tree_node_t
    pub start_agino: u32,             // ino_startnum
    pub free_mask:   u64,             // ir_free   (bit=1 -> free)
    pub confirmed:   u64,             // ino_confirmed
    pub is_dir:      u64,             // ino_isa_dir (bit per inode)
    pub disk_nlinks: [u32; 64],       // disk_nlinks  (read in P3)
    pub counted:     [u32; 64],       // counted_nlinks (tallied in R6/P6)
    pub reached:     u64,             // ino_reached  (BFS connectivity)
    pub processed:   u64,             // ino_processed
    pub parents:     [u64; 64],       // parent ino per slot (P6 reconnect)
}
```

- `disk_nlinks` is set during the P3 inode sweep; `counted` is tallied during
  the R6 directory walk; R7 writes `counted` back to disk where they differ
  (this is literally `repair/phase7.c::set_nlinks`).
- `reached` drives orphan detection: any `confirmed && !free && reached==0`
  inode is an orphan for R7.
- Keep a second "uncertain" inode map exactly as the C does
  (`add_inode_uncertain`) for inode chunks discovered by scanning that aren't
  yet confirmed by the inobt — needed only when the inobt itself is trashed.

### A.3 Phase ↔ file ↔ our-module map

| `xfs_repair` phase | What it does | Source to port | Our module / R-phase |
|---|---|---|---|
| phase1 | find + validate primary sb (vs secondaries) | `phase1.c`, `agheader.c::verify_set_primary_sb` | verifier P1 (`xfs_fsck.md`) |
| phase2 | zero/handle log; check AGF/AGI/AGFL; seed block map with FS metadata | `phase2.c`, `agheader.c`, `sb.c` | `repair/blockmap.rs` seed + verifier P1 |
| phase3 | walk every inode; validate cores/forks; fill block map + inode map; read disk nlinks | `dino_chunks.c`, `dinode.c`, `scan.c` | **R1** + R5 inode checks |
| phase4 | resolve duplicate (`MULT`) blocks; decide keeper; finalize free map | `phase4.c`, `dinode.c` | **R1** finalize |
| phase5 | rebuild bnobt/cntbt/inobt + AGF/AGI/AGFL + sb counts, bottom-up | `phase5.c` | **R2 + R3 + R4** |
| phase6 | dir connectivity BFS; fix/rebuild dirs; mk + populate `lost+found` | `phase6.c`, `dir2.c`, SGI `xfs_dir.c` | **R6 + R7 reconnect** |
| phase7 | rewrite di_nlink from counted refs | `phase7.c` | **R7 nlink** |

### A.4 First code to write (smallest provable units, in order)

1. `blockmap.rs` + `inomap.rs` (pure in-memory; unit-test the `Mult`
   transition and free-extent inversion with hand-built inputs). No disk I/O.
2. **R1 fill** — drive the maps from the *existing read-side* inode walker in
   `src/fs/xfs/mod.rs` (we already enumerate inodes + decode extents for the
   verifier). At this point we can already *report* double-allocations,
   stray blocks, and orphans with zero write code — ship as verifier output.
3. Bottom-up btree **builders** (`btree_build.rs`) for bnobt/cntbt/inobt —
   emit fully-formed AG btree blocks from a sorted record `Vec`. Validate by
   building, then reading back with our own reader **and** `xfs_db`. This is
   the first *write*; gate hard on `xfs_repair -n` clean before R4 wires it
   into a real AGF/AGI rewrite.
4. Only then the §3 general allocator / generic btree-mutation primitives —
   note **repair does NOT need them** (it rebuilds whole btrees bottom-up),
   so **full repair can ship before edit**. Edit (§4) is what needs the
   incremental insert/split allocator. Reorder §8 accordingly if repair is
   the priority: verifier → R1 reporting → bottom-up builders → R2/R3/R4 →
   R5/R6/R7 → (then) §3 primitives → §4 edit.
