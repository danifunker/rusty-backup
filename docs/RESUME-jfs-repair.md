# RESUME — JFS repair (filesystem_completion_plan §5)

**Pick this up on `m900` (Linux).** This is a self-contained prompt to continue
the JFS `repair()` work in a fresh session. Read it top-to-bottom, then start at
"Where to start coding".

---

## 0. One-paragraph mission

`docs/filesystem_completion_plan.md` step 5 is **JFS repair**. JFS today is
read-only + check-only (`src/fs/jfs.rs` + `src/fs/jfs_fsck.rs`). The goal chosen
by the user is the **full** repair: **orphan-inode adoption into `/lost+found/`**,
which pulls forward the JFS **edit primitives** (the "J.4b" milestone): a real
JFS **inode allocator** + inline **dtree entry insert** + **inode write-back**,
then wire `EditableFilesystem` for JFS + `open_editable_filesystem` dispatch so
`repair()` reaches it. Verify every write against the real `fsck.jfs`.

## 1. Why we moved to m900

macOS has **no `fsck.jfs`**. Hand-writing a JFS inode allocator with no oracle is
high-risk (an image our fsck calls clean but the Linux kernel corrupts). On m900
you get: native `mkfs.jfs`/`fsck.jfs`, the kernel `jfs` module (so you can
**mount and populate real fixtures**, not just forge bytes), and no scp round-trip.

### First: install jfsutils (you, with sudo)

Run this yourself in the session (the `!` prefix runs it here):

```
! sudo apt-get update && sudo apt-get install -y jfsutils
```

Optional, only if you want to mount + populate real multi-file fixtures rather
than forge orphans in image bytes:

```
! sudo modprobe jfs            # kernel jfs module (mount support)
! sudo apt-get install -y libguestfs-tools   # guestfish, mounts without root
```

Verify: `fsck.jfs -V` and `mkfs.jfs -V` should print `version 1.1.15`.

### The oracle harness already supports running here

`scripts/jfs-oracle.sh` auto-detects a **`local`** backend when `fsck.jfs` is on
`PATH`. So on m900:

```
scripts/jfs-oracle.sh mkfs /tmp/t.img 16     # real mkfs.jfs
scripts/jfs-oracle.sh verify /tmp/t.img      # real fsck.jfs -f -n  vs  our jfs_check → AGREE/DISAGREE
```

`examples/jfs_check.rs` is our verifier CLI (`cargo run --example jfs_check -- <img>`).
It has a `--repair` slot **reserved** but commented out until `repair()` lands —
wire it up as step (4) below so `verify` can test the repaired image end-to-end.

**Baseline already confirmed** (on the Mac, via the ssh backend): a fresh 16 MiB
`mkfs.jfs` image is clean under both `fsck.jfs` and our `jfs_check` → AGREE. Our
reader opens real mkfs.jfs 1.1.15 images correctly.

## 2. Scope decisions already made (do not re-litigate)

- **Full repair**, incl. orphan adoption into `/lost+found` (user chose this over
  the simpler "adopt into root, no allocator" and over "counter-reconciliation only").
- **Oracle-first** (this doc) before writing the allocator.
- `mkfs.jfs` creates **no `/lost+found`** (confirmed: fresh root is empty,
  `allocated_fileset_inodes=0`). So adoption must **create** lost+found → needs
  the inode allocator. That's the point of the milestone.

## 3. On-disk facts already gathered (authoritative — from the working reader)

All JFS2 is **little-endian**. Metadata pages are **4096 bytes** (`PSIZE`)
regardless of aggregate block size. Constants below are from `src/fs/jfs.rs`.

**Dinode** (512 B, `DISIZE`): di_number@8, ixpxd@16, size@24, nblocks@32,
nlink@40, uid@44, gid@48, mode@52 (POSIX type in low 16), mtime@72, type-area
(dtroot/xtroot)@224 (288 B), fastsymlink@256 (128 B).

**Inline dtree root** (`dtroot`, 288 B at dinode+224 = 9 × 32-B slots; slot 0 is
the header):
- flag@16 (u8; `BT_ROOT|BT_LEAF|0x80`), nextindex@17 (u8, active entry count),
  freecnt@18 (s8), freelist@19 (s8, head of free-slot chain, -1 sentinel),
  idotdot@20 (u32, parent inum), stbl@24 (s8[8], sorted slot indices).
- Slots 1..8 are 32-B entry slots. **ldtentry**: inumber@0(u32), next@4(s8,
  continuation slot or -1), namlen@5(u8), name@6 (UCS-2LE, ≤11 cp inline),
  index@28(u32, hash). Continuation **dtslot**: next@0(s8), cnt@1(u8),
  name@2 (UCS-2LE, ≤15 cp).
- Free-slot management: `freelist` points at a free slot; that slot's byte[0]
  (`next`) chains to the next free slot; `freecnt` counts them. Allocating N
  slots (1 head + continuations for names >11 cp) pops them off this chain.
  `stbl` is kept **sorted by name** (JFS binary-searches it) — insert the new
  slot index at the sorted position, bump `nextindex`.

**IAG** (Inode Alloc Group, 4096 B; page `1+iag_no` of FILESYSTEM_I's data):
- header: agstart@0(u64), iagnum@8, inofreefwd@12, inofreeback@16, extfreefwd@20,
  extfreeback@24, iagfree@28, **nfreeinos@32**, **nfreeexts@36**, inosmap@40
  (u32[4] summary: set bit ⇒ that extent-group has no free inodes), extsmap@56
  (u32[4]), pad to 2048.
- **wmap@2048** (u32[128], working alloc map), **pmap@2560** (u32[128], persistent
  alloc map — what fsck reads), **inoext@3072** (pxd_t[128], one inode extent per
  slot; each extent = 32 dinodes × 512 B = 16 KiB = 4 agg blocks).
- pmap/inoext bit convention: extent `e` allocated ⇒ `pmap[e/32]` bit `31-(e%32)`
  set (MSB-first) AND `inoext[e].length>0`. Per-inode: fino `f` lives in IAG
  `f/4096`, extent `(f/32)%128`, dinode index `f%32` within the extent.
  **⚠ our code's `IAG_PMAP_OFF=2560`/`IAG_INOEXT_OFF=3072` are correct; the
  `scripts/probe-jfs-fileset.py` offsets 84/628/1140 are a stale WRONG guess.**

**dinomap_disk** (page 0 of FILESYSTEM_I data): in_freeiag@0, in_nextiag@4,
in_numinos@8, in_numfree@12, then per-AG `in_agctl[]`. These per-AG + aggregate
free-inode counters must stay consistent for a kernel-faithful allocation — this
is the part with no reader coverage yet; **probe it on m900** (write a throwaway
probe or use `jfs_debugfs`) before trusting offsets.

**BMAP / dmap** (block allocation; needed only if lost+found's dtree spills off
inode or a new inode extent must be allocated): dbmap header@page0 mapsize@0,
nfree@8; per-dmap page (`logical (block>>13)+4`) nblocks@0, nfree@4, start@8,
wmap@`4096-512`, pmap@`4096-1024` (u32[256], MSB-first, block `b` = word
`(b%8192)/32` bit `31-(b%32)`). See `walk_bmap`.

**Fixture** `tests/fixtures/test_jfs.img.zst`: root is inline (flag 0x83,
nextindex=6, freelist=7, freecnt=2), entries bigdir(64) hello.txt(4) large.bin(31)
link.txt(5) subdir(32) tiny.txt(6). Extent 1 of IAG 0 (fino 32..63) holds subdir
(fino 32) and has free slots 33..63 — the existing `fsck_flags_orphan_inode` test
forges fino 34 there. Reuse that forging for the repair test.

## 4. Where to start coding (tasks, in order)

Local task list from the prior session (mirror into TaskCreate):

1. ~~Understand on-disk write structures~~ (done — §3 above).
2. **Writable-reader plumbing** on `JfsFilesystem<R>`: helpers bounded on
   `R: Read + Write + Seek` — `write_at(byte, &[u8])`, `write_dinode(fino, &JfsDinode)`
   (re-serialize the 512 B and put it at the fino's disk byte), `read/write_iag_page`,
   `read/write_fileset_logical_page` (via FILESYSTEM_I xtree). Add `sync_metadata`
   (writes flush immediately → no-op is fine) and `free_space` (from bmap nfree ×
   bsize, or dinomap for inodes — pick blocks).
3. **Edit primitives**:
   - `alloc_fileset_inode()` → find an IAG with `nfreeinos>0` and an allocated
     `inoext` with a free dinode slot (consult pmap + per-inode occupancy; a
     dinode slot is free when its 512 B are zero / di_number==0). Set the
     **wmap AND pmap** bit if allocating a whole new extent; for a free slot in
     an already-allocated extent the extent bit is already set — you're claiming
     an individual dinode, so also update the dinomap/IAG **free-inode counters**
     (`nfreeinos`, dinomap `in_numfree`, per-AG). **Verify against fsck.jfs after
     each sub-step.** If no free slot in any existing extent, allocate a new
     inode extent (4 blocks via the dmap allocator) — try to avoid this by
     reusing a free slot (fixture has plenty).
   - `dtree_insert(dir_fino, name, child_fino, is_dir)` → inline dtroot only:
     pop slots from the freelist for the ldtentry (+continuations), write the
     entry, insert into sorted `stbl`, bump nextindex, fix freecnt/freelist,
     grow `di_size`. Refuse (return a "would spill" error) if the dir is external
     or the freelist can't cover the name — repair handles a modest orphan count,
     so spilling is out of scope; surface it as unrepairable.
   - `write_dinode` for nlink / idotdot fixups.
4. **`repair_jfs`** in `jfs_fsck.rs` (bound `R: Read+Write+Seek`): re-run the
   verifier; for each `OrphanInode`: ensure `/lost+found` exists (find it in root;
   if absent, `alloc_fileset_inode` → init an empty dir dinode with idotdot=root,
   nlink=2, inline empty dtroot → `dtree_insert` "lost+found" into root, bump root
   nlink); then `dtree_insert` `ino_<inum>` into lost+found; if the orphan is a
   dir, set its idotdot=lost+found and bump lost+found nlink. Build a `RepairReport`.
   Then: `impl EditableFilesystem for JfsFilesystem<R: Read+Write+Seek+Send>` with
   `create_*`/`delete_*`/`rename` → `Unsupported`, real `sync_metadata`/`free_space`,
   and `repair()` → `repair_jfs(self)`. Add JFS to `open_editable_filesystem` +
   `open_editable_filesystem_by_string` in `src/fs/mod.rs`. Un-comment the
   `--repair` path in `examples/jfs_check.rs`.
5. **Tests + docs**: unit test (forge fino 34 orphan like `fsck_flags_orphan_inode`
   → `repair()` → our fsck clean + browse shows `/lost+found/ino_34`). **Oracle
   test**: `scripts/jfs-oracle.sh mkfs` → forge orphan → `repair` → `fsck.jfs -f -n`
   must be clean (gate the test on `fsck.jfs` presence like the fsck_msdos oracle
   tests). Then sync docs: `filesystem_completion_plan.md` §5 + row for JFS,
   `filesystem_coverage_audit.md`, README Filesystems table, and re-grade any
   MiSTer core in `full_MiSTer_support_status.md`. Per CLAUDE.md pre-commit sync.

## 5. Verification discipline (the whole reason for the oracle)

After **every** structural write, round-trip through `fsck.jfs -f -n`. Do not
trust our own fsck alone — it doesn't check the dinomap free-inode accounting or
the dtree hash index, so it will pass images the kernel rejects. The `verify`
verb (oracle vs ours) must say **AGREE (clean)** on the repaired image before the
task is done. If fsck.jfs complains, its phase output names the structure
(Phase 7 = inode alloc maps, Phase 8 = disk alloc maps) — that tells you which
counter you missed.

## 6. Artifacts from the setup session

- `scripts/jfs-oracle.sh` — local/docker/ssh backends (use `local` on m900).
- `examples/jfs_check.rs` — our verifier CLI (`--repair` reserved).
- Memory: `jfs-fsck-oracle-m900.md` (oracle setup + fixture facts).
- Prior session left the tree green; 62 JFS unit tests pass.
