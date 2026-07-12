# rb-cli: per-`put` catalog inserts pack the HFS B-tree too loosely → exhaust + corrupt near capacity

## UPDATE 2026-06-28 — your fix helped the simple cases; the BUILD still fails

After your latest rb-cli (target/release, Jun 28 07:34), the flat shuffled-`put`
repro below improved from failing at 2,414 to 3,144 (≈93% of `untar` density), and
**every simple per-`put` pattern I could synthesize now packs efficiently
(~0.3–0.5 nodes/record) and is fsck-clean:** flat shuffled `put`, multi-parent
round-robin `put`, `put` + `setrsrc` (resource forks), and `put` into an
`expand`ed base. Good progress.

**But the real MacAtrium build STILL fails identically** — `no free B-tree nodes`
during the per-file art `put`, with `IndexSiblingLinkBroken` now appearing *early*
(catalog nodes 8/19/20). Measured on the failed 2 GB disk: 13,474 files / 2,230
dirs ≈ 17,934 records using ~20,480 of 20,480 catalog nodes = **1.14 nodes/record**
(vs ~0.35–0.5 for everything that works), 776 MB data still free. So the catalog is
~3× bloated *and* structurally corrupt, exactly as before — my simple repros just
don't trigger it.

**Prime untested suspect:** the build injects every app file via per-file
**`put-macbinary`** (both forks + full Finder info; rbcli.rs uses
`put-macbinary --force` + `setrsrc` + `chmeta`), with ~30-char names like
`battletanks-2-12.shot.24.pict`. `put-macbinary` writes a *bigger* catalog record
(both fork lengths + Finder info) than plain `put`, which would explain ~2.4×
bigger records — and it's the one path I couldn't easily reproduce. **Please repro
with `put-macbinary` at scale (thousands of macbinary files, long varied names,
into a few nested dirs) and watch for nodes/record climbing toward ~1 + early
`IndexSiblingLinkBroken`.** The reliable end-to-end repro remains the build itself;
corrupted artifact at `/home/dani/macatrium-755-full.hda`.

(Original write-up below — the flat-`put` parts are now FIXED; kept for the
root-cause discussion, which still applies to whatever `put-macbinary` is doing.)

---

## Relationship to the prior bug (the bulk path — now fixed)

Follow-up to `PROMPT-hfs-catalog-btree-scaling.md`. **That part is fixed** — the
bulk writer no longer corrupts at ~7.4k files: `rb-cli untar` now imports 20,000
files into both a fresh `new` HFS volume and an `expand`ed one, `fsck`-clean.
Confirmed against the current (uncommitted) HFS work on `remote-optical-ripping`.

**This is the remaining half:** the **incremental, one-record-at-a-time `put`
path** packs the catalog B-tree much less densely than the bulk path, so it runs
out of catalog nodes ("disk full: no free B-tree nodes") at a fraction of the
file count — and leaves `IndexSiblingLinkBroken` corruption near the ceiling.

## Problem

A catalog built by many separate `rb-cli put` calls (each: open image → insert
one record → save → close) ends up with the B-tree leaves only ~half full, so it
exhausts its node budget ~1.5–3× sooner than the same records inserted via one
`untar`. The denser the keys are spread (many parent dirs, longer names,
non-sorted insert order), the worse it packs.

This is exactly what the real consumer does: **MacAtrium's image build `put`s
thousands of small art files one at a time** into the volume. On a 2 GB disk the
build dies at **~18,038 catalog records** with
`create_file: disk full: no free B-tree nodes`, and `fsck` reports
`IndexSiblingLinkBroken`.

## Evidence (all on the current rb-cli, same binary)

**Realistic (2 GB disk, ~10 MiB / 20,480-node catalog):**

| Insert path | Records | Result |
|---|---|---|
| `untar` (bulk) into a fresh `new` 2000M disk | 20,000 | ✅ fsck-clean |
| `untar` (bulk) into an `expand`ed 7.5.5 disk | 18,265 | ✅ fsck-clean |
| **MacAtrium build** — per-file `put` (nested dirs, real names) | **18,038** | ❌ no free B-tree nodes + `IndexSiblingLinkBroken` |

The failed disk's MDB: catalog `drCTFlSize` = 10.0 MiB (20,480 nodes @ 512 B),
`drFilCnt` 13,578 / `drDirCnt` 2,230 ≈ **18,038 records using ~20,326 of 20,480
nodes (~1.13 nodes/record)**. A clean `untar` of ~18k records uses **~0.35
nodes/record** (≈⅓ as many) — i.e. per-`put` leaves are ~⅓ full. `fsck` shows
`IndexSiblingLinkBroken` at nodes ~20326–20331 (right at the 99%-full ceiling) and
also node 116.

**Compact, fast reproducer (100M disk, ~512 KiB / ~1024-node catalog):**

| Insert path | Fails at |
|---|---|
| per-file `put`, **shuffled** key order | **2,414 records** |
| per-file `put`, **ascending** key order | (packs tight — survives well past 2.6k) |
| `untar` (bulk) | 3,367 records |

So on flat short names, shuffled `put` packs ~1.4× looser than `untar`; with the
build's many-parent / longer-name pattern it's ~2.7× looser. **Ascending-order
`put` packs fine** — the trigger is inserts that land *mid-leaf* (non-sorted key
order), which the bulk path avoids.

## Self-contained reproducer

```sh
RB=target/release/rb-cli            # the build with the bulk fix

# (A) bulk untar — clean / dense:
mkdir -p u/d; python3 -c '[open(f"u/d/f{i:05d}.txt","w").write("x") for i in range(20000)]'
tar -cf u.tar -C u d
$RB new --fs hfs --size 2000M new.hfv
$RB untar new.hfv u.tar /            # 20,000 files
$RB fsck new.hfv                     # clean

# (B) incremental put, SHUFFLED key order — exhausts ~3x sooner (the bug):
$RB new --fs hfs --size 100M put.hfv
$RB mkdir put.hfv /d -q
echo x > x.txt
python3 -c 'import random; n=list(range(4000)); random.shuffle(n); print("\n".join("%05d"%i for i in n))' > order.txt
while read i; do $RB put put.hfv x.txt "/d/f$i.txt" -q || { echo "FAILED at f$i"; break; }; done < order.txt
# -> FAILED around f0xxxx after ~2414 puts: "create_file: disk full: no free B-tree nodes"
$RB fsck put.hfv

# Same 100M catalog via untar holds more (and a mid count, e.g. 3000, shows
# untar succeeds where shuffled put fails): rebuild order.txt with range(3000),
# untar a 3000-file tar -> OK, but the 2414-shuffled-put run above fails.
```

The MacAtrium build is the natural end-to-end repro; the failed artifact is at
`/home/dani/macatrium-755-full.hda` (fsck it to see the `IndexSiblingLinkBroken`).

## Root-cause hypothesis (please confirm)

When a single insert splits a full leaf, the split leaves **two ~half-full
leaves**, and because each `put` is its own load→insert→save→close cycle, later
inserts (which land in *other* leaves, since real keys aren't sorted) never refill
those halves. A catalog grown one record at a time therefore accumulates ~2× the
leaves of a packed tree (plus the extra index level that implies), so it needs
~2–3× the nodes and hits the file's node ceiling early. At the ceiling,
`btree_split_leaf_with_insert` + the per-split `rebuild_index_nodes` also leave
sibling `fLink`/`bLink` inconsistent (`IndexSiblingLinkBroken`), and node
allocation then fails. The bulk `untar` path inserts everything before the tree
settles, so it packs tight and stays clean.

## Fix directions

- **Pack denser on split.** Bias the split point so leaves stay ≥ ~⅔ full instead
  of a clean 50/50 — ideally an append-aware split (catalog keys under one parent
  arrive sorted within a `put`, but the *cross-`put`* order is arbitrary), and/or
  redistribute with an existing sibling before splitting.
- **Or maintain the index incrementally + correctly** instead of a full
  `rebuild_index_nodes` per split — that rebuild is also where the sibling links
  break near capacity.
- **Verify the load→insert→save→reload round-trip** preserves the B-tree exactly
  (used-node bitmap, sibling links, header counts), since the `put` path differs
  from the in-memory bulk path precisely in that it re-reads the tree every call.
- **Goal:** a catalog grown by N single `put`s uses ≈ the node count of the same
  N via `untar`, and stays `fsck`-clean to the volume's real record limit.

## Acceptance criteria

```sh
# (1) compact: shuffled per-put no longer exhausts far below untar
$RB new --fs hfs --size 100M put.hfv && $RB mkdir put.hfv /d -q
# put 3000 shuffled files -> all succeed, fsck clean
# and node usage within ~1.2x of the equivalent untar

# (2) end-to-end: the MacAtrium 7.5.5 full build (~18k+ records via per-file
#     art `put`) completes; the disk is fsck-clean and boots on the q800.

# (3) regression: put >= 20,000 entries one-at-a-time (shuffled, multi-dir) into
#     an HFS volume sized to hold them -> fsck clean, no IndexSiblingLinkBroken.
```

## Code pointers

- `src/cli/verbs/put.rs` — the per-file path (every invocation re-opens, inserts
  one, saves).
- `src/fs/hfs.rs:945` `insert_catalog_record` — per-insert split + `rebuild_index_nodes`.
- `src/fs/hfs_common.rs:1050` `btree_split_leaf_with_insert` — the split (packing
  density lives here).
- `src/fs/hfs_fsck/btree.rs:1146` `rebuild_index_nodes` — index / sibling-link
  maintenance (the `IndexSiblingLinkBroken` source).
- Compare against the bulk import path (`src/fs/tar_import.rs`), which packs
  correctly and stays clean — the two should converge on the same packed tree.

## Consumer

MacAtrium's `atrium image` build writes per-title art via individual `rb-cli put`
calls; that's what trips this. Once fixed, the QEMU q800 harness
(`tools/qemu-harness` in the MacAtrium repo) can boot-verify the full 7.5.5 disk.
