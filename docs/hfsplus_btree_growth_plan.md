# HFS+ catalog B-tree growth & variable-length keys — implementation plan

**Status:** complete (P1–P5 landed; §4b grow-on-full intentionally deferred).
- **P1 (key-format descriptor / variable-length index keys, §4a) — landed.** The
  shared B-tree helpers in `hfs_common.rs` are now key-format-aware
  (`BTreeKeyFormat`), the HFS+ catalog/attributes inserts and the defrag builders
  thread the matching descriptor, and classic HFS is byte-identical
  (`CLASSIC_CATALOG` delegates to the legacy `normalize_catalog_index_key`).
- **P2 blank sizing (§4c) — landed.** `build_blank_hfsplus_front` sizes the
  catalog from the volume (~0.5%, mirroring classic `default_btree_sizes`), and
  `create_blank_hfsplus_sized` lets clone/test callers pin a larger catalog. The
  volume-level gate is met: 20k shuffled multi-dir inserts into a sized blank,
  catalog depth ≥ 3, `hfsplus_fsck` clean.
- **P2 grow-on-full (§4b) — deferred** (see §4b note). Classic HFS itself has no
  grow-on-full path and relies solely on pre-sizing; we mirror that. The blank
  auto-sizes and the clone path already over-sizes its target catalog, so the
  remaining gap (a *foreign*, under-sized catalog filled past capacity via live
  `put`s) matches a pre-existing classic-HFS limitation. Revisit if a real
  workload needs it.
- **P3 (extents-overflow + attributes through the descriptor, depth-1 removed) —
  landed.** `insert_extents_overflow_record` now splits + grows the root like the
  catalog/attributes inserts (`HFSPLUS_EXTENTS`, fixed 10-byte index keys); the
  attributes path was already wired in P1 (`HFSPLUS_ATTRIBUTES`). Covered by two
  buffer-level multi-level tests (fixed- and variable-index-key) plus a real-path
  integration test: a 520-block fragmented file's 64 overflow records split the
  extents tree and read back byte-for-byte.
- **P4 (defrag/clone re-verification) — landed.** A 64 MiB source with a
  multi-level catalog (300 files / 10 dirs) clones via `stream_defragmented_hfsplus`
  to a target whose defrag-built catalog is itself multi-level, fsck-clean, and
  round-trips byte-for-byte — confirming the variable-length index keys behave
  through the clone path (`btree_insert_full` + `HFSPLUS_CATALOG`), not just the
  live-edit path.
- **P5 (density rotation for HFS+, §4d) — landed.** The three HFS+ live insert
  paths (`insert_catalog_record`, `insert_xattr_record`,
  `insert_extents_overflow_record`) now delegate to `hfs_common::btree_insert_full`
  — the same shared insert classic HFS uses — which tries a B*-style sibling
  rotation before splitting. Shuffled multi-dir catalog inserts pack to ~0.84 leaf
  occupancy (was ~0.69 with plain split). This also deleted ~250 lines of
  hand-copied split/grow dance from `hfsplus.rs`.

**All five phases (P1–P4 + P5) are complete; only the optional §4b grow-on-full
remains deferred.**

**Relationship to shipped work:** the classic-HFS catalog scaling work is done —
bulk import (`PROMPT-hfs-catalog-btree-scaling.md`) and the incremental per-`put`
packing (`PROMPT-hfs-catalog-incremental-put-packing.md`, commit on
`remote-optical-ripping`). This plan covers the **HFS+** equivalent, which is a
*different and more fundamental* problem and was explicitly left as future work.

---

## 1. Problem

HFS+ catalog (and extents-overflow / attributes) B-trees **cannot grow past a
single index level without corrupting**, and a freshly built HFS+ volume gets a
catalog so small it exhausts almost immediately. So adding more than a couple
dozen files to an HFS+ volume via the live edit path breaks the volume.

This is **pre-existing** and independent of the classic-HFS packing fix
(verified: behaviour is byte-identical before and after that commit).

### Evidence (probe: 3000 shuffled `create_file`s into a blank 256 MiB HFS+ vol)

```
create_file #24 failed: disk full: no free B-tree nodes
hfsplus: ok=24/3000 node_size=4096 total=4 free=0 used=4 depth=2 leaf_records=50
fsck errors=2:
  - "leaf record (node=2, idx=0) ... sorts Greater relative to the preceding
     record (node=1, idx=24); B-tree leaves must be strictly ascending"
  - "bitmap reports 11 blocks allocated but total_blocks - free_blocks = 35"
```

Two distinct defects in one shot: the catalog is tiny (4 nodes) **and** it
corrupts the moment it splits to a second leaf and needs an index separator.

## 2. Root cause

The shared B-tree helpers in `src/fs/hfs_common.rs` are hardwired for the
**classic HFS** key format and are reused verbatim by HFS+:

| Concern | Classic HFS | HFS+ | Shared code today |
|---|---|---|---|
| Key-length prefix | **1 byte** | **2 bytes** (big keys) | reads `record[0]` as a 1-byte len |
| Index separator key | fixed 37-byte normalized (`0x25`) | **variable length**, up to `maxKeyLength` (516) | `normalize_catalog_index_key` forces classic 37 |
| Key payload | `parentID(4) + nameLen(1) + MacRoman` | `parentID(4) + nameLen(2) + UTF-16BE` | classic offsets |

So when an HFS+ leaf splits:

- `btree_split_leaf_with_insert` computes the separator with
  `key_len = first[0] as usize` (`hfs_common.rs:1227`). For an HFS+ key the first
  byte is the **high byte of the 2-byte length** — usually `0` for short names —
  so the "separator" becomes a 1-byte slice.
- `btree_insert_into_index` then runs it through `normalize_catalog_index_key`
  (`hfs_common.rs:1600`), producing a 38-byte classic key full of zeros.
- `btree_find_insert_leaf` later compares `node[start..end-4]` with
  `HfsPlusFilesystem::catalog_compare`, which reads `parentID` at `key[2..6]`
  (`hfsplus.rs:1695`) — i.e. it expects the 2-byte length prefix. The normalized
  classic separator has the wrong shape → misrouted descent → records land in
  the wrong leaf → "leaves must be strictly ascending".

The code already knows this: `insert_extents_overflow_record` is deliberately
**depth-1-only** "because the existing HFS+ B-tree growth helpers normalize keys
assuming the 1-byte HFS-classic format" (`hfsplus.rs:2009-2012`).

Separately, `build_blank_hfsplus_front` reserves only ~4 catalog blocks and there
is **no grow-on-full path**, so even a correct tree would run out almost
immediately.

## 3. Scope

Three B-trees, two write paths:

- **Trees:** catalog, extents-overflow, attributes (all HFS+ big-key trees).
- **Paths:**
  - Live edit — `HfsPlusFilesystem::insert_catalog_record` (`hfsplus.rs:1807`),
    `insert_extents_overflow_record` (`:2013`), attributes insert (`:~2197`).
    These have their **own** copy of the find→split→grow dance (they call the
    shared `btree_split_leaf_with_insert` / `btree_insert_into_index` /
    `btree_grow_root`).
  - Clone/defrag — `src/fs/hfsplus_defrag.rs` builds catalogs via the shared
    `btree_insert_full` (`NODE_SIZE = 4096`, `:562`). It inherits the same broken
    normalize, so defrag-built HFS+ catalogs that exceed one leaf level are also
    suspect and must be re-verified once the fix lands.

## 4. Design

Make the shared B-tree machinery **key-format-aware** instead of classic-only.
Preferred approach: a small descriptor passed into the shared helpers, rather
than forking a second copy of the algorithm.

### 4a. `BTreeKeyFormat` descriptor

Introduce (in `hfs_common.rs`) a cheap, `Copy` descriptor derived from the
BTHeaderRec `attributes` + `maxKeyLength` (record bytes: `maxKeyLength` at
catalog offset 34..36 — already written at `hfs_common.rs:2435`; `attributes` at
offset 54..58):

```rust
struct BTreeKeyFormat {
    big_keys: bool,            // kBTBigKeysMask (0x2): 2-byte key length
    variable_index_keys: bool, // kBTVariableIndexKeysMask (0x4)
    max_key_len: u16,          // for fixed-index-key trees
}
```

- `key_len(record) -> usize` — read 1 or 2 byte length per `big_keys`.
- `key_portion(record) -> &[u8]` — length prefix + key bytes (the bytes a
  separator must carry).
- `make_index_key(first_key) -> Vec<u8>` — classic: `normalize_catalog_index_key`
  (fixed 37); HFS+ catalog: pass through the **variable** key verbatim (with its
  2-byte length); fixed-index-key trees: pad to `max_key_len`.

Thread `&BTreeKeyFormat` through the functions that currently bake in the classic
assumption:

- `btree_split_leaf_with_insert` (separator extraction, `:1227`)
- `btree_split_index_with_insert` (separator extraction, `:1570`)
- `btree_insert_into_index` (`normalize_catalog_index_key`, `:1600`)
- `btree_grow_root` (first-key extraction + normalize, `:1730/1737/1743`)
- `btree_find_insert_leaf` (already format-agnostic — it slices `end-4` and
  defers to `cmp`; double-check the index-key/child-pointer split for big keys)
- `btree_try_rotate_leaf` / `btree_update_index_separator` (`:1363`, `:1314`) —
  the rotation's in-place separator patch currently bails when lengths differ
  (the `#[must_use]` guard added in the packing fix); with variable index keys it
  must instead rebuild the separator record (length can change), which means the
  rotation may need to fall back to split when the new separator doesn't fit, or
  the parent index node must be re-packed. Simplest first cut: keep rotation
  **classic-only** and let HFS+ use plain split+grow until 4d.

Back-compat: classic HFS passes a `BTreeKeyFormat { big_keys:false,
variable_index_keys:false, max_key_len:37 }` and the behaviour is byte-identical
to today (lock this with a golden test).

### 4b. B-tree file growth on full — DEFERRED

> **Deferred (2026-06-28).** Implemented sizing (4c) instead and mirrored classic
> HFS, which has **no** grow-on-full path at all — it relies entirely on
> pre-sizing the catalog (`default_btree_sizes` / `create_blank_hfs_sized`). The
> HFS+ blank now auto-sizes the catalog from the volume and the clone/defrag path
> already over-sizes its target (3× source pad + volume-scaled floor), so the only
> remaining gap is a *foreign* volume whose catalog was built small elsewhere and
> then filled past capacity through live `put`s — the same limitation classic HFS
> has shipped with. The design below is retained for if a real workload needs it;
> the riskiest parts are the map-node bitmap extension and the extents-overflow
> spill, neither validated against real macOS here.

Today `btree_alloc_node` returns `DiskFull` when the fork's nodes are exhausted.
Add a grow step (catalog first, then extents/attributes) invoked from the live
insert paths and the defrag builder when `free_nodes == 0`:

1. Allocate N more allocation blocks for the B-tree fork (respect `clumpSize`).
2. Extend the fork extents in the Volume Header (`vh.catalog_file` etc.); spill
   to the extents-overflow tree if the 8 inline extents are full.
3. Grow `catalog_data`, bump `totalNodes`/`freeNodes`, and extend the node
   allocation bitmap — appending **map nodes** when the header-node bitmap fills
   (mirror `hfs_common`'s classic map-node logic; HFS+ at `node_size=4096` the
   header bitmap covers ~30,720 nodes, see `hfsplus_defrag.rs:1349/1378`).
4. Update the volume bitmap + VH free-block count.

### 4c. Blank catalog sizing

In `build_blank_hfsplus_front` (`hfsplus.rs:5167`) size the initial catalog from
volume size (Apple uses ~0.25% with a sane floor), like classic HFS's
`default_btree_sizes`, so a fresh volume holds thousands of records before 4b
ever fires.

### 4d. Density rotation for HFS+ (after 4a–4c work)

Once growth + variable keys are correct, give HFS+ the same packing win:

- Call `btree_try_rotate_leaf` from `HfsPlusFilesystem::insert_catalog_record`'s
  full-leaf branch (`hfsplus.rs:1829`), as `btree_insert_full` now does for
  classic.
- Make the rotation's separator update handle variable-length index keys
  (per 4a), or accept the classic-only fast-path + split fallback.

This is largely free once the format abstraction exists; do it last so the
correctness work isn't gated on it.

## 5. Phasing (each step independently testable / shippable)

1. **P1 — key-format descriptor (4a), catalog only. [DONE]** Variable-length
   index keys for the live `insert_catalog_record` path. `BTreeKeyFormat` threads
   through `btree_split_leaf_with_insert` / `btree_split_index_with_insert` /
   `btree_insert_into_index` / `btree_grow_root` / `btree_try_rotate_leaf` /
   `btree_update_index_separator` / `btree_insert_full`. Gate met at the
   catalog-buffer level: `btree_insert_full` with `HFSPLUS_CATALOG` grows a
   shuffled multi-parent catalog to depth ≥ 3 with strictly-ascending leaves and
   every record findable by descent
   (`test_hfsplus_catalog_variable_index_keys_grow_multilevel`). The
   *volume-level* fsck gate moves to P2 — a blank catalog is only 4 nodes today,
   so it can't reach depth ≥ 3 until 4c sizing / 4b growth land. Classic HFS
   output unchanged (the existing classic scaling/fsck tests still pass).
2. **P2 — blank sizing (4c) [DONE] + catalog growth (4b) [DEFERRED].** Gate met
   via sizing: `create_blank_hfsplus_sized` + the volume-scaled blank default let
   a 64 MiB volume with a 16 MiB catalog take 20k shuffled multi-dir inserts to
   depth ≥ 3, fsck clean, no premature `DiskFull`
   (`test_hfsplus_blank_sized_catalog_20k_inserts_fsck_clean`). 4b (grow the fork
   on `free_nodes == 0`) is deferred — classic HFS relies on pre-sizing alone and
   has no grow path either; the blank auto-sizes and the clone path over-sizes its
   target, so live growth is only needed for a foreign under-sized catalog (a
   pre-existing classic-HFS limitation).
3. **P3 — extents-overflow + attributes through the same descriptor. [DONE]**
   Removed the depth-1 restriction on `insert_extents_overflow_record` (it now
   splits + grows via `HFSPLUS_EXTENTS`); attributes already routed through
   `HFSPLUS_ATTRIBUTES` in P1. Gate met: buffer-level extents and attributes trees
   grow to depth ≥ 2 with strictly-ascending leaves and every record findable, and
   a real fragmented-file create splits the extents tree (64 overflow records) and
   reads back byte-for-byte (`test_hfsplus_extents_overflow_grows_multilevel`,
   `test_hfsplus_attributes_grows_multilevel`,
   `test_hfsplus_fragmented_file_splits_extents_overflow_btree_real_path`).
4. **P4 — defrag/clone re-verification. [DONE]** `hfsplus_defrag` builds its
   target catalog through `btree_insert_full` + `HFSPLUS_CATALOG` (P1), so a
   large-volume clone now rebuilds a multi-level catalog that is fsck-clean and
   round-trips (`stream_clone_of_multilevel_catalog_is_fsck_clean`: 300 files / 10
   dirs, source and target catalog depth ≥ 2).
5. **P5 — density rotation for HFS+ (4d). [DONE]** The HFS+ live inserts now go
   through `btree_insert_full` (which rotates into a sibling before splitting),
   exactly as classic HFS does. Gate met: shuffled multi-dir inserts pack ~0.84
   leaf occupancy (`test_hfsplus_catalog_shuffled_inserts_pack_densely`), up from
   ~0.69. `btree_try_rotate_leaf`'s separator update already handled variable
   keys (in-place when the new separator matches the old length — always true for
   the fixed-length and same-length-name cases — else it abandons the rotation and
   splits), so no further index-key work was needed.

## 6. Risks / gotchas

- **Case folding & HFSX binary compare** — separators must compare identically to
  leaf keys; `catalog_compare` already branches on `case_sensitive()`
  (`keyCompareType`). Variable separators must preserve the exact key bytes so
  the compare is consistent at every level.
- **Endianness** — every HFS+ field is big-endian; key length is a 2-byte BE
  prefix (`build_catalog_key`, `hfsplus.rs:1666`).
- **Journal** — writes to a journaled HFS+ volume must go through / respect the
  journal (`prepare_for_edit`, `hfsplus_journal`). Growth touches the VH, bitmap,
  and fork extents — all journaled metadata.
- **fsck coverage** — `hfsplus_fsck` must validate index-key↔child-first-key
  equality, sibling links, and node-bitmap/free-node accounting after growth and
  rotation (the classic fixes leaned on these checks; extend them for HFS+).
- **Map nodes** — the node-allocation bitmap spilling into map nodes is the exact
  area that broke classic HFS (`IndexSiblingLinkBroken`); replicate the classic
  map-node handling carefully for the larger 4 KiB nodes.
- **Don't regress classic HFS** — the descriptor must make the classic path
  byte-identical; pin it with a golden-image test.

## 7. Acceptance criteria

```text
1. Blank HFS+ volume + 20,000 shuffled multi-dir create_file()s -> all succeed,
   hfsplus_fsck clean, every record readable, no IndexSiblingLinkBroken / order
   errors, catalog depth >= 3.
2. Fragmented-file + xattr stress (extents-overflow + attributes past depth-1)
   -> fsck clean.
3. Large HFS+ clone/resize via hfsplus_defrag -> multi-level catalog, fsck clean,
   mounts on macOS / an emulator.
4. Classic HFS output byte-identical to pre-change (golden test).
5. (P5) shuffled-insert leaf occupancy ~80%+, matching classic HFS.
```

## 8. Key code pointers

- `src/fs/hfs_common.rs` — `normalize_catalog_index_key` (:505),
  `btree_split_leaf_with_insert` (:~1050, separator at :1227),
  `btree_split_index_with_insert` (separator at :1570),
  `btree_insert_into_index` (normalize at :1600), `btree_grow_root` (:~1700,
  :1730/1737/1743), `btree_find_insert_leaf`, `btree_try_rotate_leaf` /
  `btree_update_index_separator` (:1314/1363), `btree_alloc_node` (DiskFull),
  `BTreeHeader` (:520), `max_key_len` write (:2435).
- `src/fs/hfsplus.rs` — `insert_catalog_record` (:1807, split :1835, index
  :1864/1896), `insert_extents_overflow_record` (depth-1 note :2009),
  attributes insert (:~2197), `build_catalog_key` (:1666), `catalog_compare`
  (:1695) / `catalog_compare_keys` (:437), `create_blank_hfsplus` (:5148) /
  `build_blank_hfsplus_front` (:5167), fsck via `crate::fs::hfsplus_fsck::check`.
- `src/fs/hfsplus_defrag.rs` — `btree_insert_full` call sites, `NODE_SIZE=4096`
  (:562), map-node sizing (:1349/1378).

## 9. Out of scope

- HFS+ compression (decmpfs) and hard-link inode plumbing.
- Any new on-disk format; this is HFS+-spec-compliant B-tree behaviour the
  current code approximates with the classic format.
