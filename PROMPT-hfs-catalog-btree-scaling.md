# rb-cli: HFS catalog B-tree writer corrupts at ~7.4k files ("no free B-tree nodes")

## Problem

Writing many files/directories into a single classic-HFS volume fails at roughly
**7,400 catalog entries** with:

```
error: ... disk full: no free B-tree nodes
```

…even though the catalog B-tree file is only **~20% full**. The real volume is
mostly empty (gigabytes of free data space, thousands of free B-tree nodes), so
this is **not** genuine exhaustion — it's a writer bug. `fsck` on an affected
volume reports widespread catalog index corruption:

```
ERROR  [IndexSiblingLinkBroken] index node 8: fLink = 0 but expected 19
ERROR  [IndexSiblingLinkBroken] index node 19: bLink = 0 but expected 8
... (one pair per index node up the tree)
```

### Self-contained repro (rb-cli only, no external tooling)

```sh
mkdir -p repro_files/d
python3 -c 'for i in range(9000): open(f"repro_files/d/f{i:05d}.txt","w").write("x")'
tar -cf repro.tar -C repro_files d

rb-cli new --fs hfs --size 2000M repro.hfv      # 32 KiB blocks, ~10 MiB catalog
rb-cli untar repro.hfv repro.tar /
# -> importing... 4999 files, 1 dirs
# -> error: importing repro.tar: create_file d/f07386.txt: disk full: no free B-tree nodes
```

It dies at file **#7386**. The exact threshold drifts a little with record sizes
and tree shape (a real mixed files+dirs volume failed at ~7,890 records) but is
always ~7–8k. Both block sizes hit it: `new` picks 32 KiB blocks here; the
`expand` path picks 64 KiB — both fail the same way.

### Evidence the catalog is NOT actually full

From the HFS MDB of a real failed volume (2 GB, 64 KiB alloc blocks):

| field | value | meaning |
|---|---|---|
| `drCTFlSize` | 10,496 KiB | catalog B-tree file = **20,992 nodes** @ 512 B |
| `drFilCnt` / `drDirCnt` | 6,720 / 1,170 | ~7,890 records → needs only **~2,400 nodes** |
| `drFreeBks` | 19,605 blocks | **1.28 GB** of data space free |

So ~18,500 of 20,992 catalog nodes were free when allocation "failed."

## Root-cause analysis (hypothesis — please confirm)

The catalog *sizing* is fine; the catalog *writer* corrupts the index tree:

- **`src/fs/hfs.rs` → `pick_btree_node_size()`** ignores its argument and always
  returns **512**. Tiny nodes ⇒ a deep tree (~2,400 nodes for 7.9k records) ⇒ a
  great many leaf splits.
- **`src/fs/hfs.rs` → `insert_catalog_record()`** (~line 891): on a full leaf it
  calls `hfs_common::btree_split_leaf_with_insert(...)` and then **rebuilds the
  whole index on every split** via `hfs_fsck::rebuild_index_nodes(...)`.
- At ~7.4k records the index rebuild leaves index nodes with **zeroed sibling
  links** (`IndexSiblingLinkBroken`) and the free-node accounting / map-node
  bitmap (`hfs_map_nodes_required`, `init_map_node` in `src/fs/hfs_common.rs`)
  desyncs, so the next allocation can't find a free node → "no free B-tree
  nodes." The B-tree file can't grow, so it surfaces as a spurious disk-full.

The sizing helpers are believed correct and probably need no change:
`default_btree_sizes()` (~line 2320, 0.5 % of volume), `clone_target_btree_sizes()`
(~line 2351, floors at the blank default), `HFS_MAX_BTREE_FILE_SIZE = 16 MiB`
(~line 2039).

## Fix (candidate directions)

- **A — scale the node size.** Make `pick_btree_node_size()` return a larger node
  (e.g. 4096) for large catalogs: ~4× more records per node, an order of magnitude
  fewer splits/rebuilds, and a wider header free-node bitmap. Likely sidesteps the
  corruption and lifts the ceiling well past any classic-HFS volume's file count.
  (Mac OS reads node size from the B-tree header, so 1024/2048/4096 are all
  on-disk-compatible.) This may be enough on its own.
- **B — fix the writer.** Correct `rebuild_index_nodes` sibling-link / free-node
  bookkeeping, or replace insert-then-rebuild-per-split with a **bulk bottom-up
  B-tree build** (sort all records once, pack leaves, build index levels). Bulk
  build is the classic `hformat`/newfs approach and is both correct and far faster
  for large imports (`untar`, the MacAtrium harvest).

Either way the win condition is "tens of thousands of files in one HFS volume,
fsck-clean, mountable on real Mac OS."

## Acceptance criteria

```sh
# the repro above completes with all 9000 files:
rb-cli untar repro.hfv repro.tar /          # no error
rb-cli fsck repro.hfv                        # 0 errors, no IndexSiblingLinkBroken

# and a stress level well past the real workload (full MacAtrium volume is ~285
# apps ≈ 7.9k records today, and will grow):
#   build a ~60k-file tar and untar it into a 2 GB HFS volume cleanly.
```

- `fsck` clean (no new errors) after large imports via both `untar` and repeated
  `put`/`mkdir`.
- A volume built this way mounts and boots on a real/emulated classic Mac
  (the eventual consumer boots it on a Quadra 800 under System 7.5.5).
- Add a regression test that imports ≥ 20k entries into one HFS volume.

## Related: invalid-JSON for control-char filenames (the "JSON thing")

Discovered in the same MacAtrium build session; include it here for context. It
was a **consumer-side bug, already fixed in MacAtrium** — recorded so this branch
can decide whether rb-cli wants any matching hardening.

- MacAtrium's harvest serialized dataset stub lines with Rust's `{:?}` (Debug)
  format, which escapes control chars as `\u{7f}` (curly braces) — **invalid
  JSON**. A Macintosh Garden title (`IPNetRouter␡␡_154_68k`, two raw DEL/`0x7F`
  bytes in its name) produced an unparseable line and crashed the build. Fixed in
  MacAtrium by escaping via `serde_json` (`tools/atrium-tool/src/harvest.rs`,
  `stub_line`/`jstr`).
- **rb-cli angle (optional):** confirm rb-cli round-trips filenames containing raw
  control bytes (`0x7F` and `0x01–0x1F`) through HFS paths losslessly. Empirically
  it already does — a curated entry whose leaf name carries raw `0x7F` bytes
  builds and lists fine — but a test pinning that behaviour (`put` then `ls`/`get`
  a `0x7F`-named file, bytes preserved) would lock it in. No code change expected
  unless the test surprises you.

## Notes for the consumer side (MacAtrium)

- The failing call was `rb-cli mkdir <disk> /MacAtrium/images`, but the user only
  saw a *later* confusing error — `put ... error: path component not found:
  images` — because MacAtrium's `RbCli::mkdir_p` (`tools/atrium-tool/src/rbcli.rs`)
  **swallows `mkdir` errors** (`let _ = self.run(...)`). That's a MacAtrium-side
  papercut to fix separately (surface the real "no free B-tree nodes" sooner), not
  an rb-cli bug — noting it so the failure mode is understood.
- Consumer need: MacAtrium "full library" disks (System 7.5.5 / Quadra 800, 2 GB)
  put a full System Folder + hundreds of app folders into one HFS volume — today
  ~7.9k catalog records, growing as more titles gain sources. They need a single
  HFS volume to hold tens of thousands of files.
