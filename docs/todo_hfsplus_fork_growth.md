# HFS+ B-tree fork grow-on-full (§4b) — implementation + macOS validation plan

**Status: DONE — Phases A, B, and C all shipped and `fsck_hfs`-validated on
macOS.** The catalog / extents-overflow / attributes B-trees now grow their
backing fork when they run out of nodes via `HfsPlusFilesystem::grow_btree_fork`
(`src/fs/hfsplus.rs`), with a clean-`DiskFull` + retry-once at each of the three
insert methods. Phase A is contiguous tail growth; Phase B spills the 9th+ extent
into the extents-overflow B-tree (overflow-aware `write_fork_data_with_overflow`);
Phase C appends map nodes past the `(node_size-256)*8` ≈ 30,720-node header cap.

**Two foundational bugs surfaced and fixed along the way** (every HFS+ image
rusty-backup *built from scratch* tripped them, but only a real Mac caught them —
which is exactly why this work was deferred until a Mac was available):

1. **Invalid BTH length.** `write_blank_btree_header_node` laid the BTHeaderRec
   (record 0) out as 128 bytes (record 1 at offset 142); Apple requires it to be
   exactly 106 bytes (record 1 at 120, bitmap at 248). `fsck_hfs` rejected every
   blank with "Invalid BTH length". Fixing it to the canonical 120/248 layout
   also resolved the §5 header-vs-map-node capacity discrepancy — the bitmap now
   addresses exactly `(node_size-256)*8` nodes, matching `map_nodes_required`.
2. **Empty-tree representation + trailing block.** An empty B-tree was written as
   depth-1 with an empty root leaf (Apple uses depth-0 / no leaf), and the
   allocation bitmap didn't reserve the trailing block holding the alternate
   volume header. Both made `fsck_hfs` report "Invalid node structure" /
   "Invalid volume free block count". `btree_insert_full` now bootstraps the root
   leaf on the first insert into a depth-0 tree.

Validated on macOS via `rb-cli new --fs hfsplus` (Phase 0) + `rb-cli batch`
(real `create_file` puts past the 8-extent limit) and the in-repo grow tests'
emitted images (`RB_EMIT_GROW_IMAGE` / `_SPILL_` / `_MAPC_` / `_DEFRAG_`), each
`fsck_hfs -f -n` "appears to be OK" and mountable.

---

This is the one deferred step of
[`docs/hfsplus_btree_growth_plan.md`](hfsplus_btree_growth_plan.md) (§4b). P1–P5
(variable-length index keys, blank catalog sizing, extents/attributes splitting,
defrag re-verification, and density rotation) all shipped; this doc is the
follow-on so the catalog / extents-overflow / attributes B-trees can **grow their
backing fork when they run out of nodes**, instead of returning
`DiskFull("no free B-tree nodes")`.

The headline reason this is written as its own doc: **it is the part of the plan
that can't be validated against a real Apple implementation from inside the
repo.** macOS (through at least Sequoia) still reads, writes, mounts, and
`fsck_hfs`-checks **HFS+ / HFSX** volumes — only *classic* "HFS Standard" lost
its writable driver. So the acceptance bar for this work is not just our own
`hfsplus_fsck`: it is **`fsck_hfs -fy` clean and mountable on your Mac**. The
[macOS validation recipe](#7-macos-validation-recipe-the-real-acceptance-test) is
the point of the exercise; the Rust below exists to make that recipe pass.

---

## 1. What fails today

Every B-tree node allocation funnels through one function:

- `hfs_common::btree_alloc_node` (`src/fs/hfs_common.rs:1071`) finds a clear bit
  in the tree's **node-allocation bitmap** and returns the node index. When the
  fork has no free nodes it returns
  `Err(FilesystemError::DiskFull("no free B-tree nodes"))`
  (`hfs_common.rs:1092`).

That error propagates up through `btree_insert_full` →
`HfsPlusFilesystem::insert_catalog_record` / `insert_xattr_record` /
`insert_extents_overflow_record`. There is **no path that makes the fork
bigger** — neither for HFS+ *nor for classic HFS*. Both rely entirely on the fork
being pre-sized (P2 sizes the blank catalog from the volume; the defrag/clone
path over-sizes its target). So the gap §4b closes is:

> a B-tree whose fork was sized **elsewhere** (e.g. a small catalog on a
> macOS-formatted volume, or any volume whose record count outgrew its 0.5%
> estimate) and is then filled past that size through live `put`s.

`btree_alloc_node` only has the tree buffer; it can't allocate volume blocks or
touch the Volume Header. So **growth must happen one level up**, in the
`HfsPlusFilesystem` insert methods, which own `self.vh`, `self.bitmap`,
`self.reader`, and the three tree buffers.

## 2. Why it was deferred, and the bar for picking it up

Classic HFS ships with no grow path; we matched that. Doing §4b means writing
genuinely new on-disk-mutation machinery (allocate blocks, extend a fork, append
map nodes) that **must produce structures `fsck_hfs` accepts** — and the
node-allocation-bitmap / map-node area is exactly what historically produced the
`IndexSiblingLinkBroken` corruption (see
`PROMPT-hfs-catalog-incremental-put-packing.md`). That is why the deliverable is
"Mac says clean," not "we say clean."

Pick this up when a real workload needs to fill a foreign/under-sized catalog in
place. Until then, P2 sizing covers `rb-cli new` and the clone path.

## 3. Design overview

Add one grow primitive and invoke it with retry-once from the three insert
methods.

```text
insert_*_record(rec):
    loop:
        match btree_insert_full(buf, rec, kf, cmp):
            Ok(())                      => return Ok
            Err(DiskFull) if !grown_yet => grow_btree_fork(kind)?; grown_yet = true; continue
            Err(e)                      => return Err(e)
```

`grow_btree_fork(kind)` does, in order:

1. **Decide the growth amount.** Round up to the fork's clump (`ForkData.clump_size`,
   or fall back to `VH.data_clump_size` / a fixed e.g. 8-node minimum). Growing a
   clump at a time amortises the work and matches Apple's allocator.
2. **Allocate volume blocks** via the existing `allocate_extents(blocks)`
   (`hfsplus.rs:2294`) — it updates `self.bitmap`, `vh.free_blocks`, and
   `vh.next_allocation`, and already handles multi-run allocation. *(But see
   Phase A: we want a contiguous-tail allocation, not the largest-run-first one.)*
3. **Attach the new blocks to the fork** (`vh.catalog_file` / `vh.extents_file` /
   `vh.attributes_file`, each a `ForkData` with 8 inline `ExtentDescriptor`s):
   - merge into the fork's **last extent** if the new blocks are physically
     contiguous with it (the common, fragmentation-free case — extent count
     unchanged);
   - else drop them into the next free inline extent slot;
   - else (all 8 inline slots used) **spill to the extents-overflow B-tree**
     (Phase B).
   Bump `ForkData.total_blocks` and `ForkData.logical_size`.
4. **Grow the in-memory tree buffer.** Extend `self.catalog_data`
   (`self.extents_overflow_data` / `self.attributes_data`) by
   `new_nodes * node_size` zero bytes so later inserts can write into it. The
   buffer length must stay `== total_nodes * node_size`.
5. **Publish the new nodes to the tree.** Read the `BTreeHeader`, set
   `total_nodes += new_nodes` and `free_nodes += new_nodes`, write it back. The
   new node indices must be addressable + marked **free** in the node-allocation
   bitmap:
   - within the header node's bitmap-record capacity
     (`(node_size − 278) × 8` ≈ **30,544 nodes / 117 MiB at node_size 4096** — the
     same cap P2's blank sizing clamps to) the bits already exist and read as 0
     (free), so there is nothing more to do — this is **Phase A**;
   - past that cap, append **map nodes** to the bitmap chain — **Phase C**.
6. **Bump volume bookkeeping:** `vh.free_blocks` already decremented in step 2;
   increment `vh.write_count`. On the next `sync_metadata()` the fork extents,
   bitmap, tree buffer, and VH are all persisted (`write_catalog`,
   `write_allocation_bitmap`, the VH serialize).

`btree_alloc_node` is unchanged — once `total_nodes`/`free_nodes` and the bitmap
say there's room, it just works.

## 4. Phasing

Each phase is independently shippable and independently Mac-verifiable. **Phase A
alone is the high-value 90% case**; B and C are orthogonal extensions.

### Phase 0 — testability prerequisite (CLI HFS+ create)

Not part of grow itself, but needed to drive grow from `rb-cli` for the macOS
recipe (§7) and to honour CLAUDE.md's GUI/CLI-parity rule: add
`new --fs hfsplus [--case-sensitive] [--min-catalog BYTES]` wired to
`create_blank_hfsplus_sized`. `--min-catalog` (a deliberately *small* value) is
what makes the grow path easy to trigger — a few thousand `put`s instead of
hundreds of thousands. (The in-repo Phase-A unit test doesn't need this; it calls
`create_blank_hfsplus_sized` directly. Method 1 in §7 also works without it.)

### Phase A — contiguous tail growth, ≤ 8 extents, within the node-bitmap cap

The minimum viable grow, and the one that covers nearly every real volume.

- New allocator helper `allocate_contiguous_after(last_block, blocks) ->
  Option<ExtentDescriptor>` that tries to claim the run *immediately following*
  the fork's current last extent. If those blocks are free, the fork's last
  extent simply gets longer — **extent count stays the same, write path
  unchanged**. Fall back to a free inline slot (when < 8 used) via the existing
  `allocate_extents`.
- Stay under the **header-node bitmap cap** (≈30,544 nodes @ 4096). If a grow
  would cross it, return `DiskFull` for now (Phase C lifts this). A 117 MiB
  catalog is millions of records — fine for everything this tool targets.
- `write_catalog` (`hfsplus.rs:2258`) → `write_fork_data` already walks the 8
  inline extents, so **no write-path change** is needed.
- **Touchpoints:** `insert_catalog_record` / `insert_xattr_record` /
  `insert_extents_overflow_record` (wrap with retry); new `grow_btree_fork` +
  `allocate_contiguous_after`; `ForkData` update; `BTreeHeader` total/free bump.

Gate: a catalog that starts at N nodes and is filled to ≫ N records succeeds, our
`hfsplus_fsck` is clean, **and `fsck_hfs` on macOS is clean** (recipe §7).

### Phase B — overflow extents (fork needs a 9th extent)

When the fork is fragmented enough to exhaust all 8 inline `ExtentDescriptor`s
and contiguous tail growth isn't possible:

- Insert an extents-overflow record for the **special file** being grown —
  `fileID = kHFSCatalogFileID (4)` / `kHFSExtentsFileID (3)` /
  `kHFSAttributesFileID (8)`, `forkType = 0`, `startBlock = fork-relative block
  of the new extent` — via `insert_extents_overflow_record` (now splittable,
  P3). **Recursion guard:** growing the catalog may need an overflow record,
  which may need to grow the *extents-overflow* fork, which must therefore be
  grown **first / separately** — never re-enter the same fork's grow.
- **Write path must learn overflow.** `write_catalog` /
  `write_allocation_bitmap` currently write only the inline 8 extents. The read
  side already consults overflow (`read_fork_with_overflow`, `hfsplus.rs:613`);
  the write side needs the same so a > 8-extent fork is fully persisted. This is
  the bulk of Phase B.
- Prefer to **avoid** this case: Phase A's contiguous-tail strategy keeps most
  forks at 1–2 extents. Treat Phase B as the fragmented-volume fallback.

### Phase C — map nodes past the header-bitmap cap

When `total_nodes` would exceed `(node_size − 278) × 8`:

- The node-allocation bitmap spills from the header node (record 2) into
  dedicated **map nodes** (`BTNodeKind = 2`) chained off the header's `fLink`.
  Reuse the existing, defrag-tested helpers:
  `map_nodes_required` (`hfs_common.rs:961`), `init_map_node`
  (`hfs_common.rs:1137`), `map_node_bitmap_bytes`, `btree_bitmap_segments`,
  `btree_node_bitmap_range`. The defrag builder already materialises map nodes up
  front (`hfsplus_defrag.rs:~1401`); Phase C does it **incrementally** mid-tree.
- A new map node itself consumes a node slot: allocate it from the freshly grown
  space, mark it used, link it into the chain, then the nodes it covers become
  addressable.
- **Highest-risk step** — this is the classic `IndexSiblingLinkBroken` neighbourhood.
  Validate aggressively (a multi-hundred-MiB catalog) and lean on `fsck_hfs`.

## 5. Risks & gotchas

- **Write-path overflow (Phase B).** The single biggest trap: silently writing
  only the first 8 extents of a grown fork truncates the tree on disk while it
  looks fine in memory. Mirror `read_fork_with_overflow` on the write side and
  unit-test a > 8-extent round-trip before trusting it.
- **Map-node bitmap chain (Phase C).** Off-by-one in the header-vs-map capacity
  boundary corrupts node allocation. Note the two capacity formulas already in
  the tree differ: `write_blank_btree_header_node` lays the header bitmap record
  at bytes `270..node_size−8` (= `(node_size−278)×8` bits), while
  `map_nodes_required` assumes a `(node_size−256)×8`-bit header. Pick one and use
  it everywhere; P2 deliberately clamps the blank to the **smaller** (270-based)
  value, so grow code should too.
- **Journaling.** macOS formats HFS+ as **journaled (HFS+J)** by default. We
  refuse *dirty* journaled volumes (`prepare_for_edit`) and edit a *clean* one in
  place without writing through the journal, relying on sync-boundary
  consistency. Growth mutates VH + allocation bitmap + fork extents — all
  journaled metadata. After our edit the on-disk state must be self-consistent so
  macOS replays an empty journal and mounts cleanly. **Test the non-journaled
  case first** (our blanks are non-journaled), then a macOS-made journaled
  volume.
- **Atomicity / rollback.** `create_file` snapshots the whole FS for rollback on
  error; a grow that half-allocated must leave `bitmap` / `free_blocks` / fork /
  header consistent so the snapshot guard restores cleanly. `allocate_extents`
  already rolls back partial allocations — follow that discipline.
- **Clump alignment.** Grow by a clump, not a single node, or a per-`put`
  workload re-enters grow constantly and fragments the fork (defeating Phase A).
- **`next_allocation` / free-space accounting.** Keep `vh.free_blocks` and the
  bitmap in lock-step; `fsck_hfs` recomputes the bitmap from all fork extents and
  flags any mismatch (the "bitmap reports X but should be Y" error class).
- **Special-file extent records (Phase B).** Overflow extents for the catalog
  itself are keyed by the reserved CNIDs (3/4/6/8), not user CNIDs — easy to get
  wrong, and `fsck_hfs` checks them specifically.

## 6. In-repo tests (write these alongside the code)

These let you iterate without a Mac; §7 is the real cross-check.

- **Phase A unit:** build a volume whose catalog is deliberately tiny (e.g.
  `create_blank_hfsplus_sized(.., min_catalog_bytes = small)`), insert enough
  records to force ≥ 2 grows, then assert: every insert succeeded, `total_nodes`
  grew, `hfsplus_fsck` clean, every record still findable, and a reopen
  (`HfsPlusFilesystem::open`) re-reads the grown fork (proves the fork extents +
  header persisted).
- **Contiguity:** after Phase-A growth the catalog fork should still be 1–2
  extents (assert `vh.catalog_file` extent count stays low).
- **Phase B unit:** pre-fragment the volume bitmap so the fork is forced to a 9th
  extent; grow; reopen; read the whole catalog back; `hfsplus_fsck` clean. (Reuse
  the fragmentation trick from
  `test_hfsplus_fragmented_file_splits_extents_overflow_btree_real_path`.)
- **Phase C unit:** a large enough catalog to cross the map-node boundary (sizing
  the blank just under the cap, then growing past it); `hfsplus_fsck` clean;
  walk + count all records.
- Keep `cargo test --lib`, `cargo build --all-targets` (zero warnings), and
  `cargo clippy -- -D warnings` green (CONTRIBUTING hard rules).

## 7. macOS validation recipe (the real acceptance test)

Run on a Mac. **HFS+ / HFSX is still fully supported by macOS** (read, write,
mount, `diskutil`, `fsck_hfs`) — only *classic* "HFS Standard" became read-only
(High Sierra) and then unreadable. We are exercising HFS **Extended**, which is
fine through current macOS.

> ### CLI reality check — two gaps to close first
>
> - **rb-cli can't *create* an HFS+ volume.** `new --fs` accepts only
>   `{hfs, hfv, fat, efs, affs, ntfs}` (`src/cli/verbs/new.rs`); `hfs`/`hfv` are
>   *classic* HFS. HFS+ blanks only exist internally (`create_blank_hfsplus*`,
>   used by restore/clone/tests). So there is no `rb-cli new --fs hfsplus` to make
>   a small-catalog test volume.
> - **`rb-cli put` copies a *host file*, not stdin** (`PutArgs.host_file: PathBuf`,
>   or `--zero N` to zero-fill). `put IMG HOSTFILE DEST`. `mkdir`/`put` *do* edit
>   an existing HFS+ image (they route through `open_editable_filesystem`).
>
> **Phase 0 (prerequisite, do this with the §4b work):** expose HFS+ creation on
> the CLI — `new --fs hfsplus [--case-sensitive] [--min-catalog BYTES]` wired to
> `create_blank_hfsplus_sized`, per the GUI/CLI-parity rule in CLAUDE.md. The
> `--min-catalog` knob (a *small* value) is what lets you build a volume whose
> catalog is intentionally too small, so a few thousand `put`s force the grow path
> on demand instead of needing hundreds of thousands of files. Until Phase 0
> lands, use **method 1** (a test emits the image) for grow coverage.

### Method 1 — a Rust test emits a grown image; macOS judges it (works today)

The Phase-A/B/C unit tests (§6) build a small-catalog HFS+ volume entirely
in-process and grow it. Have the test write its final image to a file:

```rust
// at the end of the grow test, after our own hfsplus_fsck is clean:
fs.sync_metadata().unwrap();   // flush VH / bitmap / fork extents / tree buffer to the image
std::fs::write("/tmp/grown-hfsplus.img", fs.reader.get_ref()).unwrap();
```

Then on the Mac:

```sh
# raw single-volume HFS+ image (no partition map) -> attach without mounting
hdiutil attach -nomount -imagekey diskimage-class=CRawDiskImage /tmp/grown-hfsplus.img
#   -> prints e.g. /dev/disk7
fsck_hfs -f -n /dev/rdisk7        # -n = check, never modify; must say "appears to be OK"
fsck_hfs -d -f -n /dev/rdisk7     # verbose, if the first reports anything
hdiutil detach /dev/disk7

# mount read-only and walk every record
hdiutil attach -readonly /tmp/grown-hfsplus.img        # /Volumes/<name>
find "/Volumes/<name>" -type f | wc -l                 # must equal the test's record count
diskutil verifyVolume /dev/disk7
hdiutil detach /dev/disk7
```

If the test produces an APM/GPT-wrapped image instead of a bare volume,
`hdiutil attach -nomount` exposes the `Apple_HFS` slice as `/dev/disk7s2` — run
`fsck_hfs` against that slice.

### Method 2 — round-trip a *macOS-formatted* HFS+ volume (the foreign-catalog case)

This is the scenario §4b actually targets: a catalog built by Apple, then grown
by us. Needs Phase 0 (so `rb-cli` can edit it) and enough files to outgrow the
catalog macOS reserved.

```sh
# 1) macOS builds a journaled HFS+ volume and seeds it.
hdiutil create -size 400m -fs HFS+ -volname Grow -type UDIF macgrow.dmg
hdiutil attach macgrow.dmg                                 # /Volumes/Grow
mkdir -p /Volumes/Grow/seed
hdiutil detach /Volumes/Grow                               # leaves a CLEAN journal (we refuse dirty)

# 2) Convert to a raw, read/write image rb-cli can edit in place.
hdiutil convert macgrow.dmg -format UDRW -o macgrow.img
RB=./target/release/rb-cli
printf x > /tmp/x                                          # a tiny host file to copy in

# 3) Fill past the catalog macOS reserved. Use img@1 if it has a partition map
#    (rb-cli prints the partitions for `rb-cli inspect macgrow.img`); a bare HFS+
#    volume needs no @N. Make many small files across many dirs (worst case for
#    node usage). This is slow one-at-a-time; scripts can batch with `rb-cli batch`.
for d in $(seq 0 199); do $RB mkdir macgrow.img "/seed/d$d" -q; done
for i in $(seq 1 40000); do
  d=$(( i % 200 ))
  $RB put macgrow.img /tmp/x "/seed/d$d/f$i.txt" -q || { echo "FAILED at $i"; break; }
done
$RB fsck macgrow.img                                       # our own checker first

# 4) The verdict: macOS fsck_hfs on the grown volume.
hdiutil attach -nomount macgrow.img
fsck_hfs -f -n /dev/rdiskN          # MUST be clean — Apple-built catalog, grown by us
hdiutil attach -readonly macgrow.img && find /Volumes/Grow -type f | wc -l && hdiutil detach /dev/diskN
```

### What "pass" means

- `fsck_hfs -f -n` reports the volume **appears to be OK** — no
  `IndexSiblingLinkBroken`, no "Invalid node structure", no bitmap / free-block
  mismatch, no key-order errors — on a volume that required **≥ 1 grow** (confirm
  the catalog `total_blocks` is larger than the freshly-built size).
- The volume **mounts** and `find` enumerates every file written.
- **Phase C bonus:** push a catalog past ~117 MiB (a multi-GiB volume,
  ≳ several-hundred-thousand small files) to exercise map-node appending, and
  `fsck_hfs` that too.

Paste the exact `fsck_hfs` output into the PR — *"`fsck_hfs` clean after N grows,
catalog grew from X to Y nodes"* is the deliverable, not our own `hfsplus_fsck`.

## 8. Acceptance criteria

```text
1. A volume whose catalog/extents/attributes fork is too small for its record
   count grows in place through live puts; no spurious DiskFull while the VOLUME
   still has free blocks.
2. hfsplus_fsck clean AND `fsck_hfs -fy` clean on macOS, after >= 1 grow (Phase A),
   a > 8-extent fork (Phase B), and a past-the-cap catalog (Phase C).
3. The grown volume mounts on macOS and every record is readable; file/dir counts
   match.
4. Reopening the image in rusty-backup re-reads the grown fork correctly (extents,
   header counts, node bitmap) and further edits still succeed.
5. Classic HFS and the existing HFS+ paths are unaffected (P1–P5 tests stay green;
   blanks that never hit the grow path are byte-identical to today).
```

## 9. Code pointers

- `src/fs/hfs_common.rs` — `btree_alloc_node` (:1071, DiskFull at :1092);
  `BTreeHeader` total/free nodes; node-bitmap helpers `btree_node_bitmap_range`
  (:939), `map_node_bitmap_bytes` (:950), `map_nodes_required` (:961),
  `init_map_node` (:1137), `btree_bitmap_segments`, `btree_bitmap_set`/`_clear`.
- `src/fs/hfsplus.rs` — the three insert methods (`insert_catalog_record`,
  `insert_xattr_record`, `insert_extents_overflow_record`); `allocate_extents`
  (:2294), `free_blocks` (:2356), `write_catalog` (:2258),
  `write_allocation_bitmap` (:2269), `read_fork_with_overflow` (:613);
  `ForkData` (:92, 8 inline extents); VH `catalog_file`/`extents_file`/
  `attributes_file` + `free_blocks`/`next_allocation`/`write_count`;
  `build_blank_hfsplus_front` (node-bitmap cap `(node_size − 278) × 8`),
  `create_blank_hfsplus_sized` (use for the tiny-catalog test fixtures).
- `src/fs/hfsplus_defrag.rs` — the *upfront* map-node + sized-fork builder
  (`map_nodes_required` call site, ~:1401) — the reference for Phase C, just
  applied incrementally.
- `src/fs/hfsplus_fsck.rs` — extend coverage so our own fsck validates grown
  forks (node-bitmap accounting, map-node chain, overflow extents) before the
  macOS cross-check.
```
