# HFS Edit Mode Stabilization Plan

Findings from analyzing HFS fsck repair code and applying lessons to the
editing (create/delete/modify) paths in `src/fs/hfs.rs`.

HFS+ is out of scope for now — this focuses on classic HFS only.

---

## Corruption Risks Identified

### 1. No Atomicity — Partial Writes on Error (HIGH)

`create_file()` performs multiple catalog mutations in sequence:

```
write_data_to_blocks(data)     → allocates blocks, writes data
insert_catalog_record(file)    → may split B-tree nodes
insert_catalog_record(thread)  → may split again
update_parent_valence(+1)
mdb.file_count += 1
do_sync_metadata()             → writes catalog + bitmap + MDB
```

If the thread record insert fails (e.g. B-tree out of free nodes), the file
record is already in the in-memory catalog and blocks are marked used in the
in-memory bitmap, but `sync_metadata` never runs. The in-memory state is now
inconsistent. If the user then does another operation that succeeds and calls
`sync_metadata`, the partial state gets flushed — a file record with no
thread, which is exactly the `MissingFileThread` issue fsck detects.

Same risk in `delete_entry()` — removing the thread record can fail after the
file record was already removed.

**Affected functions:**
- `create_file()` (hfs.rs ~1302)
- `create_directory()` (hfs.rs ~1426)
- `delete_entry()` (hfs.rs ~1497)
- `write_resource_fork()` (hfs.rs ~1601)

### 2. B-tree Node Bitmap Desync After Split (MEDIUM)

From fsck we learned that `repair_node_bitmap` was needed because B-tree
splits can leave the node bitmap out of sync. The editing code's
`insert_catalog_record` calls `btree_split_leaf` → `btree_alloc_node` which
sets the bitmap bit. But if the subsequent `btree_insert_record` into the
target half fails (node still full after split due to a large record), the
newly allocated node is marked in the bitmap but contains only half the
records — and the split_key was never inserted into the parent index.

Fsck's `rebuild_index_nodes` can recover this, but editing doesn't attempt
recovery.

### 3. Index Node Stale After Split (MEDIUM)

`insert_catalog_record` builds a `_parent_chain` via `btree_find_insert_leaf`.
After a split, it uses this chain to find the parent index node. But fsck
showed that index nodes themselves can become stale — `rebuild_index_nodes`
does a full bottom-up rebuild rather than incremental fixes.

After many insertions causing cascading splits, the index tree can have:
- Separator keys that don't match actual first keys of child leaf nodes
- Missing separators for newly-split nodes

This is the same class of bug that drove fsck repair to do full index rebuilds.

### 4. `split_index_node` Doesn't Update Sibling Links (MEDIUM)

`split_index_node` (hfs_common.rs ~749) doesn't update the `fLink`/`bLink`
of neighboring index nodes after splitting. Compare with `btree_split_leaf`
(~623-635) which correctly updates old_next's bLink. Index nodes don't use
sibling links for traversal the same way, but fsck's `IndexSiblingLinkBroken`
check flags this.

### 5. `update_parent_valence` Linear Scan (LOW)

`update_parent_valence` (hfs.rs ~696) walks the entire leaf chain to find the
parent directory record. If the leaf chain is broken (e.g. after a failed
split), it silently returns `Ok(())` without updating the valence. Fsck's
`ValenceMismatch` catches this drift.

### 6. `remove_catalog_record` Doesn't Clean Index (LOW)

`remove_catalog_record` (hfs.rs ~835) handles empty leaf nodes by unlinking
them but stale separator keys remain in parent index nodes. Fsck showed this
can cause lookups to traverse to freed/zeroed nodes. `btree_find_insert_leaf`
could follow a stale index pointer and return a wrong leaf for the next insert.

---

## Stabilization Plan

### Priority 1: Snapshot/Rollback on Error

Wrap every edit operation in a catalog + bitmap + MDB snapshot. On any error,
restore the snapshot before returning so that no partial state can ever be
flushed by a subsequent successful operation.

```rust
let catalog_snapshot = self.catalog_data.clone();
let bitmap_snapshot = self.bitmap.clone();
let mdb_snapshot = self.mdb.clone();

// ... perform all mutations ...

if let Err(e) = result {
    self.catalog_data = catalog_snapshot;
    self.bitmap = bitmap_snapshot;
    self.mdb = mdb_snapshot;
    return Err(e);
}
self.do_sync_metadata()?;
```

This is cheap (catalog is already fully in RAM) and completely eliminates
partial-write corruption. Applies to: `create_file`, `create_directory`,
`delete_entry`, `set_type_creator`, `write_resource_fork`.

Note: block data already written to disk by `write_data_to_blocks` before a
later failure becomes orphaned on disk but invisible (bitmap rolled back to
"free"). This is harmless — the blocks will be reused on the next allocation.

### Priority 2: Post-Edit Consistency Check

After `do_sync_metadata()`, run a lightweight sanity pass:

1. Walk the leaf chain — verify fLink/bLink consistency
2. Count leaf records — verify header.leaf_records matches
3. Verify node bitmap marks all leaf-chain nodes as allocated

This is what fsck's `fix_leaf_record_count` + `repair_node_bitmap` do and
they're fast (single pass over the leaf chain). If a mismatch is detected,
log a warning. This gives early visibility into B-tree drift without the
cost of a full fsck.

### Priority 3: Rebuild Index Nodes After Any Split

Rather than trusting incremental index maintenance during cascading splits,
call `rebuild_index_nodes` (from hfs_fsck.rs) after any `insert_catalog_record`
that triggered a split. The catalog B-tree is already fully in memory so this
is a fast in-memory operation.

This trades a small amount of CPU time for the certainty that index nodes
always reflect the actual leaf chain — the same guarantee fsck repair provides.

The function already exists in `hfs_fsck.rs` and operates on `&mut [u8]`
catalog data, so it can be called directly from the edit path.

### Priority 4: Fix `split_index_node` Sibling Links

Add fLink/bLink maintenance to `split_index_node` in `hfs_common.rs`,
matching the pattern already used by `btree_split_leaf`. This is a small
targeted fix:

```rust
// After splitting, update old_next's bLink to point to new_idx
let old_next = BigEndian::read_u32(&catalog_data[old_offset..old_offset + 4]);
BigEndian::write_u32(&mut catalog_data[old_offset..old_offset + 4], new_idx);
BigEndian::write_u32(&mut catalog_data[new_offset + 4..new_offset + 8], node_idx);
BigEndian::write_u32(&mut catalog_data[new_offset..new_offset + 4], old_next);
if old_next != 0 {
    let next_offset = old_next as usize * node_size;
    BigEndian::write_u32(&mut catalog_data[next_offset + 4..next_offset + 8], new_idx);
}
```

If Priority 3 is implemented (full index rebuild after splits), this becomes
less critical since the rebuild will fix links anyway. Still worth doing for
correctness.

---

## Testing Plan

- **Stress test**: Create enough files to trigger multiple leaf splits and at
  least one index split. Verify with fsck after each batch.
- **Error injection**: Mock a full B-tree (no free nodes) mid-operation to
  verify rollback restores clean state.
- **Round-trip**: Create files, delete some, create more. Run fsck check after
  each operation — zero issues expected.
- **Delete with thread**: Delete files that have thread records, verify no
  orphaned threads remain.
- **Fragmented catalog**: Start with a nearly-full catalog B-tree, trigger
  splits during create, verify index nodes are correct.
