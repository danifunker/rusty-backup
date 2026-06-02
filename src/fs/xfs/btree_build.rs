//! Bottom-up short-form btree builder — the algorithmic core shared by the R2
//! free-space rebuild and R3's multi-level inobt rebuild
//! (`docs/xfs_edit_and_repair.md`).
//!
//! Given a complete, pre-sorted record set for one allocation group, this
//! produces a fresh, fully-formed v4 short-form btree the same way offline
//! `xfs_repair` phase 5 does: pack all records into leaf blocks, then build
//! parent levels until a single root remains, writing complete btree blocks at
//! once rather than incrementally inserting. The builder is record-agnostic —
//! every short-form btree we rebuild (`bnobt`/`cntbt` 8-byte recs, `inobt`
//! 16-byte recs) uses the convention **key = the first `key_size` bytes of the
//! record**, which holds for all of them (alloc key = startblock+blockcount;
//! inobt key = startino).
//!
//! This module is **v4-only** (16-byte short-form block header, no CRC/UUID)
//! and pure: it takes block numbers to place the new btree blocks at and
//! returns the block bytes, with no disk I/O. Wiring it into a live AGF/AGI
//! rewrite (allocating those blocks, updating roots/levels/counters) is a
//! separate, oracle-gated step.
//!
//! Porting reference: `xfs_repair` @ v3.1.11 `repair/phase5.c`
//! (`build_freespace_tree` / `build_ino_tree`).

use byteorder::{BigEndian, ByteOrder};

use super::types::{NULLAGBLOCK, XFS_BTREE_SBLOCK_LEN};

/// Short-form btree pointers are always 4 bytes (AG-relative block numbers).
const PTR_SIZE: usize = 4;

/// alloc-btree on-disk widths (v4): records and keys are 8 bytes (startblock
/// u32 + blockcount u32).
const ALLOC_REC_SIZE: usize = 8;
const ALLOC_KEY_SIZE: usize = 8;

/// A free extent in one AG: `(startblock, blockcount)`, AG-relative.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FreeExtent {
    pub startblock: u32,
    pub blockcount: u32,
}

/// One built btree block: where to place it (AG-relative block number) and
/// its on-disk bytes (length = blocksize).
pub struct BuiltBlock {
    pub agbno: u32,
    pub bytes: Vec<u8>,
}

/// Result of building a btree: the blocks to write, the root block number,
/// and the tree depth (number of levels = AGF/AGI level value).
pub struct BuiltBtree {
    pub blocks: Vec<BuiltBlock>,
    pub root_agbno: u32,
    pub levels: u32,
}

/// Max records in a leaf and max keys/ptrs in an internal node for the given
/// block size and record/key widths (v4 short-form header).
fn capacities(blocksize: usize, rec_size: usize, key_size: usize) -> (usize, usize) {
    let avail = blocksize - XFS_BTREE_SBLOCK_LEN;
    let max_leaf = avail / rec_size;
    let max_intern = avail / (key_size + PTR_SIZE);
    (max_leaf, max_intern)
}

/// Number of btree blocks needed to hold `nrecs` records of the given widths,
/// summed across all levels. Used to know how many block numbers to reserve.
pub fn blocks_needed_for(
    nrecs: usize,
    blocksize: usize,
    rec_size: usize,
    key_size: usize,
) -> usize {
    let (max_leaf, max_intern) = capacities(blocksize, rec_size, key_size);
    if nrecs == 0 {
        return 1; // an empty root leaf
    }
    let mut nblocks = nrecs.div_ceil(max_leaf);
    let mut level_blocks = nblocks;
    while level_blocks > 1 {
        level_blocks = level_blocks.div_ceil(max_intern);
        nblocks += level_blocks;
    }
    nblocks
}

/// Blocks needed for a free-space (alloc) btree of `nrecs` records.
pub fn blocks_needed(nrecs: usize, blocksize: usize) -> usize {
    blocks_needed_for(nrecs, blocksize, ALLOC_REC_SIZE, ALLOC_KEY_SIZE)
}

/// Build a v4 short-form btree from flat, pre-sorted record bytes.
///
/// `records` is `nrecs * rec_size` bytes, already sorted in the btree's key
/// order. The key for every record (and the key a child propagates to its
/// parent) is the record's first `key_size` bytes. `magic` selects the tree;
/// `avail` supplies the AG-relative block numbers to place new blocks at and
/// must hold at least `blocks_needed_for(nrecs, ..)` entries. Block layout:
/// leaf = `[recs]`; internal node = `[keys[max_intern]][ptrs[max_intern]]`.
pub fn build_sblock_btree(
    records: &[u8],
    rec_size: usize,
    key_size: usize,
    magic: u32,
    blocksize: usize,
    seqno: u32,
    avail: &[u32],
) -> BuiltBtree {
    let (max_leaf, max_intern) = capacities(blocksize, rec_size, key_size);
    let nrecs = records.len() / rec_size;
    let mut blocks: Vec<BuiltBlock> = Vec::new();
    let mut next = 0usize; // index into `avail`

    // --- Level 0: leaves ---
    // Spread records *evenly* across the minimum number of leaves so that every
    // leaf (the only exception being a lone leaf that is also the root) holds at
    // least `maxrecs/2` records. XFS requires this B+tree minimum-fill invariant
    // and `xfs_repair` flags any underfilled non-root block ("dubious ... block
    // header"); a naive fill-then-remainder split leaves a starved last block.
    let leaf_sizes = balanced_sizes(nrecs, max_leaf);
    let leaf_agbnos: Vec<u32> = (0..leaf_sizes.len())
        .map(|_| take(avail, &mut next))
        .collect();

    // Each entry: (agbno, first_key) propagated to the parent level.
    let mut child_level: Vec<(u32, Vec<u8>)> = Vec::with_capacity(leaf_sizes.len());

    let mut lo = 0usize;
    for (i, &chunk_recs) in leaf_sizes.iter().enumerate() {
        let agbno = leaf_agbnos[i];
        let hi = lo + chunk_recs;
        let chunk = &records[lo * rec_size..hi * rec_size];
        let left = if i == 0 {
            NULLAGBLOCK
        } else {
            leaf_agbnos[i - 1]
        };
        let right = if i + 1 < leaf_agbnos.len() {
            leaf_agbnos[i + 1]
        } else {
            NULLAGBLOCK
        };
        let mut buf = vec![0u8; blocksize];
        write_header(&mut buf, magic, 0, chunk_recs as u16, left, right, seqno);
        buf[XFS_BTREE_SBLOCK_LEN..XFS_BTREE_SBLOCK_LEN + chunk.len()].copy_from_slice(chunk);
        // The leaf's first record's key is its key in the parent.
        let mut key = vec![0u8; key_size];
        if chunk_recs > 0 {
            key.copy_from_slice(&chunk[0..key_size]);
        }
        child_level.push((agbno, key));
        blocks.push(BuiltBlock { agbno, bytes: buf });
        lo = hi;
    }

    let mut levels = 1u32;

    // --- Internal levels until a single root remains ---
    while child_level.len() > 1 {
        let mut parent_level: Vec<(u32, Vec<u8>)> = Vec::new();
        let node_sizes = balanced_sizes(child_level.len(), max_intern);
        let mut child_lo = 0usize;
        for &node_recs in &node_sizes {
            let chunk = &child_level[child_lo..child_lo + node_recs];
            let agbno = take(avail, &mut next);
            let mut buf = vec![0u8; blocksize];
            write_header(
                &mut buf,
                magic,
                levels as u16,
                chunk.len() as u16,
                NULLAGBLOCK,
                NULLAGBLOCK,
                seqno,
            );
            // Layout: keys[maxrecs] then ptrs[maxrecs].
            let ptr_base = XFS_BTREE_SBLOCK_LEN + max_intern * key_size;
            for (j, (child_agbno, child_key)) in chunk.iter().enumerate() {
                let koff = XFS_BTREE_SBLOCK_LEN + j * key_size;
                buf[koff..koff + key_size].copy_from_slice(child_key);
                let poff = ptr_base + j * PTR_SIZE;
                BigEndian::write_u32(&mut buf[poff..poff + PTR_SIZE], *child_agbno);
            }
            // This node's key in its parent is its first child's key.
            let key = chunk[0].1.clone();
            parent_level.push((agbno, key));
            blocks.push(BuiltBlock { agbno, bytes: buf });
            child_lo += node_recs;
        }
        child_level = parent_level;
        levels += 1;
    }

    let root_agbno = child_level[0].0;
    BuiltBtree {
        blocks,
        root_agbno,
        levels,
    }
}

/// Build a v4 alloc btree from `extents` (already sorted by the caller in the
/// btree's key order — by startblock for bnobt, by (blockcount, startblock)
/// for cntbt). `magic` selects the tree (`XFS_ABTB_MAGIC` / `XFS_ABTC_MAGIC`).
/// Thin wrapper over `build_sblock_btree` that packs each extent into the
/// 8-byte alloc record `[startblock(4)][blockcount(4)]`.
pub fn build_alloc_btree(
    extents: &[FreeExtent],
    magic: u32,
    blocksize: usize,
    seqno: u32,
    avail: &[u32],
) -> BuiltBtree {
    let mut records = vec![0u8; extents.len() * ALLOC_REC_SIZE];
    for (i, ext) in extents.iter().enumerate() {
        let off = i * ALLOC_REC_SIZE;
        BigEndian::write_u32(&mut records[off..off + 4], ext.startblock);
        BigEndian::write_u32(&mut records[off + 4..off + 8], ext.blockcount);
    }
    build_sblock_btree(
        &records,
        ALLOC_REC_SIZE,
        ALLOC_KEY_SIZE,
        magic,
        blocksize,
        seqno,
        avail,
    )
}

/// Write a v4 short-form btree block header.
fn write_header(
    buf: &mut [u8],
    magic: u32,
    level: u16,
    numrecs: u16,
    leftsib: u32,
    rightsib: u32,
    _seqno: u32,
) {
    BigEndian::write_u32(&mut buf[0..4], magic);
    BigEndian::write_u16(&mut buf[4..6], level);
    BigEndian::write_u16(&mut buf[6..8], numrecs);
    BigEndian::write_u32(&mut buf[8..12], leftsib);
    BigEndian::write_u32(&mut buf[12..16], rightsib);
}

fn take(avail: &[u32], next: &mut usize) -> u32 {
    let v = avail[*next];
    *next += 1;
    v
}

/// Split `total` items into the fewest groups that each hold at most `max`,
/// distributed as evenly as possible (sizes differ by at most one). With
/// `parts = ceil(total/max)`, each group then holds between `floor(total/parts)`
/// and `ceil(total/parts)` items, and `floor(total/parts) >= max/2` for any
/// `parts >= 2` — exactly the B+tree minimum-fill invariant XFS requires of
/// every non-root block. `total == 0` yields a single empty group (an empty
/// root leaf).
fn balanced_sizes(total: usize, max: usize) -> Vec<usize> {
    if total == 0 {
        return vec![0];
    }
    let parts = total.div_ceil(max);
    let base = total / parts;
    let rem = total % parts;
    (0..parts).map(|i| base + usize::from(i < rem)).collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Walk a built btree back out of a flat AG buffer and collect its leaf
    /// records, mirroring the verifier's reader. Used to round-trip-test the
    /// builder without a full filesystem.
    fn read_back(
        blocks: &[BuiltBlock],
        root: u32,
        blocksize: usize,
        magic: u32,
    ) -> Vec<FreeExtent> {
        let find = |agbno: u32| -> &[u8] {
            &blocks
                .iter()
                .find(|b| b.agbno == agbno)
                .expect("block present")
                .bytes
        };
        let (_, max_intern) = capacities(blocksize, ALLOC_REC_SIZE, ALLOC_KEY_SIZE);
        let mut out = Vec::new();
        let mut stack = vec![root];
        while let Some(agbno) = stack.pop() {
            if agbno == NULLAGBLOCK {
                continue;
            }
            let block = find(agbno);
            assert_eq!(BigEndian::read_u32(&block[0..4]), magic, "magic");
            let level = BigEndian::read_u16(&block[4..6]);
            let numrecs = BigEndian::read_u16(&block[6..8]) as usize;
            if level == 0 {
                for j in 0..numrecs {
                    let off = XFS_BTREE_SBLOCK_LEN + j * ALLOC_REC_SIZE;
                    out.push(FreeExtent {
                        startblock: BigEndian::read_u32(&block[off..off + 4]),
                        blockcount: BigEndian::read_u32(&block[off + 4..off + 8]),
                    });
                }
            } else {
                let ptr_base = XFS_BTREE_SBLOCK_LEN + max_intern * ALLOC_KEY_SIZE;
                // Push children in reverse so we pop them left-to-right.
                for j in (0..numrecs).rev() {
                    let poff = ptr_base + j * PTR_SIZE;
                    stack.push(BigEndian::read_u32(&block[poff..poff + PTR_SIZE]));
                }
            }
        }
        out
    }

    fn make_extents(n: usize) -> Vec<FreeExtent> {
        // startblocks 10, 20, 30, ... (sorted, non-overlapping) with varied
        // lengths so the records aren't uniform.
        (0..n)
            .map(|i| FreeExtent {
                startblock: 10 + i as u32 * 10,
                blockcount: 1 + (i as u32 % 7),
            })
            .collect()
    }

    #[test]
    fn single_leaf_round_trips() {
        let magic = super::super::types::XFS_ABTB_MAGIC;
        let bs = 4096;
        let exts = make_extents(5);
        let avail: Vec<u32> = (1..=blocks_needed(exts.len(), bs) as u32).collect();
        let tree = build_alloc_btree(&exts, magic, bs, 0, &avail);
        assert_eq!(tree.levels, 1);
        assert_eq!(tree.blocks.len(), 1);
        assert_eq!(read_back(&tree.blocks, tree.root_agbno, bs, magic), exts);
    }

    #[test]
    fn multi_level_round_trips() {
        // 4096-byte block: max_leaf = (4096-16)/8 = 510. Force >2 levels by
        // using enough records that leaves overflow and the node level too.
        let magic = super::super::types::XFS_ABTB_MAGIC;
        let bs = 512; // small block: max_leaf=(512-16)/8=62, max_intern=(496)/12=41
        let exts = make_extents(5000); // 5000/62 = 81 leaves -> 81/41=2 nodes -> 1 root
        let avail: Vec<u32> = (1..=blocks_needed(exts.len(), bs) as u32 + 5).collect();
        let tree = build_alloc_btree(&exts, magic, bs, 0, &avail);
        assert!(tree.levels >= 3, "expected >=3 levels, got {}", tree.levels);
        assert_eq!(read_back(&tree.blocks, tree.root_agbno, bs, magic), exts);
    }

    #[test]
    fn inobt_shaped_records_round_trip() {
        // 16-byte records, 4-byte keys (inobt shape). Small block forces
        // multiple levels: max_leaf=(512-16)/16=31, max_intern=(496)/8=62.
        let magic = super::super::types::XFS_IBT_MAGIC;
        let bs = 512;
        let rec_size = 16;
        let key_size = 4;
        let n = 2000usize; // 2000/31 = 65 leaves -> 65/62 = 2 nodes -> 1 root
        let mut records = vec![0u8; n * rec_size];
        for i in 0..n {
            let off = i * rec_size;
            // key = startino (ascending), then arbitrary freecount/free payload.
            BigEndian::write_u32(&mut records[off..off + 4], (i as u32) * 64);
            BigEndian::write_u32(&mut records[off + 4..off + 8], i as u32 % 64);
            BigEndian::write_u64(&mut records[off + 8..off + 16], 0xDEAD_0000 | i as u64);
        }
        let need = blocks_needed_for(n, bs, rec_size, key_size);
        let avail: Vec<u32> = (1..=need as u32).collect();
        let tree = build_sblock_btree(&records, rec_size, key_size, magic, bs, 0, &avail);
        assert!(tree.levels >= 3, "expected >=3 levels, got {}", tree.levels);
        assert_eq!(tree.blocks.len(), need);

        // Walk it back and confirm every leaf record survives in key order.
        let find = |agbno: u32| -> &[u8] {
            &tree
                .blocks
                .iter()
                .find(|b| b.agbno == agbno)
                .expect("block present")
                .bytes
        };
        let (_, max_intern) = capacities(bs, rec_size, key_size);
        let mut out: Vec<u8> = Vec::new();
        // Find the leftmost leaf by descending child slot 0, then walk the
        // level-0 sibling chain so records come out in key order.
        let mut node = tree.root_agbno;
        loop {
            let block = find(node);
            let level = BigEndian::read_u16(&block[4..6]);
            if level == 0 {
                break;
            }
            let ptr_base = XFS_BTREE_SBLOCK_LEN + max_intern * key_size;
            node = BigEndian::read_u32(&block[ptr_base..ptr_base + 4]);
        }
        let mut leaf = node;
        loop {
            let block = find(leaf);
            let numrecs = BigEndian::read_u16(&block[6..8]) as usize;
            out.extend_from_slice(
                &block[XFS_BTREE_SBLOCK_LEN..XFS_BTREE_SBLOCK_LEN + numrecs * rec_size],
            );
            let right = BigEndian::read_u32(&block[12..16]);
            if right == NULLAGBLOCK {
                break;
            }
            leaf = right;
        }
        assert_eq!(out, records, "leaf records survive round-trip in order");
    }

    #[test]
    fn balanced_sizes_respect_minimum_fill() {
        // Every non-singleton group must be >= max/2 (the XFS minimum-fill
        // invariant) and <= max, and the sizes must sum back to total.
        for max in [3usize, 8, 31, 62, 510] {
            for total in 0..=(max * 5 + 1) {
                let sizes = balanced_sizes(total, max);
                assert_eq!(sizes.iter().sum::<usize>(), total, "sum total={total}");
                if total == 0 {
                    assert_eq!(sizes, vec![0]);
                    continue;
                }
                assert_eq!(sizes.len(), total.div_ceil(max), "part count total={total}");
                for &s in &sizes {
                    assert!(s <= max, "overfull: total={total} max={max} s={s}");
                    if sizes.len() > 1 {
                        assert!(s >= max / 2, "underfull: total={total} max={max} s={s}");
                    }
                }
            }
        }
    }

    #[test]
    fn blocks_needed_matches_built() {
        let bs = 512;
        for n in [0usize, 1, 62, 63, 1000, 5000] {
            let exts = make_extents(n);
            let need = blocks_needed(n, bs);
            let avail: Vec<u32> = (1..=need as u32).collect();
            let tree = build_alloc_btree(&exts, super::super::types::XFS_ABTB_MAGIC, bs, 0, &avail);
            assert_eq!(tree.blocks.len(), need, "n={n}");
        }
    }
}
