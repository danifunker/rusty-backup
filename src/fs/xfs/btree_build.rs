//! Bottom-up free-space btree builder — the algorithmic core of the R2
//! rebuild (`docs/xfs_edit_and_repair.md`).
//!
//! Given the complete set of free extents for one allocation group (derived
//! by inverting the R1 ownership map), this produces a fresh, fully-formed
//! `bnobt` (free-space-by-block) or `cntbt` (free-space-by-count) btree the
//! same way offline `xfs_repair` phase 5 does: pack all records into leaf
//! blocks, then build parent levels until a single root remains, writing
//! complete btree blocks at once rather than incrementally inserting.
//!
//! This module is **v4-only** (16-byte short-form block header, no CRC/UUID)
//! and pure: it takes block numbers to place the new btree blocks at and
//! returns the block bytes, with no disk I/O. Wiring it into a live AGF
//! rewrite (allocating those blocks, updating agf_roots/levels/freeblks) is a
//! separate, oracle-gated step.
//!
//! Porting reference: `xfs_repair` @ v3.1.11 `repair/phase5.c`
//! (`build_freespace_tree`).

use byteorder::{BigEndian, ByteOrder};

use super::types::{NULLAGBLOCK, XFS_BTREE_SBLOCK_LEN};

/// alloc-btree on-disk widths (v4): records and keys are 8 bytes
/// (startblock u32 + blockcount u32 for records; startblock + blockcount for
/// keys), pointers are 4 bytes.
const REC_SIZE: usize = 8;
const KEY_SIZE: usize = 8;
const PTR_SIZE: usize = 4;

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
/// and the tree depth (number of levels = AGF level value).
pub struct BuiltBtree {
    pub blocks: Vec<BuiltBlock>,
    pub root_agbno: u32,
    pub levels: u32,
}

/// Max records in a leaf and max keys/ptrs in an internal node for the given
/// block size (v4 short-form header).
fn capacities(blocksize: usize) -> (usize, usize) {
    let avail = blocksize - XFS_BTREE_SBLOCK_LEN;
    let max_leaf = avail / REC_SIZE;
    let max_intern = avail / (KEY_SIZE + PTR_SIZE);
    (max_leaf, max_intern)
}

/// Number of btree blocks needed to hold `nrecs` records, summed across all
/// levels. Used to know how many block numbers to reserve before building.
pub fn blocks_needed(nrecs: usize, blocksize: usize) -> usize {
    let (max_leaf, max_intern) = capacities(blocksize);
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

/// Build a v4 alloc btree from `extents` (already sorted by the caller in the
/// btree's key order — by startblock for bnobt, by (blockcount, startblock)
/// for cntbt). `magic` selects the tree (`XFS_ABTB_MAGIC` / `XFS_ABTC_MAGIC`).
/// `avail` supplies the AG-relative block numbers to place the new blocks at;
/// it must contain at least `blocks_needed(extents.len(), blocksize)` entries.
pub fn build_alloc_btree(
    extents: &[FreeExtent],
    magic: u32,
    blocksize: usize,
    seqno: u32,
    avail: &[u32],
) -> BuiltBtree {
    let (max_leaf, max_intern) = capacities(blocksize);
    let mut blocks: Vec<BuiltBlock> = Vec::new();
    let mut next = 0usize; // index into `avail`

    // --- Level 0: leaves ---
    // Each leaf carries up to max_leaf records and is chained left<->right.
    let leaf_chunks: Vec<&[FreeExtent]> = if extents.is_empty() {
        vec![&[]]
    } else {
        extents.chunks(max_leaf).collect()
    };
    let leaf_agbnos: Vec<u32> = leaf_chunks.iter().map(|_| take(avail, &mut next)).collect();

    // Each entry: (agbno, first_key) propagated to the parent level.
    let mut child_level: Vec<(u32, [u8; KEY_SIZE])> = Vec::with_capacity(leaf_chunks.len());

    for (i, chunk) in leaf_chunks.iter().enumerate() {
        let agbno = leaf_agbnos[i];
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
        write_header(&mut buf, magic, 0, chunk.len() as u16, left, right, seqno);
        for (j, ext) in chunk.iter().enumerate() {
            let off = XFS_BTREE_SBLOCK_LEN + j * REC_SIZE;
            BigEndian::write_u32(&mut buf[off..off + 4], ext.startblock);
            BigEndian::write_u32(&mut buf[off + 4..off + 8], ext.blockcount);
        }
        // The leaf's first record is its key in the parent.
        let mut key = [0u8; KEY_SIZE];
        if let Some(first) = chunk.first() {
            BigEndian::write_u32(&mut key[0..4], first.startblock);
            BigEndian::write_u32(&mut key[4..8], first.blockcount);
        }
        child_level.push((agbno, key));
        blocks.push(BuiltBlock { agbno, bytes: buf });
    }

    let mut levels = 1u32;

    // --- Internal levels until a single root remains ---
    while child_level.len() > 1 {
        let mut parent_level: Vec<(u32, [u8; KEY_SIZE])> = Vec::new();
        for chunk in child_level.chunks(max_intern) {
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
            let ptr_base = XFS_BTREE_SBLOCK_LEN + max_intern * KEY_SIZE;
            for (j, (child_agbno, child_key)) in chunk.iter().enumerate() {
                let koff = XFS_BTREE_SBLOCK_LEN + j * KEY_SIZE;
                buf[koff..koff + KEY_SIZE].copy_from_slice(child_key);
                let poff = ptr_base + j * PTR_SIZE;
                BigEndian::write_u32(&mut buf[poff..poff + PTR_SIZE], *child_agbno);
            }
            // This node's key in its parent is its first child's key.
            let key = chunk[0].1;
            parent_level.push((agbno, key));
            blocks.push(BuiltBlock { agbno, bytes: buf });
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
        let (_, max_intern) = capacities(blocksize);
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
                    let off = XFS_BTREE_SBLOCK_LEN + j * REC_SIZE;
                    out.push(FreeExtent {
                        startblock: BigEndian::read_u32(&block[off..off + 4]),
                        blockcount: BigEndian::read_u32(&block[off + 4..off + 8]),
                    });
                }
            } else {
                let ptr_base = XFS_BTREE_SBLOCK_LEN + max_intern * KEY_SIZE;
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
