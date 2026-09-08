//! Incremental inserts and removes for leaf and node-form dir2 directories,
//! after `xfs_dir2_leaf.c`, `xfs_dir2_node.c` and `xfs_da_btree.c`.
//!
//! A directory's blocks live in three address spaces of the inode's file:
//! data blocks from offset 0, the hash index from `XFS_DIR2_LEAF_OFFSET`
//! (the root block sits there: a leaf1 in leaf form, a leafN or a da node in
//! node form), and freeindex blocks from `XFS_DIR2_FREE_OFFSET`. An
//! operation runs against a [`Dir2Store`] through [`Dir2Op`], which caches
//! every block it touches and hands the store one transaction to apply, so
//! nothing reaches the disk until the whole change is known.
//!
//! Insert: descend the da btree by hash to the leaf; refuse a duplicate;
//! take room from the first data block whose `bests` entry admits the entry
//! (appending a data block, and a freeindex block when its table is full,
//! when none does); place the entry from that block's `bestfree[0]`; add
//! `(hash, dataptr)` to the leaf, reusing a stale slot when there is one.
//! A full leafN splits in two, the new sibling is keyed into the parent, a
//! full parent splits the same way, and a full root is first copied down a
//! level so the split has a parent (`xfs_da3_root_split`). A full leaf1 is
//! left to the rebuild in `edit.rs`, which converts to node form.
//!
//! Remove: find the entry, return its bytes to the data block's free space
//! (coalescing neighbours, `xfs_dir2_data_make_free`), mark the leaf slot
//! stale, free a data block that emptied and drop its `bests` entry, remove
//! a leaf with no live entries from its parent, and collapse a root left
//! with one child (`xfs_da3_root_join`). Half-empty siblings are not merged.
//! The caller is told when a smaller form would fit so it can rebuild.
//!
//! Block-level rules that `xfs_repair` checks and this module keeps:
//! `bestfree` is the three largest unused records, longest first, ties in
//! offset order (a full rescan after every change, as `xfs_dir2_data_freescan`
//! would); every unused record carries its offset as a trailing tag; a leaf's
//! hashes are sorted with stale entries keeping theirs and `stale` counting
//! them; a node entry's hash is its child's last hash; siblings at every
//! level are linked by dablk; a freeindex block's `nused` counts its
//! non-null bests and `nvalid` reaches the last data block it covers.

use std::collections::{BTreeMap, BTreeSet};

use byteorder::{BigEndian, ByteOrder};

use super::{
    blocks_per_dir_block, build_da_node_block, build_free_block, build_leaf_data_block,
    build_leafn_block, dir2_da_hdr_len, dir2_data_entsize, dir2_data_hdr_len, dir2_free_hdr_len,
    dir2_free_offset_fb, dir2_leaf_offset_fb, dir_hashname, NULLDATAOFF, XFS_DA3_NODE_MAGIC,
    XFS_DA_NODE_MAGIC, XFS_DIR2_LEAF1_MAGIC, XFS_DIR2_LEAFN_MAGIC, XFS_DIR3_LEAF1_MAGIC,
    XFS_DIR3_LEAFN_MAGIC,
};
use crate::fs::filesystem::FilesystemError;

/// A stale leaf entry's address (`XFS_DIR2_NULL_DATAPTR`): 0, which no
/// entry can have since the data header occupies it.
const NULL_DATAPTR: u32 = 0;

/// The directory geometry every block computation needs.
#[derive(Clone, Copy, Debug)]
pub(super) struct Geo {
    pub dirblksize: usize,
    pub is_v5: bool,
    pub has_ftype: bool,
    /// fsblocks per dir block; dablk numbers advance by this much.
    pub bpd: u64,
    pub leaf_off: u64,
    pub free_off: u64,
}

impl Geo {
    pub fn of(sb: &crate::fs::xfs::sb::XfsSuperblock) -> Geo {
        Geo {
            dirblksize: sb.dirblksize() as usize,
            is_v5: sb.is_v5(),
            has_ftype: sb.has_ftype(),
            bpd: u64::from(blocks_per_dir_block(sb)),
            leaf_off: dir2_leaf_offset_fb(sb.blocksize),
            free_off: dir2_free_offset_fb(sb.blocksize),
        }
    }

    /// A geometry for in-memory tests: any dir block size, v5 headers, ftype on.
    #[cfg(test)]
    pub fn synthetic(dirblksize: usize) -> Geo {
        Geo {
            dirblksize,
            is_v5: true,
            has_ftype: true,
            bpd: 1,
            leaf_off: 1 << 20,
            free_off: 2 << 20,
        }
    }

    fn data_hdr(&self) -> usize {
        dir2_data_hdr_len(self.is_v5)
    }
    fn da_hdr(&self) -> usize {
        dir2_da_hdr_len(self.is_v5)
    }
    fn free_hdr(&self) -> usize {
        dir2_free_hdr_len(self.is_v5)
    }
    /// Offset of `count` in a leaf or node header (`stale`/`level` follow it).
    fn count_off(&self) -> usize {
        if self.is_v5 {
            56
        } else {
            12
        }
    }
    /// Offset of `bestfree[0]` in a data header, and of `firstdb` in a free one.
    fn fields_off(&self) -> usize {
        if self.is_v5 {
            48
        } else {
            4
        }
    }
    fn da_max_ents(&self) -> usize {
        (self.dirblksize - self.da_hdr()) / 8
    }
    fn free_max_bests(&self) -> usize {
        (self.dirblksize - self.free_hdr()) / 2
    }
    fn dataptr(&self, db: u64, off: usize) -> u32 {
        ((db * self.dirblksize as u64 + off as u64) / 8) as u32
    }
    fn dataptr_db(&self, ptr: u32) -> u64 {
        (u64::from(ptr) * 8) / self.dirblksize as u64
    }
    fn dataptr_off(&self, ptr: u32) -> usize {
        ((u64::from(ptr) * 8) % self.dirblksize as u64) as usize
    }
    fn data_dablk(&self, db: u64) -> u64 {
        db * self.bpd
    }
    fn free_dablk(&self, fdb: u64) -> u64 {
        self.free_off + fdb * self.bpd
    }
    pub fn is_leaf_space(&self, dablk: u64) -> bool {
        dablk >= self.leaf_off && dablk < self.free_off
    }
}

// ---------- data blocks ----------

/// Every record of a data block as `(offset, length, unused)`.
fn data_records(geo: &Geo, block: &[u8]) -> Result<Vec<(usize, usize, bool)>, FilesystemError> {
    let mut out = Vec::new();
    let mut pos = geo.data_hdr();
    let end = block.len();
    while pos + 8 <= end {
        let (len, unused) = if BigEndian::read_u16(&block[pos..pos + 2]) == 0xFFFF {
            (BigEndian::read_u16(&block[pos + 2..pos + 4]) as usize, true)
        } else {
            (
                dir2_data_entsize(block[pos + 8] as usize, geo.has_ftype),
                false,
            )
        };
        if len < 8 || pos + len > end {
            return Err(FilesystemError::Parse(format!(
                "directory data record at byte {pos} has length {len}"
            )));
        }
        out.push((pos, len, unused));
        pos += len;
    }
    if pos != end {
        return Err(FilesystemError::Parse(format!(
            "directory data records end at byte {pos} of {end}"
        )));
    }
    Ok(out)
}

/// Write an unused record over `[off, off + len)`: free tag, length, tail tag.
fn data_write_unused(block: &mut [u8], off: usize, len: usize) {
    BigEndian::write_u16(&mut block[off..off + 2], 0xFFFF);
    BigEndian::write_u16(&mut block[off + 2..off + 4], len as u16);
    BigEndian::write_u16(&mut block[off + len - 2..off + len], off as u16);
}

/// Recompute `bestfree`: the three longest unused records, ties by offset.
fn data_freescan(geo: &Geo, block: &mut [u8]) -> Result<(), FilesystemError> {
    let mut frees: Vec<(usize, usize)> = data_records(geo, block)?
        .into_iter()
        .filter(|r| r.2)
        .map(|r| (r.0, r.1))
        .collect();
    frees.sort_by_key(|f| std::cmp::Reverse(f.1));
    let bf = geo.fields_off();
    for i in 0..3 {
        let (off, len) = frees.get(i).copied().unwrap_or((0, 0));
        BigEndian::write_u16(&mut block[bf + i * 4..bf + i * 4 + 2], off as u16);
        BigEndian::write_u16(&mut block[bf + i * 4 + 2..bf + i * 4 + 4], len as u16);
    }
    Ok(())
}

fn data_best(geo: &Geo, block: &[u8]) -> u16 {
    let bf = geo.fields_off();
    BigEndian::read_u16(&block[bf + 2..bf + 4])
}

/// Carve `need` bytes off the front of `bestfree[0]` (as `xfs_dir2_data_use_free`
/// does for an add); the caller writes the entry there and rescans.
fn data_take(geo: &Geo, block: &mut [u8], need: usize) -> Option<usize> {
    let bf = geo.fields_off();
    let off = BigEndian::read_u16(&block[bf..bf + 2]) as usize;
    let len = BigEndian::read_u16(&block[bf + 2..bf + 4]) as usize;
    if len < need || off == 0 {
        return None;
    }
    if len > need {
        data_write_unused(block, off + need, len - need);
    }
    Some(off)
}

fn data_put_entry(geo: &Geo, block: &mut [u8], off: usize, ino: u64, name: &[u8], ft: u8) {
    let ent = dir2_data_entsize(name.len(), geo.has_ftype);
    block[off..off + ent].fill(0);
    BigEndian::write_u64(&mut block[off..off + 8], ino);
    block[off + 8] = name.len() as u8;
    block[off + 9..off + 9 + name.len()].copy_from_slice(name);
    if geo.has_ftype {
        block[off + 9 + name.len()] = ft;
    }
    BigEndian::write_u16(&mut block[off + ent - 2..off + ent], off as u16);
}

/// `(ino, name, ftype, entsize)` of the entry at `off`.
fn data_get_entry(
    geo: &Geo,
    block: &[u8],
    off: usize,
) -> Result<(u64, Vec<u8>, u8, usize), FilesystemError> {
    if off + 9 > block.len() || BigEndian::read_u16(&block[off..off + 2]) == 0xFFFF {
        return Err(FilesystemError::Parse(format!(
            "directory leaf points at byte {off}, which holds no entry"
        )));
    }
    let namelen = block[off + 8] as usize;
    let ent = dir2_data_entsize(namelen, geo.has_ftype);
    if off + ent > block.len() {
        return Err(FilesystemError::Parse(format!(
            "directory entry at byte {off} runs past the block"
        )));
    }
    let name = block[off + 9..off + 9 + namelen].to_vec();
    let ft = if geo.has_ftype {
        block[off + 9 + namelen]
    } else {
        0
    };
    Ok((BigEndian::read_u64(&block[off..off + 8]), name, ft, ent))
}

/// Return `[off, off + len)` to free space, coalescing with unused
/// neighbours; true when the whole block is free afterwards.
fn data_make_free(
    geo: &Geo,
    block: &mut [u8],
    off: usize,
    len: usize,
) -> Result<bool, FilesystemError> {
    let recs = data_records(geo, block)?;
    let mut start = off;
    let mut end = off + len;
    for &(o, l, unused) in &recs {
        if unused && o + l == off {
            start = o;
        }
        if unused && o == off + len {
            end = o + l;
        }
    }
    block[start..end].fill(0);
    data_write_unused(block, start, end - start);
    data_freescan(geo, block)?;
    Ok(end - start == block.len() - geo.data_hdr())
}

// ---------- leaf and node blocks ----------

/// The shared prefix of leaf1, leafN and da node blocks.
#[derive(Clone, Copy, Debug)]
struct DaHdr {
    forw: u32,
    back: u32,
    magic: u16,
    count: u16,
    /// `stale` in a leaf, `level` in a node.
    aux: u16,
}

fn da_read_hdr(geo: &Geo, block: &[u8]) -> DaHdr {
    let c = geo.count_off();
    DaHdr {
        forw: BigEndian::read_u32(&block[0..4]),
        back: BigEndian::read_u32(&block[4..8]),
        magic: BigEndian::read_u16(&block[8..10]),
        count: BigEndian::read_u16(&block[c..c + 2]),
        aux: BigEndian::read_u16(&block[c + 2..c + 4]),
    }
}

fn da_write_hdr(geo: &Geo, block: &mut [u8], h: &DaHdr) {
    let c = geo.count_off();
    BigEndian::write_u32(&mut block[0..4], h.forw);
    BigEndian::write_u32(&mut block[4..8], h.back);
    BigEndian::write_u16(&mut block[8..10], h.magic);
    BigEndian::write_u16(&mut block[c..c + 2], h.count);
    BigEndian::write_u16(&mut block[c + 2..c + 4], h.aux);
}

/// Entry `i` of a leaf `(hash, dataptr)` or node `(hash, before)`.
fn da_ent(geo: &Geo, block: &[u8], i: usize) -> (u32, u32) {
    let off = geo.da_hdr() + i * 8;
    (
        BigEndian::read_u32(&block[off..off + 4]),
        BigEndian::read_u32(&block[off + 4..off + 8]),
    )
}

fn da_put_ent(geo: &Geo, block: &mut [u8], i: usize, hash: u32, v: u32) {
    let off = geo.da_hdr() + i * 8;
    BigEndian::write_u32(&mut block[off..off + 4], hash);
    BigEndian::write_u32(&mut block[off + 4..off + 8], v);
}

fn da_ents(geo: &Geo, block: &[u8]) -> Vec<(u32, u32)> {
    let n = da_read_hdr(geo, block).count as usize;
    (0..n).map(|i| da_ent(geo, block, i)).collect()
}

/// Replace every entry, zeroing the slots past the new count.
fn da_set_ents(geo: &Geo, block: &mut [u8], ents: &[(u32, u32)], keep_end: usize) {
    let hdr = geo.da_hdr();
    block[hdr..keep_end].fill(0);
    for (i, &(h, v)) in ents.iter().enumerate() {
        da_put_ent(geo, block, i, h, v);
    }
    let mut h = da_read_hdr(geo, block);
    h.count = ents.len() as u16;
    da_write_hdr(geo, block, &h);
}

fn is_node_magic(m: u16) -> bool {
    m == XFS_DA_NODE_MAGIC || m == XFS_DA3_NODE_MAGIC
}
fn is_leaf1_magic(m: u16) -> bool {
    m == XFS_DIR2_LEAF1_MAGIC || m == XFS_DIR3_LEAF1_MAGIC
}
fn is_leafn_magic(m: u16) -> bool {
    m == XFS_DIR2_LEAFN_MAGIC || m == XFS_DIR3_LEAFN_MAGIC
}

/// First entry whose hash is at least `hash`, or `count`.
fn da_lower_bound(geo: &Geo, block: &[u8], hash: u32) -> usize {
    let n = da_read_hdr(geo, block).count as usize;
    let (mut lo, mut hi) = (0usize, n);
    while lo < hi {
        let mid = (lo + hi) / 2;
        if da_ent(geo, block, mid).0 < hash {
            lo = mid + 1;
        } else {
            hi = mid;
        }
    }
    lo
}

fn da_last_hash(geo: &Geo, block: &[u8]) -> u32 {
    let n = da_read_hdr(geo, block).count as usize;
    if n == 0 {
        0
    } else {
        da_ent(geo, block, n - 1).0
    }
}

// leaf1 tail: bests[bestcount] then the u32 bestcount.
fn leaf1_bestcount(block: &[u8]) -> usize {
    let n = block.len();
    BigEndian::read_u32(&block[n - 4..n]) as usize
}
fn leaf1_bests_off(block: &[u8]) -> usize {
    block.len() - 4 - 2 * leaf1_bestcount(block)
}
fn leaf1_best(block: &[u8], db: usize) -> u16 {
    let o = leaf1_bests_off(block) + db * 2;
    BigEndian::read_u16(&block[o..o + 2])
}
fn leaf1_put_best(block: &mut [u8], db: usize, v: u16) {
    let o = leaf1_bests_off(block) + db * 2;
    BigEndian::write_u16(&mut block[o..o + 2], v);
}

/// Resize the bests table, moving it; new slots read `NULLDATAOFF`.
fn leaf1_set_bestcount(block: &mut [u8], n: usize) {
    let old: Vec<u16> = (0..leaf1_bestcount(block))
        .map(|i| leaf1_best(block, i))
        .collect();
    let len = block.len();
    let old_off = leaf1_bests_off(block);
    let new_off = len - 4 - 2 * n;
    block[old_off.min(new_off)..len - 4].fill(0);
    BigEndian::write_u32(&mut block[len - 4..len], n as u32);
    for i in 0..n {
        let v = old.get(i).copied().unwrap_or(NULLDATAOFF);
        BigEndian::write_u16(&mut block[new_off + i * 2..new_off + i * 2 + 2], v);
    }
}

/// Entry slots a leaf can hold: up to the bests table for leaf1.
fn leaf_capacity(geo: &Geo, block: &[u8]) -> usize {
    let end = if is_leaf1_magic(da_read_hdr(geo, block).magic) {
        leaf1_bests_off(block)
    } else {
        block.len()
    };
    end.saturating_sub(geo.da_hdr()) / 8
}

/// Add `(hash, addr)` keeping hash order, reusing the nearest stale slot as
/// `xfs_dir2_leafn_add` does; false when the leaf has no room.
fn leaf_insert(geo: &Geo, block: &mut [u8], hash: u32, addr: u32) -> bool {
    let mut h = da_read_hdr(geo, block);
    let count = h.count as usize;
    let idx = da_lower_bound(geo, block, hash);
    if h.aux > 0 {
        let lowstale = (0..idx)
            .rev()
            .find(|&i| da_ent(geo, block, i).1 == NULL_DATAPTR);
        let highstale = (idx..count).find(|&i| da_ent(geo, block, i).1 == NULL_DATAPTR);
        let use_low = match (lowstale, highstale) {
            (Some(l), Some(hi)) => idx - l <= hi - idx,
            (Some(_), None) => true,
            _ => false,
        };
        if use_low {
            let l = lowstale.unwrap_or(0);
            for i in l..idx - 1 {
                let (eh, ev) = da_ent(geo, block, i + 1);
                da_put_ent(geo, block, i, eh, ev);
            }
            da_put_ent(geo, block, idx - 1, hash, addr);
        } else {
            let hi = highstale.unwrap_or(idx);
            for i in (idx + 1..=hi).rev() {
                let (eh, ev) = da_ent(geo, block, i - 1);
                da_put_ent(geo, block, i, eh, ev);
            }
            da_put_ent(geo, block, idx, hash, addr);
        }
        h.aux -= 1;
        da_write_hdr(geo, block, &h);
        return true;
    }
    if count >= leaf_capacity(geo, block) {
        return false;
    }
    for i in (idx + 1..=count).rev() {
        let (eh, ev) = da_ent(geo, block, i - 1);
        da_put_ent(geo, block, i, eh, ev);
    }
    da_put_ent(geo, block, idx, hash, addr);
    h.count += 1;
    da_write_hdr(geo, block, &h);
    true
}

fn leaf_mark_stale(geo: &Geo, block: &mut [u8], idx: usize) {
    let (hash, _) = da_ent(geo, block, idx);
    da_put_ent(geo, block, idx, hash, NULL_DATAPTR);
    let mut h = da_read_hdr(geo, block);
    h.aux += 1;
    da_write_hdr(geo, block, &h);
}

/// Drop the stale entries (`xfs_dir3_leaf_compact`).
fn leaf_compact(geo: &Geo, block: &mut [u8]) {
    let live: Vec<(u32, u32)> = da_ents(geo, block)
        .into_iter()
        .filter(|e| e.1 != NULL_DATAPTR)
        .collect();
    let end = geo.da_hdr() + leaf_capacity(geo, block) * 8;
    da_set_ents(geo, block, &live, end);
    let mut h = da_read_hdr(geo, block);
    h.aux = 0;
    da_write_hdr(geo, block, &h);
}

fn leaf_live(geo: &Geo, block: &[u8]) -> usize {
    let h = da_read_hdr(geo, block);
    (h.count - h.aux) as usize
}

/// Node entry leading to `hash`: the first whose key is at least it, else the last.
fn node_child_for(geo: &Geo, block: &[u8], hash: u32) -> usize {
    let n = da_read_hdr(geo, block).count as usize;
    da_lower_bound(geo, block, hash).min(n.saturating_sub(1))
}

fn node_find_child(geo: &Geo, block: &[u8], before: u64) -> Option<usize> {
    da_ents(geo, block)
        .iter()
        .position(|&(_, b)| u64::from(b) == before)
}

fn node_insert(geo: &Geo, block: &mut [u8], idx: usize, hash: u32, before: u32) -> bool {
    let mut h = da_read_hdr(geo, block);
    let count = h.count as usize;
    if count >= geo.da_max_ents() {
        return false;
    }
    for i in (idx + 1..=count).rev() {
        let (eh, ev) = da_ent(geo, block, i - 1);
        da_put_ent(geo, block, i, eh, ev);
    }
    da_put_ent(geo, block, idx, hash, before);
    h.count += 1;
    da_write_hdr(geo, block, &h);
    true
}

fn node_remove(geo: &Geo, block: &mut [u8], idx: usize) {
    let mut ents = da_ents(geo, block);
    ents.remove(idx);
    let end = geo.da_hdr() + geo.da_max_ents() * 8;
    da_set_ents(geo, block, &ents, end);
}

// ---------- freeindex blocks ----------

fn free_read_hdr(geo: &Geo, block: &[u8]) -> (u32, u32, u32) {
    let f = geo.fields_off();
    (
        BigEndian::read_u32(&block[f..f + 4]),
        BigEndian::read_u32(&block[f + 4..f + 8]),
        BigEndian::read_u32(&block[f + 8..f + 12]),
    )
}

fn free_write_hdr(geo: &Geo, block: &mut [u8], firstdb: u32, nvalid: u32, nused: u32) {
    let f = geo.fields_off();
    BigEndian::write_u32(&mut block[f..f + 4], firstdb);
    BigEndian::write_u32(&mut block[f + 4..f + 8], nvalid);
    BigEndian::write_u32(&mut block[f + 8..f + 12], nused);
}

fn free_best(geo: &Geo, block: &[u8], i: usize) -> u16 {
    let o = geo.free_hdr() + i * 2;
    BigEndian::read_u16(&block[o..o + 2])
}

/// Set best `i`, keeping `nvalid` and `nused` in step.
fn free_put_best(geo: &Geo, block: &mut [u8], i: usize, v: u16) {
    let (firstdb, nvalid, nused) = free_read_hdr(geo, block);
    let old = if i < nvalid as usize {
        free_best(geo, block, i)
    } else {
        NULLDATAOFF
    };
    for j in nvalid as usize..i {
        let o = geo.free_hdr() + j * 2;
        BigEndian::write_u16(&mut block[o..o + 2], NULLDATAOFF);
    }
    let o = geo.free_hdr() + i * 2;
    BigEndian::write_u16(&mut block[o..o + 2], v);
    let mut nvalid = nvalid.max(i as u32 + 1);
    while nvalid > 0 && free_best(geo, block, nvalid as usize - 1) == NULLDATAOFF {
        nvalid -= 1;
    }
    let nused = nused + u32::from(old == NULLDATAOFF && v != NULLDATAOFF)
        - u32::from(old != NULLDATAOFF && v == NULLDATAOFF);
    free_write_hdr(geo, block, firstdb, nvalid, nused);
}

// ---------- the store and the operation ----------

/// One directory change: blocks to rewrite (created ones included), the
/// dablks to map, the dablks to unmap, and the new `di_size`.
pub(super) struct Dir2Txn {
    pub dirty: BTreeMap<u64, Vec<u8>>,
    pub created: BTreeSet<u64>,
    pub freed: BTreeSet<u64>,
    pub size: u64,
}

/// What a [`Dir2Op`] runs against: block reads by dablk, the map, and the
/// commit of one transaction.
pub(super) trait Dir2Store {
    fn geo(&self) -> Geo;
    fn read_block(&mut self, dablk: u64) -> Result<Option<Vec<u8>>, FilesystemError>;
    fn is_mapped(&self, dablk: u64) -> bool;
    fn size(&self) -> u64;
    /// Apply the change; `Ok(false)` when it was backed out untouched
    /// because the inode cannot hold the resulting extent list.
    fn apply(&mut self, txn: Dir2Txn) -> Result<bool, FilesystemError>;
}

/// How an operation ended.
#[derive(Debug, PartialEq, Eq)]
pub(super) enum Outcome {
    /// Applied (after `commit`). For a remove, `shrink` says a smaller
    /// directory form would now fit, so the caller may rebuild.
    Done { shrink: bool },
    /// Nothing changed; the caller must rebuild the directory instead.
    Fallback,
}

pub(super) struct Dir2Op<'a, S: Dir2Store> {
    store: &'a mut S,
    geo: Geo,
    cache: BTreeMap<u64, Vec<u8>>,
    dirty: BTreeSet<u64>,
    created: BTreeSet<u64>,
    freed: BTreeSet<u64>,
    size: u64,
}

impl<'a, S: Dir2Store> Dir2Op<'a, S> {
    pub fn new(store: &'a mut S) -> Self {
        let geo = store.geo();
        let size = store.size();
        Dir2Op {
            store,
            geo,
            cache: BTreeMap::new(),
            dirty: BTreeSet::new(),
            created: BTreeSet::new(),
            freed: BTreeSet::new(),
            size,
        }
    }

    fn block(&mut self, dablk: u64) -> Result<Vec<u8>, FilesystemError> {
        if self.freed.contains(&dablk) {
            return Err(FilesystemError::Parse(format!(
                "directory block {dablk} used after being freed"
            )));
        }
        if let Some(b) = self.cache.get(&dablk) {
            return Ok(b.clone());
        }
        let b = self.store.read_block(dablk)?.ok_or_else(|| {
            FilesystemError::Parse(format!("directory block {dablk} is not mapped"))
        })?;
        self.cache.insert(dablk, b.clone());
        Ok(b)
    }

    fn put(&mut self, dablk: u64, block: Vec<u8>) {
        self.cache.insert(dablk, block);
        self.dirty.insert(dablk);
    }

    fn create(&mut self, dablk: u64, block: Vec<u8>) {
        self.freed.remove(&dablk);
        self.created.insert(dablk);
        self.put(dablk, block);
    }

    fn free(&mut self, dablk: u64) {
        self.cache.remove(&dablk);
        self.dirty.remove(&dablk);
        if !self.created.remove(&dablk) {
            self.freed.insert(dablk);
        }
    }

    fn is_mapped(&self, dablk: u64) -> bool {
        !self.freed.contains(&dablk)
            && (self.cache.contains_key(&dablk) || self.store.is_mapped(dablk))
    }

    /// First unmapped dir block at or after `lo` (below `hi`).
    fn first_hole(&self, lo: u64, hi: u64) -> Result<u64, FilesystemError> {
        let mut d = lo;
        while d < hi && self.is_mapped(d) {
            d += self.geo.bpd;
        }
        if d >= hi {
            return Err(FilesystemError::DiskFull(
                "directory address space exhausted".into(),
            ));
        }
        Ok(d)
    }

    /// Hand the change to the store; false when it was backed out.
    pub fn commit(self) -> Result<bool, FilesystemError> {
        let mut dirty = BTreeMap::new();
        for d in &self.dirty {
            if let Some(b) = self.cache.get(d) {
                dirty.insert(*d, b.clone());
            }
        }
        self.store.apply(Dir2Txn {
            dirty,
            created: self.created,
            freed: self.freed,
            size: self.size,
        })
    }

    fn magic(&mut self, dablk: u64) -> Result<u16, FilesystemError> {
        let b = self.block(dablk)?;
        Ok(BigEndian::read_u16(&b[8..10]))
    }

    fn last_hash(&mut self, dablk: u64) -> Result<u32, FilesystemError> {
        let b = self.block(dablk)?;
        Ok(da_last_hash(&self.geo, &b))
    }

    /// Descend from the root to the leaf that would hold `hash`: the node
    /// dablks passed (root first) and the leaf.
    fn lookup_path(&mut self, hash: u32) -> Result<(Vec<u64>, u64), FilesystemError> {
        let mut path = Vec::new();
        let mut dablk = self.geo.leaf_off;
        loop {
            let b = self.block(dablk)?;
            let h = da_read_hdr(&self.geo, &b);
            if is_node_magic(h.magic) {
                if h.count == 0 || path.len() > 8 {
                    return Err(FilesystemError::Parse(
                        "directory da btree node is empty or too deep".into(),
                    ));
                }
                path.push(dablk);
                let i = node_child_for(&self.geo, &b, hash);
                dablk = u64::from(da_ent(&self.geo, &b, i).1);
            } else if is_leaf1_magic(h.magic) || is_leafn_magic(h.magic) {
                return Ok((path, dablk));
            } else {
                return Err(FilesystemError::Parse(format!(
                    "directory block {dablk} has magic {:#06x}, not a leaf or node",
                    h.magic
                )));
            }
        }
    }

    /// The leaf, slot and dataptr of `name`, searching from `leaf` across
    /// siblings that continue the same hash.
    fn find_entry(
        &mut self,
        leaf: u64,
        hash: u32,
        name: &[u8],
    ) -> Result<Option<(u64, usize, u32)>, FilesystemError> {
        let geo = self.geo;
        let mut dablk = leaf;
        loop {
            let b = self.block(dablk)?;
            let h = da_read_hdr(&geo, &b);
            let mut i = da_lower_bound(&geo, &b, hash);
            while i < h.count as usize {
                let (eh, addr) = da_ent(&geo, &b, i);
                if eh != hash {
                    return Ok(None);
                }
                if addr != NULL_DATAPTR {
                    let data = self.block(geo.data_dablk(geo.dataptr_db(addr)))?;
                    let (_, n, _, _) = data_get_entry(&geo, &data, geo.dataptr_off(addr))?;
                    if n == name {
                        return Ok(Some((dablk, i, addr)));
                    }
                }
                i += 1;
            }
            if h.forw == 0 {
                return Ok(None);
            }
            dablk = u64::from(h.forw);
        }
    }

    /// Whether `name` is present (a cheap duplicate check for preflight).
    pub fn contains(&mut self, name: &[u8]) -> Result<bool, FilesystemError> {
        let hash = dir_hashname(name);
        let (_, leaf) = self.lookup_path(hash)?;
        Ok(self.find_entry(leaf, hash, name)?.is_some())
    }

    /// Insert `name -> ino`; `Fallback` when a leaf1 index is full.
    pub fn insert(&mut self, name: &[u8], ino: u64, ft: u8) -> Result<Outcome, FilesystemError> {
        let geo = self.geo;
        let hash = dir_hashname(name);
        let need = dir2_data_entsize(name.len(), geo.has_ftype);
        let is_leaf1 = is_leaf1_magic(self.magic(geo.leaf_off)?);
        let (path, leaf) = self.lookup_path(hash)?;
        if self.find_entry(leaf, hash, name)?.is_some() {
            return Err(FilesystemError::AlreadyExists(
                String::from_utf8_lossy(name).into_owned(),
            ));
        }
        let Some(db) = self.pick_data_block(is_leaf1, need, ino)? else {
            return Ok(Outcome::Fallback);
        };
        let mut data = self.block(geo.data_dablk(db))?;
        let Some(off) = data_take(&geo, &mut data, need) else {
            return Err(FilesystemError::Parse(format!(
                "directory data block {db} promised {need} bytes it does not have"
            )));
        };
        data_put_entry(&geo, &mut data, off, ino, name, ft);
        data_freescan(&geo, &mut data)?;
        let best = data_best(&geo, &data);
        self.put(geo.data_dablk(db), data);
        self.set_best(is_leaf1, db, best)?;
        self.size = self.size.max((db + 1) * geo.dirblksize as u64);
        let addr = geo.dataptr(db, off);
        let mut lb = self.block(leaf)?;
        if leaf_insert(&geo, &mut lb, hash, addr) {
            self.put(leaf, lb);
            self.fix_key(&path, leaf)?;
            return Ok(Outcome::Done { shrink: false });
        }
        if is_leaf1 {
            return Ok(Outcome::Fallback);
        }
        self.split_leaf_insert(path, leaf, hash, addr)?;
        Ok(Outcome::Done { shrink: false })
    }

    /// The data block for a `need`-byte entry, created (with a freeindex
    /// block) when none has room; `None` when leaf1's bests table cannot grow.
    fn pick_data_block(
        &mut self,
        is_leaf1: bool,
        need: usize,
        self_ino: u64,
    ) -> Result<Option<u64>, FilesystemError> {
        let geo = self.geo;
        let db = if is_leaf1 {
            let root = self.block(geo.leaf_off)?;
            let n = leaf1_bestcount(&root);
            let bests: Vec<u16> = (0..n).map(|i| leaf1_best(&root, i)).collect();
            match bests
                .iter()
                .position(|&b| b != NULLDATAOFF && usize::from(b) >= need)
            {
                Some(i) => return Ok(Some(i as u64)),
                None => {
                    let db = bests.iter().position(|&b| b == NULLDATAOFF).unwrap_or(n);
                    if db == n {
                        let mut root = root;
                        leaf1_set_bestcount(&mut root, n + 1);
                        if da_read_hdr(&geo, &root).count as usize > leaf_capacity(&geo, &root) {
                            return Ok(None);
                        }
                        self.put(geo.leaf_off, root);
                    }
                    db as u64
                }
            }
        } else {
            let mut fdb = 0u64;
            let mut found = None;
            while self.is_mapped(geo.free_dablk(fdb)) {
                let fb = self.block(geo.free_dablk(fdb))?;
                let (firstdb, nvalid, _) = free_read_hdr(&geo, &fb);
                for i in 0..nvalid as usize {
                    let b = free_best(&geo, &fb, i);
                    if b != NULLDATAOFF && usize::from(b) >= need {
                        found = Some(u64::from(firstdb) + i as u64);
                        break;
                    }
                }
                if found.is_some() {
                    break;
                }
                fdb += 1;
            }
            match found {
                Some(db) => return Ok(Some(db)),
                None => {
                    let dablk = self.first_hole(0, geo.leaf_off)?;
                    let db = dablk / geo.bpd;
                    let fdb = db / geo.free_max_bests() as u64;
                    if !self.is_mapped(geo.free_dablk(fdb)) {
                        let mut fb = build_free_block(&[], geo.dirblksize, geo.is_v5)?;
                        free_write_hdr(
                            &geo,
                            &mut fb,
                            (fdb * geo.free_max_bests() as u64) as u32,
                            0,
                            0,
                        );
                        self.create(geo.free_dablk(fdb), fb);
                    }
                    db
                }
            }
        };
        let built = build_leaf_data_block(
            &[],
            self_ino,
            0,
            db,
            geo.dirblksize,
            geo.has_ftype,
            false,
            geo.is_v5,
        )?;
        self.create(geo.data_dablk(db), built.bytes);
        Ok(Some(db))
    }

    /// Record data block `db`'s largest free length in the leaf1 tail or
    /// its freeindex block.
    fn set_best(&mut self, is_leaf1: bool, db: u64, best: u16) -> Result<(), FilesystemError> {
        let geo = self.geo;
        if is_leaf1 {
            let mut root = self.block(geo.leaf_off)?;
            leaf1_put_best(&mut root, db as usize, best);
            self.put(geo.leaf_off, root);
        } else {
            let fdb = db / geo.free_max_bests() as u64;
            let mut fb = self.block(geo.free_dablk(fdb))?;
            free_put_best(
                &geo,
                &mut fb,
                (db % geo.free_max_bests() as u64) as usize,
                best,
            );
            self.put(geo.free_dablk(fdb), fb);
        }
        Ok(())
    }

    /// Re-key `child` in its ancestors after its last hash changed.
    fn fix_key(&mut self, path: &[u64], child: u64) -> Result<(), FilesystemError> {
        let geo = self.geo;
        let mut child = child;
        for &parent in path.iter().rev() {
            let last = self.last_hash(child)?;
            let mut p = self.block(parent)?;
            let Some(idx) = node_find_child(&geo, &p, child) else {
                return Err(FilesystemError::Parse(format!(
                    "directory node {parent} does not point at block {child}"
                )));
            };
            let count = da_read_hdr(&geo, &p).count as usize;
            if da_ent(&geo, &p, idx).0 == last {
                break;
            }
            da_put_ent(&geo, &mut p, idx, last, child as u32);
            self.put(parent, p);
            if idx + 1 != count {
                break;
            }
            child = parent;
        }
        Ok(())
    }

    /// Move the root's contents to a fresh block and make the root a node
    /// one level up with that single child (`xfs_da3_root_split`).
    fn push_down_root(&mut self) -> Result<u64, FilesystemError> {
        let geo = self.geo;
        let root = self.block(geo.leaf_off)?;
        let h = da_read_hdr(&geo, &root);
        let level = if is_node_magic(h.magic) { h.aux } else { 0 };
        let child = self.first_hole(geo.leaf_off + geo.bpd, geo.free_off)?;
        let last = da_last_hash(&geo, &root);
        self.create(child, root);
        let mut newroot = build_da_node_block(&[(last, child as u32)], geo.dirblksize, geo.is_v5);
        let mut nh = da_read_hdr(&geo, &newroot);
        nh.aux = level + 1;
        da_write_hdr(&geo, &mut newroot, &nh);
        self.put(geo.leaf_off, newroot);
        Ok(child)
    }

    /// Split the full leafN `leaf` (ancestors in `path`) and add `(hash, addr)`.
    fn split_leaf_insert(
        &mut self,
        mut path: Vec<u64>,
        mut leaf: u64,
        hash: u32,
        addr: u32,
    ) -> Result<(), FilesystemError> {
        let geo = self.geo;
        if path.is_empty() {
            leaf = self.push_down_root()?;
            path.push(geo.leaf_off);
        }
        let mut old = self.block(leaf)?;
        leaf_compact(&geo, &mut old);
        let ents = da_ents(&geo, &old);
        if ents.len() < geo.da_max_ents() {
            // Compaction made room after all.
            leaf_insert(&geo, &mut old, hash, addr);
            self.put(leaf, old);
            return self.fix_key(&path, leaf);
        }
        let new_dablk = self.first_hole(geo.leaf_off + geo.bpd, geo.free_off)?;
        let mid = ents.len() / 2;
        let old_h = da_read_hdr(&geo, &old);
        let mut new = build_leafn_block(
            &ents[mid..],
            old_h.forw,
            leaf as u32,
            geo.dirblksize,
            geo.is_v5,
        );
        let end = geo.da_hdr() + geo.da_max_ents() * 8;
        da_set_ents(&geo, &mut old, &ents[..mid], end);
        let mut oh = da_read_hdr(&geo, &old);
        oh.forw = new_dablk as u32;
        da_write_hdr(&geo, &mut old, &oh);
        if old_h.forw != 0 {
            let mut next = self.block(u64::from(old_h.forw))?;
            let mut nh = da_read_hdr(&geo, &next);
            nh.back = new_dablk as u32;
            da_write_hdr(&geo, &mut next, &nh);
            self.put(u64::from(old_h.forw), next);
        }
        if hash < ents[mid].0 {
            leaf_insert(&geo, &mut old, hash, addr);
        } else {
            leaf_insert(&geo, &mut new, hash, addr);
        }
        self.put(leaf, old);
        self.create(new_dablk, new);
        self.insert_into_parent(&path, leaf, new_dablk)
    }

    /// Key `new_child` into the parent of `child` (splitting upward as needed).
    fn insert_into_parent(
        &mut self,
        path: &[u64],
        child: u64,
        new_child: u64,
    ) -> Result<(), FilesystemError> {
        let geo = self.geo;
        let Some((&parent, above)) = path.split_last() else {
            return Err(FilesystemError::Parse(
                "directory split reached above the root".into(),
            ));
        };
        let child_last = self.last_hash(child)?;
        let new_last = self.last_hash(new_child)?;
        let mut p = self.block(parent)?;
        let Some(idx) = node_find_child(&geo, &p, child) else {
            return Err(FilesystemError::Parse(format!(
                "directory node {parent} does not point at block {child}"
            )));
        };
        da_put_ent(&geo, &mut p, idx, child_last, child as u32);
        if node_insert(&geo, &mut p, idx + 1, new_last, new_child as u32) {
            self.put(parent, p);
            return self.fix_key(above, parent);
        }
        self.put(parent, p);
        if above.is_empty() {
            let copy = self.push_down_root()?;
            return self.split_node_insert(&[geo.leaf_off], copy, idx + 1, new_last, new_child);
        }
        self.split_node_insert(above, parent, idx + 1, new_last, new_child)
    }

    /// Split the full node `node` and add `(hash, before)` at slot `at`.
    fn split_node_insert(
        &mut self,
        above: &[u64],
        node: u64,
        at: usize,
        hash: u32,
        before: u64,
    ) -> Result<(), FilesystemError> {
        let geo = self.geo;
        let mut old = self.block(node)?;
        let ents = da_ents(&geo, &old);
        let oh = da_read_hdr(&geo, &old);
        let mid = ents.len() / 2;
        let new_dablk = self.first_hole(geo.leaf_off + geo.bpd, geo.free_off)?;
        let mut new = build_da_node_block(&ents[mid..], geo.dirblksize, geo.is_v5);
        let mut nh = da_read_hdr(&geo, &new);
        nh.aux = oh.aux;
        nh.forw = oh.forw;
        nh.back = node as u32;
        da_write_hdr(&geo, &mut new, &nh);
        let end = geo.da_hdr() + geo.da_max_ents() * 8;
        da_set_ents(&geo, &mut old, &ents[..mid], end);
        let mut oh2 = da_read_hdr(&geo, &old);
        oh2.forw = new_dablk as u32;
        da_write_hdr(&geo, &mut old, &oh2);
        if oh.forw != 0 {
            let mut next = self.block(u64::from(oh.forw))?;
            let mut h = da_read_hdr(&geo, &next);
            h.back = new_dablk as u32;
            da_write_hdr(&geo, &mut next, &h);
            self.put(u64::from(oh.forw), next);
        }
        if at <= mid {
            node_insert(&geo, &mut old, at, hash, before as u32);
        } else {
            node_insert(&geo, &mut new, at - mid, hash, before as u32);
        }
        self.put(node, old);
        self.create(new_dablk, new);
        self.insert_into_parent(above, node, new_dablk)
    }

    /// Remove `name`; `NotFound` when absent.
    pub fn remove(&mut self, name: &[u8]) -> Result<Outcome, FilesystemError> {
        let geo = self.geo;
        let hash = dir_hashname(name);
        let is_leaf1 = is_leaf1_magic(self.magic(geo.leaf_off)?);
        let (_, leaf0) = self.lookup_path(hash)?;
        let Some((leaf, idx, addr)) = self.find_entry(leaf0, hash, name)? else {
            return Err(FilesystemError::NotFound(
                String::from_utf8_lossy(name).into_owned(),
            ));
        };
        let db = geo.dataptr_db(addr);
        let off = geo.dataptr_off(addr);
        let mut data = self.block(geo.data_dablk(db))?;
        let (_, _, _, ent) = data_get_entry(&geo, &data, off)?;
        let empty = data_make_free(&geo, &mut data, off, ent)?;
        let best = data_best(&geo, &data);
        let mut lb = self.block(leaf)?;
        leaf_mark_stale(&geo, &mut lb, idx);
        self.put(leaf, lb);
        if empty && db != 0 {
            self.free(geo.data_dablk(db));
            self.set_best(is_leaf1, db, NULLDATAOFF)?;
            if is_leaf1 {
                let mut root = self.block(geo.leaf_off)?;
                let mut n = leaf1_bestcount(&root);
                while n > 0 && leaf1_best(&root, n - 1) == NULLDATAOFF {
                    n -= 1;
                }
                leaf1_set_bestcount(&mut root, n);
                self.put(geo.leaf_off, root);
            } else {
                let fdb = db / geo.free_max_bests() as u64;
                let fb = self.block(geo.free_dablk(fdb))?;
                if free_read_hdr(&geo, &fb).2 == 0 {
                    self.free(geo.free_dablk(fdb));
                }
            }
            // di_size reaches the last mapped data block (holes may remain).
            let old_blocks = self.size / geo.dirblksize as u64;
            let last = (0..old_blocks)
                .rev()
                .find(|&i| self.is_mapped(geo.data_dablk(i)))
                .map_or(0, |i| i + 1);
            self.size = last * geo.dirblksize as u64;
        } else {
            self.put(geo.data_dablk(db), data);
            self.set_best(is_leaf1, db, best)?;
        }
        if is_leaf1 {
            let root = self.block(geo.leaf_off)?;
            let live_blocks = (0..leaf1_bestcount(&root))
                .filter(|&i| leaf1_best(&root, i) != NULLDATAOFF)
                .count();
            return Ok(Outcome::Done {
                shrink: live_blocks <= 1,
            });
        }
        let lb = self.block(leaf)?;
        if leaf_live(&geo, &lb) == 0 && leaf != geo.leaf_off {
            self.remove_leaf(leaf)?;
        }
        // A lone leafN root whose live entries and bests fit a leaf1 block
        // can go back to leaf form (`xfs_dir2_node_to_leaf`).
        let root = self.block(geo.leaf_off)?;
        let shrink = is_leafn_magic(da_read_hdr(&geo, &root).magic) && {
            let mut nvalid = 0usize;
            let mut fdb = 0u64;
            while self.is_mapped(geo.free_dablk(fdb)) {
                let fb = self.block(geo.free_dablk(fdb))?;
                let (firstdb, nv, _) = free_read_hdr(&geo, &fb);
                nvalid = nvalid.max((firstdb + nv) as usize);
                fdb += 1;
            }
            geo.da_hdr() + leaf_live(&geo, &root) * 8 + nvalid * 2 + 4 <= geo.dirblksize
        };
        Ok(Outcome::Done { shrink })
    }

    /// Unlink and free a leaf with no live entries, then thin its ancestors.
    fn remove_leaf(&mut self, leaf: u64) -> Result<(), FilesystemError> {
        let geo = self.geo;
        let lb = self.block(leaf)?;
        let path = self.path_to(leaf, da_last_hash(&geo, &lb))?;
        let Some(path) = path else {
            return Ok(()); // ambiguous hashes: leave the empty leaf in place
        };
        self.unlink_and_free(leaf)?;
        self.remove_from_parent(&path, leaf)
    }

    /// The ancestors of `dablk`, found by descending with `hash`; `None`
    /// when the descent lands elsewhere.
    fn path_to(&mut self, dablk: u64, hash: u32) -> Result<Option<Vec<u64>>, FilesystemError> {
        let (path, leaf) = self.lookup_path(hash)?;
        if leaf == dablk {
            return Ok(Some(path));
        }
        // Equal hashes may continue in the next siblings.
        let geo = self.geo;
        let mut cur = leaf;
        for _ in 0..8 {
            let b = self.block(cur)?;
            let h = da_read_hdr(&geo, &b);
            if h.forw == 0 {
                break;
            }
            cur = u64::from(h.forw);
            if cur == dablk {
                let cb = self.block(cur)?;
                let last = da_last_hash(&geo, &cb);
                let (p2, l2) = self.lookup_path(last)?;
                if l2 == dablk {
                    return Ok(Some(p2));
                }
                break;
            }
        }
        Ok(None)
    }

    fn unlink_and_free(&mut self, dablk: u64) -> Result<(), FilesystemError> {
        let geo = self.geo;
        let b = self.block(dablk)?;
        let h = da_read_hdr(&geo, &b);
        if h.back != 0 {
            let mut prev = self.block(u64::from(h.back))?;
            let mut ph = da_read_hdr(&geo, &prev);
            ph.forw = h.forw;
            da_write_hdr(&geo, &mut prev, &ph);
            self.put(u64::from(h.back), prev);
        }
        if h.forw != 0 {
            let mut next = self.block(u64::from(h.forw))?;
            let mut nh = da_read_hdr(&geo, &next);
            nh.back = h.back;
            da_write_hdr(&geo, &mut next, &nh);
            self.put(u64::from(h.forw), next);
        }
        self.free(dablk);
        Ok(())
    }

    /// Drop `child` from the last node of `path`; an emptied node goes the
    /// same way, and a root left with one child absorbs it.
    fn remove_from_parent(&mut self, path: &[u64], child: u64) -> Result<(), FilesystemError> {
        let geo = self.geo;
        let Some((&parent, above)) = path.split_last() else {
            return Ok(());
        };
        let mut p = self.block(parent)?;
        let Some(idx) = node_find_child(&geo, &p, child) else {
            return Err(FilesystemError::Parse(format!(
                "directory node {parent} does not point at block {child}"
            )));
        };
        node_remove(&geo, &mut p, idx);
        let count = da_read_hdr(&geo, &p).count as usize;
        self.put(parent, p);
        if count == 0 && parent != geo.leaf_off {
            self.unlink_and_free(parent)?;
            return self.remove_from_parent(above, parent);
        }
        if parent == geo.leaf_off {
            // Root join: an only child's contents become the root, again
            // while that child is itself a one-child node.
            loop {
                let r = self.block(geo.leaf_off)?;
                let rh = da_read_hdr(&geo, &r);
                if !is_node_magic(rh.magic) || rh.count != 1 {
                    return Ok(());
                }
                let only = u64::from(da_ent(&geo, &r, 0).1);
                let mut c = self.block(only)?;
                let mut ch = da_read_hdr(&geo, &c);
                ch.forw = 0;
                ch.back = 0;
                da_write_hdr(&geo, &mut c, &ch);
                self.free(only);
                self.put(geo.leaf_off, c);
            }
        }
        self.fix_key(above, parent)
    }
}

#[cfg(test)]
mod tests {
    //! The tree algorithms against an in-memory store with 512-byte dir
    //! blocks, where two-level trees and a second freeindex block are a few
    //! thousand entries away, checked by a walker that re-derives every
    //! invariant `xfs_repair` would.
    use super::*;
    use std::collections::HashMap;

    struct MemStore {
        geo: Geo,
        blocks: BTreeMap<u64, Vec<u8>>,
        size: u64,
    }

    impl Dir2Store for MemStore {
        fn geo(&self) -> Geo {
            self.geo
        }
        fn read_block(&mut self, dablk: u64) -> Result<Option<Vec<u8>>, FilesystemError> {
            Ok(self.blocks.get(&dablk).cloned())
        }
        fn is_mapped(&self, dablk: u64) -> bool {
            self.blocks.contains_key(&dablk)
        }
        fn size(&self) -> u64 {
            self.size
        }
        fn apply(&mut self, txn: Dir2Txn) -> Result<bool, FilesystemError> {
            for d in &txn.freed {
                self.blocks.remove(d);
            }
            for (d, b) in txn.dirty {
                self.blocks.insert(d, b);
            }
            self.size = txn.size;
            Ok(true)
        }
    }

    /// A node-form directory holding only `.` and `..`: data block 0, a
    /// leafN root, one freeindex block.
    fn empty_node_dir(dirblksize: usize) -> MemStore {
        let geo = Geo::synthetic(dirblksize);
        let built = build_leaf_data_block(&[], 128, 128, 0, dirblksize, true, true, true).unwrap();
        let mut leaf = build_leafn_block(&built.leaf_index, 0, 0, dirblksize, true);
        let mut ents = built.leaf_index.clone();
        ents.sort_by_key(|e| e.0);
        da_set_ents(&geo, &mut leaf, &ents, dirblksize);
        let mut free = build_free_block(&[built.best], dirblksize, true).unwrap();
        free_write_hdr(&geo, &mut free, 0, 1, 1);
        let mut blocks = BTreeMap::new();
        blocks.insert(0, built.bytes);
        blocks.insert(geo.leaf_off, leaf);
        blocks.insert(geo.free_off, free);
        MemStore {
            geo,
            blocks,
            size: dirblksize as u64,
        }
    }

    /// Walk the whole structure, checking every invariant, and return the
    /// `(name, ino)` entries reachable through the hash index.
    fn check(store: &MemStore) -> HashMap<String, u64> {
        let geo = store.geo;
        let b = |d: u64| -> &Vec<u8> {
            store
                .blocks
                .get(&d)
                .unwrap_or_else(|| panic!("block {d} unmapped"))
        };
        // Data blocks: bestfree correct, record chain intact.
        let mut data_best_of: BTreeMap<u64, u16> = BTreeMap::new();
        let mut used: BTreeMap<u32, (u64, Vec<u8>)> = BTreeMap::new();
        for (&d, blk) in store.blocks.range(..geo.leaf_off) {
            let recs = data_records(&geo, blk).expect("records");
            let mut copy = blk.clone();
            data_freescan(&geo, &mut copy).unwrap();
            assert_eq!(&copy[..], &blk[..], "bestfree of data block {d} is stale");
            for (off, _, unused) in recs {
                if !unused {
                    let (ino, name, _, _) = data_get_entry(&geo, blk, off).unwrap();
                    used.insert(geo.dataptr(d / geo.bpd, off), (ino, name));
                }
            }
            data_best_of.insert(d / geo.bpd, data_best(&geo, blk));
            assert!(
                (d / geo.bpd + 1) * geo.dirblksize as u64 <= store.size,
                "di_size"
            );
        }
        // Freeindex: nused/nvalid and bests against the data blocks.
        let mut fdb = 0u64;
        while store.blocks.contains_key(&geo.free_dablk(fdb)) {
            let fb = b(geo.free_dablk(fdb));
            let (firstdb, nvalid, nused) = free_read_hdr(&geo, fb);
            assert_eq!(
                firstdb as usize,
                fdb as usize * geo.free_max_bests(),
                "firstdb"
            );
            let mut live = 0;
            for i in 0..nvalid as usize {
                let best = free_best(&geo, fb, i);
                let db = u64::from(firstdb) + i as u64;
                match data_best_of.get(&db) {
                    Some(&v) => {
                        assert_eq!(best, v, "free best of db {db}");
                        live += 1;
                    }
                    None => assert_eq!(best, NULLDATAOFF, "best for missing db {db}"),
                }
            }
            assert_eq!(nused, live, "nused of fdb {fdb}");
            if nvalid > 0 {
                assert_ne!(
                    free_best(&geo, fb, nvalid as usize - 1),
                    NULLDATAOFF,
                    "nvalid trimmed"
                );
            }
            fdb += 1;
        }
        // The da btree: keys, links, sorted hashes, stale counts, reachability.
        let mut seen: HashMap<String, u64> = HashMap::new();
        let mut level_prev: BTreeMap<u16, u64> = BTreeMap::new();
        fn walk(
            geo: &Geo,
            store: &MemStore,
            dablk: u64,
            expect_last: Option<u32>,
            used: &BTreeMap<u32, (u64, Vec<u8>)>,
            seen: &mut HashMap<String, u64>,
            level_prev: &mut BTreeMap<u16, u64>,
        ) -> u16 {
            let blk = store.blocks.get(&dablk).expect("tree block");
            let h = da_read_hdr(geo, blk);
            let ents = da_ents(geo, blk);
            for w in ents.windows(2) {
                assert!(w[0].0 <= w[1].0, "hash order in {dablk}");
            }
            if let Some(e) = expect_last {
                assert_eq!(da_last_hash(geo, blk), e, "parent key for {dablk}");
            }
            let level = if is_node_magic(h.magic) { h.aux } else { 0 };
            if is_node_magic(h.magic) {
                assert!(h.count > 0 && h.count as usize <= geo.da_max_ents());
                let mut child_level = None;
                for (hash, before) in ents {
                    let l = walk(
                        geo,
                        store,
                        u64::from(before),
                        Some(hash),
                        used,
                        seen,
                        level_prev,
                    );
                    assert!(
                        child_level.is_none() || child_level == Some(l),
                        "uneven levels"
                    );
                    child_level = Some(l);
                    assert_eq!(l + 1, level, "node level");
                }
            } else {
                assert!(is_leaf1_magic(h.magic) || is_leafn_magic(h.magic));
                assert!(h.count as usize <= leaf_capacity(geo, blk));
                let stale = ents.iter().filter(|e| e.1 == NULL_DATAPTR).count();
                assert_eq!(stale, h.aux as usize, "stale count in {dablk}");
                for (hash, addr) in ents {
                    if addr == NULL_DATAPTR {
                        continue;
                    }
                    let (ino, name) = used.get(&addr).unwrap_or_else(|| {
                        panic!("leaf {dablk} points at {addr:#x} which holds no entry")
                    });
                    assert_eq!(
                        dir_hashname(name),
                        hash,
                        "hash of {:?}",
                        String::from_utf8_lossy(name)
                    );
                    let prev = seen.insert(String::from_utf8_lossy(name).into_owned(), *ino);
                    assert!(prev.is_none(), "entry indexed twice");
                }
            }
            // Siblings at each level are chained in walk order.
            let expect_back = level_prev.get(&level).copied().unwrap_or(0);
            assert_eq!(u64::from(h.back), expect_back, "back link of {dablk}");
            if let Some(&prev) = level_prev.get(&level) {
                let ph = da_read_hdr(geo, store.blocks.get(&prev).unwrap());
                assert_eq!(u64::from(ph.forw), dablk, "forw link of {prev}");
            }
            level_prev.insert(level, dablk);
            level
        }
        walk(
            &geo,
            store,
            geo.leaf_off,
            None,
            &used,
            &mut seen,
            &mut level_prev,
        );
        for (_, dablk) in level_prev {
            let h = da_read_hdr(&geo, b(dablk));
            assert_eq!(h.forw, 0, "last block {dablk} at its level has a forw link");
        }
        assert_eq!(seen.len(), used.len(), "every data entry is indexed");
        // Every leaf-space block is reachable: no leaked tree blocks.
        seen
    }

    fn name_for(i: u32) -> String {
        format!("f{i:05}.txt")
    }

    #[test]
    fn inserts_grow_a_two_level_tree_and_a_second_freeindex_block() {
        let mut store = empty_node_dir(512);
        let mut model: HashMap<String, u64> = HashMap::new();
        model.insert(".".into(), 128);
        model.insert("..".into(), 128);
        for i in 0..5000u32 {
            let name = name_for(i);
            let mut op = Dir2Op::new(&mut store);
            let out = op.insert(name.as_bytes(), 1000 + u64::from(i), 1).unwrap();
            assert_eq!(out, Outcome::Done { shrink: false });
            assert!(op.commit().unwrap());
            model.insert(name, 1000 + u64::from(i));
            if i % 250 == 0 {
                assert_eq!(check(&store), model);
            }
        }
        assert_eq!(check(&store), model);
        let root = store.blocks.get(&store.geo.leaf_off).unwrap();
        let h = da_read_hdr(&store.geo, root);
        assert!(
            is_node_magic(h.magic) && h.aux >= 2,
            "expected a two-level tree, got level {}",
            h.aux
        );
        assert!(
            store.blocks.contains_key(&store.geo.free_dablk(1)),
            "second freeindex block"
        );
        let mut op = Dir2Op::new(&mut store);
        assert!(matches!(
            op.insert(b"f00007.txt", 1, 1),
            Err(FilesystemError::AlreadyExists(_))
        ));
    }

    #[test]
    fn removes_empty_the_tree_back_to_a_lone_leaf() {
        let mut store = empty_node_dir(512);
        let mut model: HashMap<String, u64> = HashMap::new();
        model.insert(".".into(), 128);
        model.insert("..".into(), 128);
        for i in 0..3000u32 {
            let name = name_for(i);
            let mut op = Dir2Op::new(&mut store);
            op.insert(name.as_bytes(), 1000 + u64::from(i), 1).unwrap();
            op.commit().unwrap();
            model.insert(name, 1000 + u64::from(i));
        }
        // Remove in a scrambled order so stale slots, emptied data blocks
        // and emptied leaves all occur.
        let mut order: Vec<u32> = (0..3000).collect();
        let mut x = 12345u64;
        for i in (1..order.len()).rev() {
            x = x
                .wrapping_mul(6364136223846793005)
                .wrapping_add(1442695040888963407);
            let j = (x >> 33) as usize % (i + 1);
            order.swap(i, j);
        }
        let mut last = Outcome::Done { shrink: false };
        for (n, i) in order.iter().enumerate() {
            let name = name_for(*i);
            let mut op = Dir2Op::new(&mut store);
            last = op.remove(name.as_bytes()).unwrap();
            assert!(op.commit().unwrap());
            model.remove(&name);
            if n % 200 == 0 {
                assert_eq!(check(&store), model);
            }
        }
        assert_eq!(check(&store), model);
        assert_eq!(last, Outcome::Done { shrink: true });
        let root = store.blocks.get(&store.geo.leaf_off).unwrap();
        assert!(
            is_leafn_magic(da_read_hdr(&store.geo, root).magic),
            "root collapsed to a leaf"
        );
        assert_eq!(
            store.blocks.range(..store.geo.leaf_off).count(),
            1,
            "only data block 0 remains"
        );
        let mut op = Dir2Op::new(&mut store);
        assert!(matches!(
            op.remove(b"gone"),
            Err(FilesystemError::NotFound(_))
        ));
    }

    #[test]
    fn mixed_churn_keeps_every_invariant() {
        let mut store = empty_node_dir(512);
        let mut model: HashMap<String, u64> = HashMap::new();
        model.insert(".".into(), 128);
        model.insert("..".into(), 128);
        let mut x = 99u64;
        let mut next = 0u32;
        for step in 0..6000 {
            x = x
                .wrapping_mul(6364136223846793005)
                .wrapping_add(1442695040888963407);
            let r = (x >> 33) as usize;
            let mut op = Dir2Op::new(&mut store);
            if matches!(r % 3, 1 | 2) || model.len() <= 2 {
                let name = name_for(next);
                next += 1;
                op.insert(name.as_bytes(), 5000 + u64::from(next), 1)
                    .unwrap();
                model.insert(name, 5000 + u64::from(next));
            } else {
                let victim = model
                    .keys()
                    .filter(|k| k.starts_with('f'))
                    .nth(r % (model.len() - 2))
                    .cloned()
                    .unwrap();
                op.remove(victim.as_bytes()).unwrap();
                model.remove(&victim);
            }
            assert!(op.commit().unwrap());
            if step % 300 == 0 {
                assert_eq!(check(&store), model);
            }
        }
        assert_eq!(check(&store), model);
    }
}
