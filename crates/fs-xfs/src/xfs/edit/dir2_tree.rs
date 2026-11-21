//! In-place inserts and removes for leaf-form and node-form directories.
//!
//! Written from SGI's *XFS Algorithms & Data Structures* (CC-BY-SA): ch.
//! Directories (secs. Block Directories, Leaf Directories, Node Directories,
//! B+tree Directories) for the block formats, and ch. Variable Length Record
//! B+trees (secs. Block Headers, Internal Nodes) for the hash tree.
//!
//! A directory's file has three address spaces: data blocks from offset 0,
//! the hash index from the leaf offset (32 GiB; its first block is the root:
//! a leaf1 block in leaf form, a leafN or a da node in node form) and
//! freeindex blocks from the free offset (64 GiB). A [`Dir2Op`] reads blocks
//! through a [`Dir2Store`], keeps every block it changes in memory, and hands
//! the store one [`Dir2Txn`] on commit, so nothing reaches the disk until the
//! whole change is known. The store stamps v5 CRC headers (blkno, owner, uuid,
//! crc) itself; this module only lays out the payload.
//!
//! Insert: hash the name, descend the tree (a node's entry covers every hash
//! up to its `hashval`), refuse a duplicate, pick the first data block whose
//! best free length fits the entry (else fill a hole or append a block, and a
//! freeindex block when the last one is full), carve the entry out of that
//! block's largest unused record, and put `(hash, address)` into the leaf,
//! taking the place of a stale entry when the leaf has one. A full leafN is
//! split in half; the new right sibling is linked after it and keyed into the
//! parent, which splits the same way when full. A full root is first copied
//! into a new block so the split has a parent (the root block never moves).
//! A full leaf1 returns [`Outcome::Fallback`] and `edit.rs` rebuilds the
//! directory into node form.
//!
//! Remove: find the entry, turn its bytes into an unused record merged with
//! unused neighbours, zero the leaf entry's address (stale; it keeps its
//! hash), free a data block left empty and null its best, unlink and free a
//! leaf whose entries are all stale (and a node left empty), and copy a root
//! with one child back up. Half-empty siblings are not merged. `shrink` tells
//! the caller when the next smaller form would hold what is left.
//!
//! Rules held for `xfs_repair`: `bestfree` lists the three longest unused
//! records, longest first, ties by offset, rescanned after every change; each
//! unused record ends with its own offset as a tag; leaf hashes stay sorted
//! and `stale` counts zero addresses; a node entry's hash is its child's last
//! hash; blocks at each level are chained through `forw` (next block) and
//! `back` (previous block); a freeindex block's `nused` counts its non-null
//! bests and `nvalid` ends at the last data block it covers.
//!
//! Two points where the document's prose does not match the disk: it
//! describes `forw` as the previous block and `back` as the next (its own
//! `xfs_db` dumps show `forw` as the next), and it places a freeindex
//! block's `bests` at the end of the block (its dump shows them right after
//! the header). This module follows the dumps, as the crate's readers do.

use std::collections::{BTreeMap, BTreeSet};

use byteorder::{BigEndian, ByteOrder};
use disk_core::filesystem::FilesystemError;

#[cfg(test)]
use super::build_leafn_block;
use super::{
    blocks_per_dir_block, build_free_block, build_leaf_data_block, dir2_da_hdr_len,
    dir2_data_entsize, dir2_data_hdr_len, dir2_free_hdr_len, dir2_free_offset_fb,
    dir2_leaf_offset_fb, dir_hashname, NULLDATAOFF, XFS_DA3_NODE_MAGIC, XFS_DA_NODE_MAGIC,
    XFS_DIR2_LEAF1_MAGIC, XFS_DIR2_LEAFN_MAGIC, XFS_DIR3_LEAF1_MAGIC, XFS_DIR3_LEAFN_MAGIC,
};
use crate::xfs::dir2::{XFS_DIR2_DATA_MAGIC, XFS_DIR3_DATA_MAGIC};

/// Address of a stale leaf entry (doc ch. Directories, sec. Leaf Directories).
const NULL_DATAPTR: u32 = 0;
/// `freetag` opening an unused data record (sec. Block Directories).
const DATA_FREE_TAG: u16 = 0xFFFF;
/// Deepest hash tree accepted while descending; guards against loops on a corrupt tree.
const MAX_TREE_DEPTH: usize = 16;

/// Directory geometry (doc ch. Directories, intro and sec. Leaf Directories).
#[derive(Clone, Copy, Debug)]
pub(super) struct Geo {
    pub dirblksize: usize,
    pub is_v5: bool,
    pub has_ftype: bool,
    /// Filesystem blocks per directory block.
    pub bpd: u64,
    /// First fsblock of the leaf (hash index) address space.
    pub leaf_off: u64,
    /// First fsblock of the freeindex address space.
    pub free_off: u64,
}

impl Geo {
    /// Geometry of a volume's directories, from its superblock.
    pub fn of(sb: &crate::xfs::sb::XfsSuperblock) -> Geo {
        Geo {
            dirblksize: sb.dirblksize() as usize,
            is_v5: sb.is_v5(),
            has_ftype: sb.has_ftype(),
            bpd: u64::from(blocks_per_dir_block(sb)),
            leaf_off: dir2_leaf_offset_fb(sb.blocksize),
            free_off: dir2_free_offset_fb(sb.blocksize),
        }
    }

    /// A v5, ftype geometry with one fsblock per `dirblksize` directory block.
    #[cfg(test)]
    pub fn synthetic(dirblksize: usize) -> Geo {
        Geo {
            dirblksize,
            is_v5: true,
            has_ftype: true,
            bpd: 1,
            leaf_off: dir2_leaf_offset_fb(dirblksize as u32),
            free_off: dir2_free_offset_fb(dirblksize as u32),
        }
    }

    /// True for a block of the hash index (leaf1, leafN or da node).
    pub fn is_leaf_space(&self, dablk: u64) -> bool {
        dablk >= self.leaf_off && dablk < self.free_off
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

    /// Leaf address of byte `off` in data block `db`: data-space offset / 8.
    fn dataptr(&self, db: u64, off: usize) -> u32 {
        ((db * self.dirblksize as u64 + off as u64) / 8) as u32
    }

    /// `(data block, byte offset in it)` of a leaf address.
    fn split_dataptr(&self, addr: u32) -> (u64, usize) {
        let byte = u64::from(addr) * 8;
        let dbs = self.dirblksize as u64;
        (byte / dbs, (byte % dbs) as usize)
    }

    fn data_dablk(&self, db: u64) -> u64 {
        db * self.bpd
    }

    /// File offset of freeindex block `fdb` (sec. Node Directories).
    fn free_dablk(&self, fdb: u64) -> u64 {
        self.free_off + fdb * self.bpd
    }

    /// Bests one freeindex block holds; block `fdb` covers data blocks from `fdb * this`.
    fn free_max_bests(&self) -> usize {
        (self.dirblksize - self.free_hdr()) / 2
    }

    /// Entries a leafN or da node block holds.
    fn da_max_ents(&self) -> usize {
        (self.dirblksize - self.da_hdr()) / 8
    }

    /// Bytes a data block offers entries when empty.
    fn data_body(&self) -> usize {
        self.dirblksize - self.data_hdr()
    }

    fn node_magic(&self) -> u16 {
        if self.is_v5 {
            XFS_DA3_NODE_MAGIC
        } else {
            XFS_DA_NODE_MAGIC
        }
    }
}

/// One directory change: blocks to write (created ones included), dablks to
/// map fresh, dablks to unmap and free, and the new `di_size`.
pub(super) struct Dir2Txn {
    pub dirty: BTreeMap<u64, Vec<u8>>,
    pub created: BTreeSet<u64>,
    pub freed: BTreeSet<u64>,
    pub size: u64,
}

/// Where a directory's blocks live; `apply` stamps v5 headers and writes,
/// returning `Ok(false)` when it backed out without touching anything.
pub(super) trait Dir2Store {
    fn geo(&self) -> Geo;
    fn read_block(&mut self, dablk: u64) -> Result<Option<Vec<u8>>, FilesystemError>;
    fn is_mapped(&self, dablk: u64) -> bool;
    fn size(&self) -> u64;
    fn apply(&mut self, txn: Dir2Txn) -> Result<bool, FilesystemError>;
}

/// Result of an insert or remove that did not fail.
#[derive(Debug, PartialEq, Eq)]
pub(super) enum Outcome {
    /// Staged; `shrink` when a smaller directory form would now fit.
    Done { shrink: bool },
    /// Not done here (a full leaf1); the caller rebuilds the directory.
    Fallback,
}

fn corrupt(msg: impl Into<String>) -> FilesystemError {
    FilesystemError::Parse(format!("directory: {}", msg.into()))
}

// ---- data blocks (doc ch. Directories, secs. Block and Leaf Directories) ----

fn bestfree_off(geo: &Geo) -> usize {
    if geo.is_v5 {
        48
    } else {
        4
    }
}

/// Every record of a data block as `(offset, length, unused)`, in offset order.
fn data_records(geo: &Geo, blk: &[u8]) -> Result<Vec<(usize, usize, bool)>, FilesystemError> {
    let mut out = Vec::new();
    let mut pos = geo.data_hdr();
    while pos < blk.len() {
        if pos + 8 > blk.len() {
            return Err(corrupt(format!("record at {pos} runs past the block")));
        }
        let unused = BigEndian::read_u16(&blk[pos..pos + 2]) == DATA_FREE_TAG;
        let len = if unused {
            BigEndian::read_u16(&blk[pos + 2..pos + 4]) as usize
        } else {
            dir2_data_entsize(blk[pos + 8] as usize, geo.has_ftype)
        };
        if len < 8 || len % 8 != 0 || pos + len > blk.len() {
            return Err(corrupt(format!("record at {pos} has bad length {len}")));
        }
        out.push((pos, len, unused));
        pos += len;
    }
    Ok(out)
}

/// The entry at `off`: `(inumber, name, ftype, record length)`.
fn data_get_entry(
    geo: &Geo,
    blk: &[u8],
    off: usize,
) -> Result<(u64, Vec<u8>, u8, usize), FilesystemError> {
    if off < geo.data_hdr()
        || off + 8 > blk.len()
        || BigEndian::read_u16(&blk[off..off + 2]) == DATA_FREE_TAG
    {
        return Err(corrupt(format!("no entry at data offset {off}")));
    }
    let namelen = blk[off + 8] as usize;
    let len = dir2_data_entsize(namelen, geo.has_ftype);
    if off + len > blk.len() {
        return Err(corrupt(format!("entry at {off} runs past the block")));
    }
    let ino = BigEndian::read_u64(&blk[off..off + 8]);
    let name = blk[off + 9..off + 9 + namelen].to_vec();
    let ft = if geo.has_ftype {
        blk[off + 9 + namelen]
    } else {
        0
    };
    Ok((ino, name, ft, len))
}

/// Rewrite `bestfree[]` from the records: the three longest unused, longest first, ties by offset.
fn data_freescan(geo: &Geo, blk: &mut [u8]) -> Result<(), FilesystemError> {
    let mut free: Vec<(usize, usize)> = data_records(geo, blk)?
        .into_iter()
        .filter(|r| r.2)
        .map(|r| (r.0, r.1))
        .collect();
    free.sort_by(|a, b| b.1.cmp(&a.1).then(a.0.cmp(&b.0)));
    let bf = bestfree_off(geo);
    for i in 0..3 {
        let (off, len) = free.get(i).copied().unwrap_or((0, 0));
        BigEndian::write_u16(&mut blk[bf + i * 4..bf + i * 4 + 2], off as u16);
        BigEndian::write_u16(&mut blk[bf + i * 4 + 2..bf + i * 4 + 4], len as u16);
    }
    Ok(())
}

/// `bestfree[0].length`, the value the leaf1 / freeindex `bests` mirror.
fn data_best(geo: &Geo, blk: &[u8]) -> u16 {
    let bf = bestfree_off(geo);
    BigEndian::read_u16(&blk[bf + 2..bf + 4])
}

/// Write an unused record over `[off, off + len)`: freetag, length, then the offset tag at its end.
fn write_unused(blk: &mut [u8], off: usize, len: usize) {
    blk[off..off + len].fill(0);
    BigEndian::write_u16(&mut blk[off..off + 2], DATA_FREE_TAG);
    BigEndian::write_u16(&mut blk[off + 2..off + 4], len as u16);
    BigEndian::write_u16(&mut blk[off + len - 2..off + len], off as u16);
}

/// Place an entry in the block's longest unused record; `None` when it does not fit.
fn data_place(
    geo: &Geo,
    blk: &mut [u8],
    name: &[u8],
    ino: u64,
    ft: u8,
) -> Result<Option<usize>, FilesystemError> {
    let need = dir2_data_entsize(name.len(), geo.has_ftype);
    let mut best: Option<(usize, usize)> = None;
    for (off, len, unused) in data_records(geo, blk)? {
        if unused && best.map_or(true, |b| len > b.1) {
            best = Some((off, len));
        }
    }
    let Some((off, len)) = best.filter(|b| b.1 >= need) else {
        return Ok(None);
    };
    blk[off..off + need].fill(0);
    BigEndian::write_u64(&mut blk[off..off + 8], ino);
    blk[off + 8] = name.len() as u8;
    blk[off + 9..off + 9 + name.len()].copy_from_slice(name);
    if geo.has_ftype {
        blk[off + 9 + name.len()] = ft;
    }
    BigEndian::write_u16(&mut blk[off + need - 2..off + need], off as u16);
    if len > need {
        write_unused(blk, off + need, len - need);
    }
    data_freescan(geo, blk)?;
    Ok(Some(off))
}

/// Free the entry at `off`, merging the unused records on either side of it.
fn data_make_free(geo: &Geo, blk: &mut [u8], off: usize) -> Result<(), FilesystemError> {
    let recs = data_records(geo, blk)?;
    let Some(i) = recs.iter().position(|r| r.0 == off && !r.2) else {
        return Err(corrupt(format!("no entry at data offset {off}")));
    };
    let mut start = off;
    let mut end = off + recs[i].1;
    if i > 0 && recs[i - 1].2 {
        start = recs[i - 1].0;
    }
    if let Some(next) = recs.get(i + 1).filter(|r| r.2) {
        end = next.0 + next.1;
    }
    write_unused(blk, start, end - start);
    data_freescan(geo, blk)
}

/// Bytes the block's live entries occupy.
fn data_used(geo: &Geo, blk: &[u8]) -> Result<usize, FilesystemError> {
    Ok(data_records(geo, blk)?
        .iter()
        .filter(|r| !r.2)
        .map(|r| r.1)
        .sum())
}

// ---- hash tree blocks (ch. Variable Length Record B+trees; ch. Directories) ----

/// The header shared by leaf1, leafN and da node blocks; `aux` is `stale` in
/// a leaf and `level` in a node.
#[derive(Clone, Copy, Debug)]
struct DaHdr {
    forw: u32,
    back: u32,
    magic: u16,
    count: u16,
    aux: u16,
}

fn da_count_off(geo: &Geo) -> usize {
    if geo.is_v5 {
        56
    } else {
        12
    }
}

fn da_read_hdr(geo: &Geo, blk: &[u8]) -> DaHdr {
    let c = da_count_off(geo);
    DaHdr {
        forw: BigEndian::read_u32(&blk[0..4]),
        back: BigEndian::read_u32(&blk[4..8]),
        magic: BigEndian::read_u16(&blk[8..10]),
        count: BigEndian::read_u16(&blk[c..c + 2]),
        aux: BigEndian::read_u16(&blk[c + 2..c + 4]),
    }
}

fn set_forw(blk: &mut [u8], forw: u32) {
    BigEndian::write_u32(&mut blk[0..4], forw);
}

fn set_back(blk: &mut [u8], back: u32) {
    BigEndian::write_u32(&mut blk[4..8], back);
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

/// Whether `m` is a hash tree magic of this volume's version.
fn tree_magic_ok(geo: &Geo, m: u16) -> bool {
    if geo.is_v5 {
        matches!(
            m,
            XFS_DIR3_LEAF1_MAGIC | XFS_DIR3_LEAFN_MAGIC | XFS_DA3_NODE_MAGIC
        )
    } else {
        matches!(
            m,
            XFS_DIR2_LEAF1_MAGIC | XFS_DIR2_LEAFN_MAGIC | XFS_DA_NODE_MAGIC
        )
    }
}

/// A leaf1 block's `bestcount` (the u32 tail at the end of the block).
fn leaf1_bestcount(blk: &[u8]) -> usize {
    BigEndian::read_u32(&blk[blk.len() - 4..]) as usize
}

/// Entries the block can hold: leaf1 shares its room with the bests table.
fn leaf_capacity(geo: &Geo, blk: &[u8]) -> usize {
    if is_leaf1_magic(da_read_hdr(geo, blk).magic) {
        let tail = 4 + 2 * leaf1_bestcount(blk);
        blk.len().saturating_sub(geo.da_hdr() + tail) / 8
    } else {
        geo.da_max_ents()
    }
}

/// `(hashval, address)` of a leaf or `(hashval, before)` of a node.
fn da_ents(geo: &Geo, blk: &[u8]) -> Vec<(u32, u32)> {
    let hdr = geo.da_hdr();
    let n = (da_read_hdr(geo, blk).count as usize).min((blk.len() - hdr) / 8);
    (0..n)
        .map(|i| {
            let o = hdr + i * 8;
            (
                BigEndian::read_u32(&blk[o..o + 4]),
                BigEndian::read_u32(&blk[o + 4..o + 8]),
            )
        })
        .collect()
}

/// Store `ents` and `count`, zeroing the rest up to `end`; a leaf's `stale` is recounted.
fn da_set_ents(geo: &Geo, blk: &mut [u8], ents: &[(u32, u32)], end: usize) {
    let hdr = geo.da_hdr();
    blk[hdr..end].fill(0);
    for (i, (h, a)) in ents.iter().enumerate() {
        let o = hdr + i * 8;
        BigEndian::write_u32(&mut blk[o..o + 4], *h);
        BigEndian::write_u32(&mut blk[o + 4..o + 8], *a);
    }
    let c = da_count_off(geo);
    BigEndian::write_u16(&mut blk[c..c + 2], ents.len() as u16);
    if !is_node_magic(BigEndian::read_u16(&blk[8..10])) {
        let stale = ents.iter().filter(|e| e.1 == NULL_DATAPTR).count();
        BigEndian::write_u16(&mut blk[c + 2..c + 4], stale as u16);
    }
}

/// Hash of the block's last entry: the key its parent holds for it.
fn da_last_hash(geo: &Geo, blk: &[u8]) -> u32 {
    da_ents(geo, blk).last().map_or(0, |e| e.0)
}

/// An empty leafN / node block with `magic` and, for a node, `level`.
fn new_da_block(geo: &Geo, magic: u16, level: u16) -> Vec<u8> {
    let mut blk = vec![0u8; geo.dirblksize];
    BigEndian::write_u16(&mut blk[8..10], magic);
    if is_node_magic(magic) {
        let c = da_count_off(geo);
        BigEndian::write_u16(&mut blk[c + 2..c + 4], level);
    }
    blk
}

/// A leaf1 block's `bests` table, one per data block (sec. Leaf Directories).
fn leaf1_bests(blk: &[u8]) -> Vec<u16> {
    let n = leaf1_bestcount(blk);
    let start = blk.len() - 4 - 2 * n;
    (0..n)
        .map(|i| BigEndian::read_u16(&blk[start + i * 2..start + i * 2 + 2]))
        .collect()
}

/// Lay out a leaf1 block: entries after the header, `bests` then `bestcount` at the end.
fn leaf1_write(geo: &Geo, blk: &mut [u8], ents: &[(u32, u32)], bests: &[u16]) {
    let len = blk.len();
    let start = len - 4 - 2 * bests.len();
    da_set_ents(geo, blk, ents, start);
    for (i, b) in bests.iter().enumerate() {
        BigEndian::write_u16(&mut blk[start + i * 2..start + i * 2 + 2], *b);
    }
    BigEndian::write_u32(&mut blk[len - 4..], bests.len() as u32);
}

// ---- freeindex blocks (ch. Directories, sec. Node Directories) ----

fn free_fields_off(geo: &Geo) -> usize {
    if geo.is_v5 {
        48
    } else {
        4
    }
}

/// `(firstdb, nvalid, nused)`.
fn free_read_hdr(geo: &Geo, blk: &[u8]) -> (u32, u32, u32) {
    let f = free_fields_off(geo);
    (
        BigEndian::read_u32(&blk[f..f + 4]),
        BigEndian::read_u32(&blk[f + 4..f + 8]),
        BigEndian::read_u32(&blk[f + 8..f + 12]),
    )
}

fn free_write_hdr(geo: &Geo, blk: &mut [u8], firstdb: u32, nvalid: u32, nused: u32) {
    let f = free_fields_off(geo);
    BigEndian::write_u32(&mut blk[f..f + 4], firstdb);
    BigEndian::write_u32(&mut blk[f + 4..f + 8], nvalid);
    BigEndian::write_u32(&mut blk[f + 8..f + 12], nused);
}

fn free_best(geo: &Geo, blk: &[u8], i: usize) -> u16 {
    let o = geo.free_hdr() + i * 2;
    BigEndian::read_u16(&blk[o..o + 2])
}

fn free_set_best(geo: &Geo, blk: &mut [u8], i: usize, v: u16) {
    let o = geo.free_hdr() + i * 2;
    BigEndian::write_u16(&mut blk[o..o + 2], v);
}

// ---- the operation ----

/// A path from the root: `(dablk, entry index)` per level, the leaf last.
type Chain = Vec<(u64, usize)>;

/// One insert or remove against a [`Dir2Store`], staged until [`Dir2Op::commit`].
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

    /// True when `name` is in the directory.
    pub fn contains(&mut self, name: &[u8]) -> Result<bool, FilesystemError> {
        Ok(self.find(name)?.is_some())
    }

    /// Stage adding `name -> ino` (with `ft` when the volume stores ftype).
    pub fn insert(&mut self, name: &[u8], ino: u64, ft: u8) -> Result<Outcome, FilesystemError> {
        if name.is_empty() || name.len() > 255 {
            return Err(FilesystemError::InvalidData(format!(
                "directory entry name of {} bytes",
                name.len()
            )));
        }
        if self.find(name)?.is_some() {
            return Err(FilesystemError::AlreadyExists(
                String::from_utf8_lossy(name).into_owned(),
            ));
        }
        let root = self.get(self.geo.leaf_off)?;
        if is_leaf1_magic(da_read_hdr(&self.geo, &root).magic) {
            self.insert_leaf1(root, name, ino, ft)
        } else {
            self.insert_node(name, ino, ft)
        }
    }

    /// Stage removing `name`; `NotFound` when it is not there.
    pub fn remove(&mut self, name: &[u8]) -> Result<Outcome, FilesystemError> {
        let geo = self.geo;
        let Some(mut chain) = self.find(name)? else {
            return Err(FilesystemError::NotFound(
                String::from_utf8_lossy(name).into_owned(),
            ));
        };
        let (leaf_d, slot) = *chain.last().expect("chain has a leaf");
        let mut leaf = self.get(leaf_d)?;
        let mut ents = da_ents(&geo, &leaf);
        let (db, off) = geo.split_dataptr(ents[slot].1);
        let dd = geo.data_dablk(db);
        let mut data = self.get_data(dd)?;
        data_make_free(&geo, &mut data, off)?;
        let best = data_best(&geo, &data);
        let emptied = db != 0 && best as usize == geo.data_body();
        if emptied {
            self.free(dd);
        } else {
            self.put(dd, data);
        }
        ents[slot].1 = NULL_DATAPTR;
        let new_best = if emptied { NULLDATAOFF } else { best };

        if is_leaf1_magic(da_read_hdr(&geo, &leaf).magic) {
            let mut bests = leaf1_bests(&leaf);
            let Some(b) = bests.get_mut(db as usize) else {
                return Err(corrupt(format!("data block {db} has no leaf1 best")));
            };
            *b = new_best;
            while bests.last() == Some(&NULLDATAOFF) {
                bests.pop();
            }
            leaf1_write(&geo, &mut leaf, &ents, &bests);
            self.put(leaf_d, leaf);
            if emptied {
                self.trim_size();
            }
            let shrink = self.leaf1_fits_block(&ents, &bests)?;
            return Ok(Outcome::Done { shrink });
        }

        da_set_ents(&geo, &mut leaf, &ents, geo.dirblksize);
        self.put(leaf_d, leaf);
        self.set_free_best(db, new_best)?;
        if emptied {
            self.trim_size();
        }
        if ents.iter().all(|e| e.1 == NULL_DATAPTR) && self.can_unhook(&chain)? {
            let k = chain.len() - 1;
            self.unhook(&mut chain, k)?;
        }
        self.collapse_root()?;
        let shrink = self.node_fits_leaf1()?;
        Ok(Outcome::Done { shrink })
    }

    /// Hand the staged change to the store; `Ok(false)` when it backed out.
    pub fn commit(self) -> Result<bool, FilesystemError> {
        if self.dirty.is_empty() && self.freed.is_empty() {
            return Ok(true);
        }
        let mut dirty = BTreeMap::new();
        for d in &self.dirty {
            dirty.insert(*d, self.cache[d].clone());
        }
        self.store.apply(Dir2Txn {
            dirty,
            created: self.created,
            freed: self.freed,
            size: self.size,
        })
    }

    // ---- block cache ----

    fn mapped(&self, d: u64) -> bool {
        self.created.contains(&d) || (self.store.is_mapped(d) && !self.freed.contains(&d))
    }

    /// The current content of block `d`, read through the store once.
    fn get(&mut self, d: u64) -> Result<Vec<u8>, FilesystemError> {
        if let Some(b) = self.cache.get(&d) {
            return Ok(b.clone());
        }
        if self.freed.contains(&d) {
            return Err(corrupt(format!("block {d} used after it was freed")));
        }
        let Some(b) = self.store.read_block(d)? else {
            return Err(corrupt(format!("block {d} is not mapped")));
        };
        if b.len() != self.geo.dirblksize {
            return Err(corrupt(format!("block {d} read as {} bytes", b.len())));
        }
        self.cache.insert(d, b.clone());
        Ok(b)
    }

    /// A data block, checked for this volume's data magic.
    fn get_data(&mut self, d: u64) -> Result<Vec<u8>, FilesystemError> {
        let b = self.get(d)?;
        let want = if self.geo.is_v5 {
            XFS_DIR3_DATA_MAGIC
        } else {
            XFS_DIR2_DATA_MAGIC
        };
        if BigEndian::read_u32(&b[0..4]) != want {
            return Err(corrupt(format!("data block {d} has a bad magic")));
        }
        Ok(b)
    }

    /// A hash tree block, checked for a tree magic of this volume's version.
    fn get_tree(&mut self, d: u64) -> Result<Vec<u8>, FilesystemError> {
        let b = self.get(d)?;
        if !tree_magic_ok(&self.geo, da_read_hdr(&self.geo, &b).magic) {
            return Err(corrupt(format!("hash tree block {d} has a bad magic")));
        }
        Ok(b)
    }

    fn put(&mut self, d: u64, b: Vec<u8>) {
        self.cache.insert(d, b);
        self.dirty.insert(d);
    }

    /// Map block `d` with content `b` (re-using it when freed earlier in this op).
    fn create(&mut self, d: u64, b: Vec<u8>) {
        if !self.freed.remove(&d) {
            self.created.insert(d);
        }
        self.put(d, b);
    }

    fn free(&mut self, d: u64) {
        self.cache.remove(&d);
        self.dirty.remove(&d);
        if !self.created.remove(&d) {
            self.freed.insert(d);
        }
    }

    // ---- lookup ----

    /// Root-to-leaf path for `hash`: in a node, the first entry whose hash is
    /// not below it (else the last); in the leaf, the first such slot.
    fn descend(&mut self, hash: u32) -> Result<Chain, FilesystemError> {
        let geo = self.geo;
        let mut chain = Chain::new();
        let mut d = geo.leaf_off;
        let mut level: Option<u16> = None;
        loop {
            if chain.len() >= MAX_TREE_DEPTH || !geo.is_leaf_space(d) {
                return Err(corrupt(format!("hash tree walk reached block {d}")));
            }
            let blk = self.get_tree(d)?;
            let h = da_read_hdr(&geo, &blk);
            let ents = da_ents(&geo, &blk);
            if !is_node_magic(h.magic) {
                let misplaced = !chain.is_empty() && !is_leafn_magic(h.magic);
                if misplaced || level.is_some_and(|l| l != 1) {
                    return Err(corrupt(format!(
                        "block {d} is not a leaf where one belongs"
                    )));
                }
                chain.push((d, ents.partition_point(|e| e.0 < hash)));
                return Ok(chain);
            }
            if ents.is_empty() || level.is_some_and(|l| h.aux.checked_add(1) != Some(l)) {
                return Err(corrupt(format!("node {d} is empty or out of level")));
            }
            let i = ents
                .iter()
                .position(|e| e.0 >= hash)
                .unwrap_or(ents.len() - 1);
            chain.push((d, i));
            level = Some(h.aux);
            d = u64::from(ents[i].1);
        }
    }

    /// Move `chain` to the next leaf in hash order; false after the last.
    fn advance(&mut self, chain: &mut Chain) -> Result<bool, FilesystemError> {
        let geo = self.geo;
        for k in (0..chain.len().saturating_sub(1)).rev() {
            let ents = da_ents(&geo, &self.get_tree(chain[k].0)?);
            if chain[k].1 + 1 >= ents.len() {
                continue;
            }
            chain[k].1 += 1;
            let mut child = u64::from(ents[chain[k].1].1);
            for j in k + 1..chain.len() {
                chain[j] = (child, 0);
                if j + 1 < chain.len() {
                    let below = da_ents(&geo, &self.get_tree(child)?);
                    let Some(first) = below.first() else {
                        return Err(corrupt(format!("node {child} is empty")));
                    };
                    child = u64::from(first.1);
                }
            }
            return Ok(true);
        }
        Ok(false)
    }

    /// Path to the leaf slot naming `name`, following equal hashes across leaves.
    fn find(&mut self, name: &[u8]) -> Result<Option<Chain>, FilesystemError> {
        let geo = self.geo;
        let hash = dir_hashname(name);
        let mut chain = self.descend(hash)?;
        loop {
            let (leaf, start) = *chain.last().expect("chain has a leaf");
            let ents = da_ents(&geo, &self.get_tree(leaf)?);
            let mut i = start;
            while i < ents.len() && ents[i].0 == hash {
                if ents[i].1 != NULL_DATAPTR {
                    let (db, off) = geo.split_dataptr(ents[i].1);
                    let data = self.get_data(geo.data_dablk(db))?;
                    if data_get_entry(&geo, &data, off)?.1 == name {
                        chain.last_mut().expect("leaf").1 = i;
                        return Ok(Some(chain));
                    }
                }
                i += 1;
            }
            if i < ents.len() || !self.advance(&mut chain)? {
                return Ok(None);
            }
        }
    }

    // ---- insert ----

    /// Leaf form: room in the leaf1 for one more entry (and a best when a data block is added), or Fallback.
    fn insert_leaf1(
        &mut self,
        mut leaf: Vec<u8>,
        name: &[u8],
        ino: u64,
        ft: u8,
    ) -> Result<Outcome, FilesystemError> {
        let geo = self.geo;
        let need = dir2_data_entsize(name.len(), geo.has_ftype);
        let mut ents = da_ents(&geo, &leaf);
        let mut bests = leaf1_bests(&leaf);
        let fits = bests
            .iter()
            .position(|&b| b != NULLDATAOFF && b as usize >= need);
        let (db, fresh) = match fits {
            Some(i) => (i, false),
            None => {
                let hole = (0..bests.len())
                    .find(|&i| bests[i] == NULLDATAOFF && !self.mapped(geo.data_dablk(i as u64)));
                (hole.unwrap_or(bests.len()), true)
            }
        };
        let nbests = bests.len().max(db + 1);
        let room = geo.dirblksize.saturating_sub(geo.da_hdr() + 4 + 2 * nbests) / 8;
        let stale = ents.iter().filter(|e| e.1 == NULL_DATAPTR).count();
        let after = if stale > 0 {
            ents.len()
        } else {
            ents.len() + 1
        };
        if after > room {
            if ents.len() - stale + 1 > room {
                return Ok(Outcome::Fallback);
            }
            ents.retain(|e| e.1 != NULL_DATAPTR);
        }
        let (addr, best) = self.place_in_data(db as u64, fresh, name, ino, ft)?;
        if db == bests.len() {
            bests.push(best);
        } else {
            bests[db] = best;
        }
        add_leaf_entry(&mut ents, dir_hashname(name), addr);
        leaf1_write(&geo, &mut leaf, &ents, &bests);
        self.put(geo.leaf_off, leaf);
        Ok(Outcome::Done { shrink: false })
    }

    /// Node form: data block from the freeindex, then the leaf, splitting upward as needed.
    fn insert_node(&mut self, name: &[u8], ino: u64, ft: u8) -> Result<Outcome, FilesystemError> {
        let geo = self.geo;
        let need = dir2_data_entsize(name.len(), geo.has_ftype);
        let (db, fresh) = self.pick_data_block(need)?;
        let (addr, best) = self.place_in_data(db, fresh, name, ino, ft)?;
        self.set_free_best(db, best)?;
        let hash = dir_hashname(name);
        let mut chain = self.descend(hash)?;
        let k = chain.len() - 1;
        let mut ents = da_ents(&geo, &self.get_tree(chain[k].0)?);
        add_leaf_entry(&mut ents, hash, addr);
        self.put_ents(&mut chain, k, ents)?;
        Ok(Outcome::Done { shrink: false })
    }

    /// Write the entry into data block `db` (made when `fresh`): its address and new best.
    fn place_in_data(
        &mut self,
        db: u64,
        fresh: bool,
        name: &[u8],
        ino: u64,
        ft: u8,
    ) -> Result<(u32, u16), FilesystemError> {
        let geo = self.geo;
        let dd = geo.data_dablk(db);
        if dd >= geo.leaf_off {
            return Err(FilesystemError::DiskFull(
                "directory data space is full".into(),
            ));
        }
        let mut data = if fresh {
            build_leaf_data_block(
                &[],
                0,
                0,
                db,
                geo.dirblksize,
                geo.has_ftype,
                false,
                geo.is_v5,
            )?
            .bytes
        } else {
            self.get_data(dd)?
        };
        let Some(off) = data_place(&geo, &mut data, name, ino, ft)? else {
            return Err(corrupt(format!(
                "data block {db} lacks the room its best claims"
            )));
        };
        let best = data_best(&geo, &data);
        if fresh {
            self.create(dd, data);
        } else {
            self.put(dd, data);
        }
        self.size = self.size.max((db + 1) * geo.dirblksize as u64);
        Ok((geo.dataptr(db, off), best))
    }

    /// Data blocks the directory spans: from `di_size`, extended over any mapped block past it.
    fn data_top(&self) -> u64 {
        let mut n = self.size / self.geo.dirblksize as u64;
        while self.geo.data_dablk(n) < self.geo.leaf_off && self.mapped(self.geo.data_dablk(n)) {
            n += 1;
        }
        n
    }

    /// After freeing data blocks, end `di_size` at the last one still mapped.
    fn trim_size(&mut self) {
        let mut n = self.data_top();
        while n > 0 && !self.mapped(self.geo.data_dablk(n - 1)) {
            n -= 1;
        }
        self.size = n * self.geo.dirblksize as u64;
    }

    /// Node form: the first data block whose best admits `need`, else a hole, else a new last block.
    fn pick_data_block(&mut self, need: usize) -> Result<(u64, bool), FilesystemError> {
        let geo = self.geo;
        let top = self.data_top();
        let max = geo.free_max_bests() as u64;
        let mut hole = None;
        for fdb in 0..top.div_ceil(max).max(1) {
            let d = geo.free_dablk(fdb);
            if !self.mapped(d) {
                continue;
            }
            let fb = self.get(d)?;
            let (firstdb, nvalid, _) = free_read_hdr(&geo, &fb);
            for i in 0..(nvalid as usize).min(geo.free_max_bests()) {
                let b = free_best(&geo, &fb, i);
                let db = u64::from(firstdb) + i as u64;
                if b == NULLDATAOFF {
                    if hole.is_none() && !self.mapped(geo.data_dablk(db)) {
                        hole = Some(db);
                    }
                } else if b as usize >= need {
                    return Ok((db, false));
                }
            }
        }
        Ok((hole.unwrap_or(top), true))
    }

    /// Record data block `db`'s best in its freeindex block (made when missing),
    /// keeping `nused` and `nvalid`; an emptied block past the first is freed.
    fn set_free_best(&mut self, db: u64, best: u16) -> Result<(), FilesystemError> {
        let geo = self.geo;
        let max = geo.free_max_bests() as u64;
        let (fdb, i) = (db / max, (db % max) as usize);
        let d = geo.free_dablk(fdb);
        let (mut fb, existed) = if self.mapped(d) {
            (self.get(d)?, true)
        } else {
            let mut b = build_free_block(&[], geo.dirblksize, geo.is_v5)?;
            free_write_hdr(&geo, &mut b, (fdb * max) as u32, 0, 0);
            (b, false)
        };
        let (firstdb, mut nvalid, mut nused) = free_read_hdr(&geo, &fb);
        if u64::from(firstdb) != fdb * max {
            return Err(corrupt(format!(
                "freeindex block {fdb} has firstdb {firstdb}"
            )));
        }
        while (nvalid as usize) <= i {
            free_set_best(&geo, &mut fb, nvalid as usize, NULLDATAOFF);
            nvalid += 1;
        }
        let prev = free_best(&geo, &fb, i);
        if prev == NULLDATAOFF && best != NULLDATAOFF {
            nused += 1;
        } else if prev != NULLDATAOFF && best == NULLDATAOFF {
            nused = nused.saturating_sub(1);
        }
        free_set_best(&geo, &mut fb, i, best);
        while nvalid > 0 && free_best(&geo, &fb, nvalid as usize - 1) == NULLDATAOFF {
            nvalid -= 1;
            free_set_best(&geo, &mut fb, nvalid as usize, 0);
        }
        free_write_hdr(&geo, &mut fb, firstdb, nvalid, nused);
        if nvalid == 0 && fdb > 0 {
            if existed {
                self.free(d);
            }
        } else if existed {
            self.put(d, fb);
        } else {
            self.create(d, fb);
        }
        Ok(())
    }

    /// Lowest unmapped block of the hash index past the root.
    fn alloc_tree_dablk(&self) -> Result<u64, FilesystemError> {
        let mut d = self.geo.leaf_off + self.geo.bpd;
        while self.mapped(d) {
            d += self.geo.bpd;
        }
        if d >= self.geo.free_off || d > u64::from(u32::MAX) {
            return Err(FilesystemError::DiskFull(
                "directory hash index space is full".into(),
            ));
        }
        Ok(d)
    }

    /// Store `ents` in `chain[k]`, splitting it (and its parents) when they
    /// overflow, then bring the keys above it up to date.
    fn put_ents(
        &mut self,
        chain: &mut Chain,
        k: usize,
        ents: Vec<(u32, u32)>,
    ) -> Result<(), FilesystemError> {
        let geo = self.geo;
        let d = chain[k].0;
        let mut blk = self.get_tree(d)?;
        let h = da_read_hdr(&geo, &blk);
        if ents.len() <= leaf_capacity(&geo, &blk) {
            if is_leaf1_magic(h.magic) {
                let bests = leaf1_bests(&blk);
                leaf1_write(&geo, &mut blk, &ents, &bests);
            } else {
                da_set_ents(&geo, &mut blk, &ents, geo.dirblksize);
            }
            self.put(d, blk);
            return self.fix_keys(chain, k);
        }
        if is_leaf1_magic(h.magic) {
            return Err(corrupt("leaf1 block overflowed"));
        }
        if k == 0 {
            // Root full: move its content to a new child so the split has a parent.
            let child = self.alloc_tree_dablk()?;
            let level = if is_node_magic(h.magic) { h.aux + 1 } else { 1 };
            let mut root = new_da_block(&geo, geo.node_magic(), level);
            da_set_ents(
                &geo,
                &mut root,
                &[(da_last_hash(&geo, &blk), child as u32)],
                geo.dirblksize,
            );
            self.create(child, blk);
            self.put(d, root);
            let slot = chain[0].1;
            chain[0].1 = 0;
            chain.insert(1, (child, slot));
            return self.put_ents(chain, 1, ents);
        }
        let mid = ents.len() / 2;
        let (left, right) = ents.split_at(mid);
        let sib = self.alloc_tree_dablk()?;
        let level = if is_node_magic(h.magic) { h.aux } else { 0 };
        let mut nb = new_da_block(&geo, h.magic, level);
        da_set_ents(&geo, &mut nb, right, geo.dirblksize);
        set_forw(&mut nb, h.forw);
        set_back(&mut nb, d as u32);
        da_set_ents(&geo, &mut blk, left, geo.dirblksize);
        set_forw(&mut blk, sib as u32);
        self.put(d, blk);
        self.create(sib, nb);
        if h.forw != 0 {
            let mut next = self.get_tree(u64::from(h.forw))?;
            set_back(&mut next, sib as u32);
            self.put(u64::from(h.forw), next);
        }
        let (p, pi) = chain[k - 1];
        let mut pents = da_ents(&geo, &self.get_tree(p)?);
        pents[pi].0 = left.last().expect("left half").0;
        pents.insert(pi + 1, (right.last().expect("right half").0, sib as u32));
        chain.truncate(k);
        self.put_ents(chain, k - 1, pents)
    }

    /// Re-key each parent from `chain[k]` up while its child's last hash changed.
    fn fix_keys(&mut self, chain: &Chain, k: usize) -> Result<(), FilesystemError> {
        let geo = self.geo;
        for j in (1..=k).rev() {
            let last = da_last_hash(&geo, &self.get_tree(chain[j].0)?);
            let (p, pi) = chain[j - 1];
            let mut pblk = self.get_tree(p)?;
            let o = geo.da_hdr() + pi * 8;
            if BigEndian::read_u32(&pblk[o..o + 4]) == last {
                break;
            }
            BigEndian::write_u32(&mut pblk[o..o + 4], last);
            self.put(p, pblk);
        }
        Ok(())
    }

    // ---- remove ----

    /// A dead leaf may go unless that would leave the root without children.
    fn can_unhook(&mut self, chain: &Chain) -> Result<bool, FilesystemError> {
        for k in (0..chain.len().saturating_sub(1)).rev() {
            let geo = self.geo;
            if da_read_hdr(&geo, &self.get_tree(chain[k].0)?).count > 1 {
                return Ok(true);
            }
        }
        Ok(false)
    }

    /// Unlink `chain[k]` from its siblings, free it, and drop its parent entry
    /// (the parent too, when that leaves it empty).
    fn unhook(&mut self, chain: &mut Chain, k: usize) -> Result<(), FilesystemError> {
        let geo = self.geo;
        let d = chain[k].0;
        let h = da_read_hdr(&geo, &self.get_tree(d)?);
        if h.back != 0 {
            let mut prev = self.get_tree(u64::from(h.back))?;
            set_forw(&mut prev, h.forw);
            self.put(u64::from(h.back), prev);
        }
        if h.forw != 0 {
            let mut next = self.get_tree(u64::from(h.forw))?;
            set_back(&mut next, h.back);
            self.put(u64::from(h.forw), next);
        }
        self.free(d);
        let (p, pi) = chain[k - 1];
        let mut pblk = self.get_tree(p)?;
        let mut pents = da_ents(&geo, &pblk);
        pents.remove(pi);
        chain.truncate(k);
        if pents.is_empty() && k > 1 {
            return self.unhook(chain, k - 1);
        }
        da_set_ents(&geo, &mut pblk, &pents, geo.dirblksize);
        self.put(p, pblk);
        let last = chain.len() - 1;
        chain[last].1 = pi.min(pents.len().saturating_sub(1));
        self.fix_keys(chain, last)
    }

    /// While the root is a node with one child, copy that child into the root.
    fn collapse_root(&mut self) -> Result<(), FilesystemError> {
        let geo = self.geo;
        loop {
            let root = self.get_tree(geo.leaf_off)?;
            let h = da_read_hdr(&geo, &root);
            if !is_node_magic(h.magic) || h.count != 1 {
                return Ok(());
            }
            let child = u64::from(da_ents(&geo, &root)[0].1);
            let mut cb = self.get_tree(child)?;
            set_forw(&mut cb, 0);
            set_back(&mut cb, 0);
            self.free(child);
            self.put(geo.leaf_off, cb);
        }
    }

    // ---- shrink checks ----

    /// Leaf form: one data block left and its entries plus leaf fit a block-form directory.
    fn leaf1_fits_block(
        &mut self,
        ents: &[(u32, u32)],
        bests: &[u16],
    ) -> Result<bool, FilesystemError> {
        if bests.iter().filter(|&&b| b != NULLDATAOFF).count() != 1 {
            return Ok(false);
        }
        let geo = self.geo;
        let used = data_used(&geo, &self.get_data(0)?)?;
        let live = ents.iter().filter(|e| e.1 != NULL_DATAPTR).count();
        Ok(geo.data_hdr() + used + live * 8 + 8 <= geo.dirblksize)
    }

    /// Node form: a lone leafN whose live entries and bests fit one leaf1 block.
    fn node_fits_leaf1(&mut self) -> Result<bool, FilesystemError> {
        let geo = self.geo;
        let root = self.get_tree(geo.leaf_off)?;
        let h = da_read_hdr(&geo, &root);
        if !is_leafn_magic(h.magic) || self.data_top() > geo.free_max_bests() as u64 {
            return Ok(false);
        }
        let live = h.count.saturating_sub(h.aux) as usize;
        if !self.mapped(geo.free_dablk(0)) {
            return Ok(false);
        }
        let (_, _, nused) = free_read_hdr(&geo, &self.get(geo.free_dablk(0))?);
        Ok(geo.da_hdr() + live * 8 + nused as usize * 2 + 4 <= geo.dirblksize)
    }
}

/// Add `(hash, addr)` in hash order, taking the place of the stale entry nearest it when there is one.
fn add_leaf_entry(ents: &mut Vec<(u32, u32)>, hash: u32, addr: u32) {
    let pos = ents.partition_point(|e| e.0 <= hash);
    let stale = ents
        .iter()
        .enumerate()
        .filter(|(_, e)| e.1 == NULL_DATAPTR)
        .min_by_key(|(i, _)| i.abs_diff(pos))
        .map(|(i, _)| i);
    if let Some(s) = stale {
        ents.remove(s);
    }
    let pos = ents.partition_point(|e| e.0 <= hash);
    ents.insert(pos, (hash, addr));
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
