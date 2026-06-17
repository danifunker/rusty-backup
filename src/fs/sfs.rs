// SFS ops take many parameters by nature (partition_offset, block_size,
// bitmap chain state, log/cancel callbacks).
#![allow(clippy::too_many_arguments)]

//! Smart File System (SFS) reader.
//!
//! Phase 7 scope: **read-only browse + backup**. Both DosType variants
//! that share the on-disk format — `SFS\0` and `SFS\2` — go through this
//! module. SFS write support is Phase 8.
//!
//! Block ID conventions (4-byte big-endian ASCII):
//! - `SFS\0` — RootBlock (block 0 and block totalblocks-1; pick highest
//!   sequencenumber).
//! - `OBJC`  — ObjectContainer.
//! - `BTMP`  — Bitmap.
//! - `ADMC`  — AdminSpaceContainer (we don't traverse these for read).
//! - `NDC ` — NodeContainer.
//! - `BNDC`  — BNodeContainer (B-tree of extents).
//! - `HTAB`  — HashTable (we ignore the hash and walk the chain).
//! - `SLNK`  — SoftLink (currently surfaced as a regular file).
//!
//! Every multi-byte field is big-endian. The block checksum field is the
//! 32-bit two's-complement so that the sum of all u32s in the block plus
//! 1 equals zero — when validating, zero the checksum first then verify
//! `sum(u32) == 0` ignoring the checksum slot. (See `SFScheck.c`
//! `checkchecksum`.)
//!
//! Bitmap convention: **set bit = free** (same as AFFS / PFS3), packed as
//! big-endian u32 words with the MSB representing the lowest block in
//! that word.

use std::collections::HashMap;
use std::io::{Read, Seek, SeekFrom, Write};

use super::entry::FileEntry;
use super::filesystem::{Filesystem, FilesystemError};
use super::CompactResult;

const ROOTBLOCK_ID: u32 = u32::from_be_bytes([b'S', b'F', b'S', 0]);
const OBJECTCONTAINER_ID: u32 = u32::from_be_bytes(*b"OBJC");
const NODECONTAINER_ID: u32 = u32::from_be_bytes(*b"NDC ");
const BNODECONTAINER_ID: u32 = u32::from_be_bytes(*b"BNDC");
const BITMAP_ID: u32 = u32::from_be_bytes(*b"BTMP");
#[allow(dead_code)]
const HASHTABLE_ID: u32 = u32::from_be_bytes(*b"HTAB");
const SOFTLINK_ID: u32 = u32::from_be_bytes(*b"SLNK");

const STRUCTURE_VERSION: u16 = 3;

const OTYPE_DIR: u8 = 128;
const OTYPE_LINK: u8 = 64;
#[allow(dead_code)]
const OTYPE_HARDLINK: u8 = 32;
const OTYPE_HIDDEN: u8 = 1;

const ROOTNODE: u32 = 1;

fn parse_err<S: Into<String>>(msg: S) -> FilesystemError {
    FilesystemError::Parse(msg.into())
}

fn rd_u16(buf: &[u8], o: usize) -> u16 {
    u16::from_be_bytes([buf[o], buf[o + 1]])
}
fn rd_u32(buf: &[u8], o: usize) -> u32 {
    u32::from_be_bytes([buf[o], buf[o + 1], buf[o + 2], buf[o + 3]])
}

/// Verify the SFS block header checksum and id+ownblock match.
fn validate_block(buf: &[u8], expected_id: u32, expected_blk: u32) -> Result<(), FilesystemError> {
    validate_block_with(
        buf,
        expected_id,
        expected_blk,
        /*check_checksum=*/ true,
    )
}

fn validate_block_with(
    buf: &[u8],
    expected_id: u32,
    expected_blk: u32,
    check_checksum: bool,
) -> Result<(), FilesystemError> {
    if buf.len() < 12 || !buf.len().is_multiple_of(4) {
        return Err(parse_err("SFS block buffer size invalid"));
    }
    let id = rd_u32(buf, 0);
    if id != expected_id {
        return Err(parse_err(format!(
            "SFS block id mismatch at {}: got {:08x}, want {:08x}",
            expected_blk, id, expected_id
        )));
    }
    let own = rd_u32(buf, 8);
    if own != expected_blk {
        return Err(parse_err(format!(
            "SFS block ownblock {} != expected {}",
            own, expected_blk
        )));
    }
    if !check_checksum {
        return Ok(());
    }
    // CALCCHECKSUM (asmsupport.s) starts d0 at 1 then sums all longs:
    // returns 0 when the block is valid. So sum_all_longs.wrapping_add(1)
    // == 0, i.e. sum_all_longs == 0xFFFFFFFF.
    let mut sum: u32 = 1;
    for o in (0..buf.len()).step_by(4) {
        sum = sum.wrapping_add(rd_u32(buf, o));
    }
    if sum != 0 {
        return Err(parse_err(format!(
            "SFS block {} checksum failure (calc={:#x})",
            expected_blk, sum
        )));
    }
    Ok(())
}

#[derive(Debug, Clone)]
pub struct SfsRootBlock {
    pub version: u16,
    pub sequencenumber: u16,
    pub datecreated: u32,
    pub bits: u8,
    pub firstbyte_full: u64,
    pub lastbyte_full: u64,
    pub totalblocks: u32,
    pub blocksize: u32,
    pub bitmapbase: u32,
    pub adminspacecontainer: u32,
    pub rootobjectcontainer: u32,
    pub extentbnoderoot: u32,
    pub objectnoderoot: u32,
}

impl SfsRootBlock {
    fn parse(buf: &[u8], own_block: u32) -> Result<Self, FilesystemError> {
        validate_block(buf, ROOTBLOCK_ID, own_block)?;
        if buf.len() < 0x60 {
            return Err(parse_err("SFS root block too small"));
        }
        let version = rd_u16(buf, 12);
        let sequencenumber = rd_u16(buf, 14);
        let datecreated = rd_u32(buf, 16);
        let bits = buf[20];
        // 20 bits, 21 pad1, 22 pad2 (UWORD), 24..32 reserved1, 32 firstbyteh,
        // 36 firstbyte, 40 lastbyteh, 44 lastbyte, 48 totalblocks, 52 blocksize.
        let firstbyteh = rd_u32(buf, 32) as u64;
        let firstbyte = rd_u32(buf, 36) as u64;
        let lastbyteh = rd_u32(buf, 40) as u64;
        let lastbyte = rd_u32(buf, 44) as u64;
        let totalblocks = rd_u32(buf, 48);
        let blocksize = rd_u32(buf, 52);
        // reserved2 (8 bytes) at 56, reserved3 (32 bytes) at 64.
        let bitmapbase = rd_u32(buf, 96);
        let adminspacecontainer = rd_u32(buf, 100);
        let rootobjectcontainer = rd_u32(buf, 104);
        let extentbnoderoot = rd_u32(buf, 108);
        let objectnoderoot = rd_u32(buf, 112);
        Ok(SfsRootBlock {
            version,
            sequencenumber,
            datecreated,
            bits,
            firstbyte_full: (firstbyteh << 32) | firstbyte,
            lastbyte_full: (lastbyteh << 32) | lastbyte,
            totalblocks,
            blocksize,
            bitmapbase,
            adminspacecontainer,
            rootobjectcontainer,
            extentbnoderoot,
            objectnoderoot,
        })
    }
}

/// One parsed object (file or directory) inside an ObjectContainer.
#[derive(Debug, Clone)]
struct SfsObject {
    objectnode: u32,
    protection: u32,
    data_or_hashtable: u32,
    size_or_firstdirblock: u32,
    datemodified: u32,
    bits: u8,
    name: String,
    comment: String,
}

impl SfsObject {
    fn is_dir(&self) -> bool {
        (self.bits & OTYPE_DIR) != 0
    }
    fn is_link(&self) -> bool {
        (self.bits & OTYPE_LINK) != 0
    }
    fn is_hidden(&self) -> bool {
        (self.bits & OTYPE_HIDDEN) != 0
    }
}

/// Parse a single fsObject starting at `buf[off]`. Returns the parsed
/// object and the byte length consumed (aligned to even). Returns None
/// when there is no room for another header.
fn parse_object(buf: &[u8], off: usize) -> Option<(SfsObject, usize)> {
    // fsObject layout:
    //  0..2  owneruid
    //  2..4  ownergid
    //  4..8  objectnode
    //  8..12 protection
    // 12..16 data | hashtable
    // 16..20 size | firstdirblock
    // 20..24 datemodified
    // 24..25 bits
    // 25     name (NUL-terminated)
    // ...    comment (NUL-terminated, directly after name)
    if off + 25 > buf.len() {
        return None;
    }
    let objectnode = rd_u32(buf, off + 4);
    if objectnode == 0 {
        // Slot is empty / end-of-container marker.
        return None;
    }
    let protection = rd_u32(buf, off + 8);
    let data_or_ht = rd_u32(buf, off + 12);
    let size_or_fdb = rd_u32(buf, off + 16);
    let datemodified = rd_u32(buf, off + 20);
    let bits = buf[off + 24];
    let mut p = off + 25;
    let name_end = match buf[p..].iter().position(|&b| b == 0) {
        Some(n) => p + n,
        None => return None,
    };
    let name = String::from_utf8_lossy(&buf[p..name_end]).to_string();
    p = name_end + 1;
    if p >= buf.len() {
        return None;
    }
    let comment_end = match buf[p..].iter().position(|&b| b == 0) {
        Some(n) => p + n,
        None => return None,
    };
    let comment = String::from_utf8_lossy(&buf[p..comment_end]).to_string();
    let mut end = comment_end + 1;
    // Align to even boundary.
    if end & 1 == 1 {
        end += 1;
    }
    Some((
        SfsObject {
            objectnode,
            protection,
            data_or_hashtable: data_or_ht,
            size_or_firstdirblock: size_or_fdb,
            datemodified,
            bits,
            name,
            comment,
        },
        end - off,
    ))
}

/// Tiny LRU-ish cache of block buffers.
type BlockBuf = Vec<u8>;
const CACHE_LIMIT: usize = 64;

/// Snapshot of mutable state for `EditableFilesystem` rollback. Captured
/// at the start of every mutation and restored on error so a partial
/// failure leaves the in-memory volume identical to the pre-call state.
#[derive(Clone)]
struct SfsSnapshot {
    root: SfsRootBlock,
    free_blocks_cached: u64,
    dirty: HashMap<u32, Vec<u8>>,
    cache: HashMap<u32, Vec<u8>>,
}

pub struct SfsFilesystem<R: Read + Seek> {
    reader: R,
    partition_offset: u64,
    partition_size: u64,
    root: SfsRootBlock,
    cache: HashMap<u32, BlockBuf>,
    /// Pending writes keyed by block number. Each value is a full block
    /// (`root.blocksize` bytes). `sync_metadata` flushes in ascending
    /// block order; `read_block` checks `dirty` first so subsequent reads
    /// observe the in-memory mutation. Checksums are re-stamped at flush
    /// time so callers don't need to keep them current.
    dirty: HashMap<u32, Vec<u8>>,
    /// Computed at open(); used by the &self `used_size` accessor.
    free_blocks_cached: u64,
}

impl<R: Read + Seek> SfsFilesystem<R> {
    pub fn open(mut reader: R, partition_offset: u64) -> Result<Self, FilesystemError> {
        let end = reader.seek(SeekFrom::End(0))?;
        let part_size_upper = end.saturating_sub(partition_offset);
        if part_size_upper < 2 * 512 {
            return Err(parse_err("partition too small for SFS"));
        }

        // Try root at block 0 first. Use a probe block size of 512; once
        // parsed, re-read with the real block size if different.
        let mut probe = [0u8; 512];
        reader.seek(SeekFrom::Start(partition_offset))?;
        reader.read_exact(&mut probe)?;
        if rd_u32(&probe, 0) != ROOTBLOCK_ID {
            return Err(parse_err(format!(
                "not an SFS volume: block-0 id = {:08x}",
                rd_u32(&probe, 0)
            )));
        }
        // We trust the probe to give us blocksize; re-read at proper size.
        let blocksize = rd_u32(&probe, 52) as u64;
        if !(512..=65536).contains(&blocksize) || !blocksize.is_multiple_of(512) {
            return Err(parse_err(format!(
                "SFS rootblock blocksize {} out of range",
                blocksize
            )));
        }
        let mut buf0 = vec![0u8; blocksize as usize];
        reader.seek(SeekFrom::Start(partition_offset))?;
        reader.read_exact(&mut buf0)?;
        let root0 = SfsRootBlock::parse(&buf0, 0).ok();

        // Attempt the backup root at block totalblocks-1.
        let totalblocks = root0
            .as_ref()
            .map(|r| r.totalblocks)
            .or_else(|| {
                // Fallback: use partition_size to estimate.
                Some((part_size_upper / blocksize) as u32)
            })
            .unwrap();
        let backup_off = partition_offset + (totalblocks.saturating_sub(1)) as u64 * blocksize;
        let root1 = if backup_off + blocksize <= partition_offset + part_size_upper {
            let mut buf1 = vec![0u8; blocksize as usize];
            reader.seek(SeekFrom::Start(backup_off))?;
            if reader.read_exact(&mut buf1).is_ok() {
                SfsRootBlock::parse(&buf1, totalblocks - 1).ok()
            } else {
                None
            }
        } else {
            None
        };

        // Pick the root with highest sequencenumber.
        let root = match (root0, root1) {
            (Some(a), Some(b)) => {
                if b.sequencenumber > a.sequencenumber {
                    b
                } else {
                    a
                }
            }
            (Some(a), None) => a,
            (None, Some(b)) => b,
            (None, None) => return Err(parse_err("SFS: neither root block parses")),
        };

        if root.version != STRUCTURE_VERSION {
            return Err(parse_err(format!(
                "SFS root structure version {} unsupported (expected {})",
                root.version, STRUCTURE_VERSION
            )));
        }
        if root.blocksize as u64 != blocksize {
            return Err(parse_err("SFS root blocksize mismatch between probes"));
        }

        // Clamp partition_size to root.totalblocks * blocksize.
        let claimed = root.totalblocks as u64 * blocksize;
        let partition_size = claimed.min(part_size_upper).max(blocksize);

        let mut fs = SfsFilesystem {
            reader,
            partition_offset,
            partition_size,
            root,
            cache: HashMap::new(),
            dirty: HashMap::new(),
            free_blocks_cached: 0,
        };
        // Best-effort: compute free block count once at open so the &self
        // used_size accessor doesn't need to walk the bitmap.
        fs.free_blocks_cached = match read_bitmap(&mut fs) {
            Ok(b) => b.iter().map(|x| x.count_ones() as u64).sum(),
            Err(_) => 0,
        };
        Ok(fs)
    }

    fn block_size(&self) -> u32 {
        self.root.blocksize
    }

    fn read_block(&mut self, blk: u32) -> Result<&[u8], FilesystemError> {
        // Pending writes shadow the on-disk state so subsequent reads
        // observe in-memory mutations. Metadata-block buffers carry
        // stale checksums between mutations and the next sync_metadata,
        // so re-stamp them on the way out — callers (and
        // `validate_block`) then see a self-consistent block. Data
        // blocks (no header) flow through untouched.
        if self.dirty.contains_key(&blk) {
            let buf = self.dirty.get_mut(&blk).unwrap();
            if is_metadata_block(buf) {
                stamp_checksum(buf);
            }
            return Ok(buf.as_slice());
        }
        if !self.cache.contains_key(&blk) {
            if self.cache.len() >= CACHE_LIMIT {
                if let Some(k) = self.cache.keys().next().copied() {
                    self.cache.remove(&k);
                }
            }
            let bs = self.root.blocksize as u64;
            let off = self.partition_offset + blk as u64 * bs;
            if off + bs > self.partition_offset + self.partition_size {
                return Err(parse_err(format!("SFS block {} out of range", blk)));
            }
            let mut buf = vec![0u8; bs as usize];
            self.reader.seek(SeekFrom::Start(off))?;
            self.reader.read_exact(&mut buf)?;
            self.cache.insert(blk, buf);
        }
        Ok(self.cache.get(&blk).unwrap().as_slice())
    }

    /// Walk the NodeContainer tree to translate `objectnode` into the BLCK
    /// of the ObjectContainer that holds the object.
    ///
    /// On-disk encoding (per pfs3aio's SFS sources, `nodes.c`):
    /// - Leaf NodeContainer (`nodes==1`): entries are direct `fsNode.data`
    ///   = BLCK of the ObjectContainer.
    /// - Internal NodeContainer (`nodes>1`): entries are `BLCKn` =
    ///   `(BLCK << shifts_block32) | flags`, where `shifts_block32 =
    ///   log2(blocksize) - 5` (4 for 512-byte blocks). The low bit is a
    ///   "container is full" flag we can simply mask off by shifting.
    fn lookup_object_block(&mut self, objectnode: u32) -> Result<u32, FilesystemError> {
        let shifts_block = self.root.blocksize.trailing_zeros();
        let shifts_block32 = shifts_block.saturating_sub(5);
        let mut blk = self.root.objectnoderoot;
        let mut guard = 0;
        loop {
            guard += 1;
            if guard > 64 {
                return Err(parse_err("NodeContainer tree too deep (loop?)"));
            }
            let buf = self.read_block(blk)?.to_vec();
            validate_block(&buf, NODECONTAINER_ID, blk)?;
            if buf.len() < 20 {
                return Err(parse_err("NodeContainer block too small"));
            }
            let nodenumber = rd_u32(&buf, 12);
            let nodes = rd_u32(&buf, 16);
            if nodes == 0 {
                return Err(parse_err("NodeContainer.nodes == 0"));
            }
            if objectnode < nodenumber {
                return Err(parse_err(format!(
                    "objectnode {} below container base {}",
                    objectnode, nodenumber
                )));
            }
            let offset_in_container = objectnode - nodenumber;
            let entry_idx = (offset_in_container / nodes) as usize;
            // Leaf entries are `fsObjectNode` (10 bytes); internal entries
            // are `BLCKn` (4 bytes). The BLCK pointer is the first 4 bytes
            // of either layout.
            let stride = if nodes == 1 { 10 } else { 4 };
            let entry_off = 20 + entry_idx * stride;
            if entry_off + 4 > buf.len() {
                return Err(parse_err(format!(
                    "objectnode {} entry index {} out of NodeContainer range",
                    objectnode, entry_idx
                )));
            }
            let entry = rd_u32(&buf, entry_off);
            if entry == 0 {
                return Err(parse_err(format!(
                    "objectnode {} has no mapping",
                    objectnode
                )));
            }
            if nodes == 1 {
                // Leaf — `entry` is a direct BLCK pointer.
                return Ok(entry);
            }
            // Internal — `entry` is BLCKn; shift to recover the BLCK.
            blk = entry >> shifts_block32;
        }
    }

    /// Walk the extent B-tree to find the extent describing data block `key`.
    /// Returns `(next_data_blk, prev_data_blk, blocks_in_extent)`.
    fn lookup_extent(&mut self, key: u32) -> Result<Option<(u32, u32, u32)>, FilesystemError> {
        let root = self.root.extentbnoderoot;
        if root == 0 {
            return Ok(None);
        }
        let mut blk = root;
        loop {
            let buf = self.read_block(blk)?.to_vec();
            validate_block(&buf, BNODECONTAINER_ID, blk)?;
            // fsBNodeContainer layout: 12B header + BTreeContainer:
            //   12: nodecount (u16), 14: isleaf (u8), 15: nodesize (u8)
            //   16: bnode[0]...
            if buf.len() < 16 {
                return Err(parse_err("BNodeContainer too small"));
            }
            let nodecount = rd_u16(&buf, 12) as usize;
            let isleaf = buf[14];
            let nodesize = buf[15] as usize;
            if nodesize == 0 || nodesize < 8 {
                return Err(parse_err("BNodeContainer nodesize invalid"));
            }
            let entries_off = 16;
            if isleaf == 0 {
                // Internal node: each BNode is (key,data) — find largest key <= key,
                // descend into its data pointer.
                let mut chosen: Option<u32> = None;
                for i in 0..nodecount {
                    let eo = entries_off + i * nodesize;
                    if eo + 8 > buf.len() {
                        break;
                    }
                    let k = rd_u32(&buf, eo);
                    if k > key {
                        break;
                    }
                    chosen = Some(rd_u32(&buf, eo + 4));
                }
                match chosen {
                    Some(c) => blk = c,
                    None => return Ok(None),
                }
            } else {
                // Leaf: each fsExtentBNode is 14 bytes (key, next, prev, blocks(u16)).
                // Find largest key <= search-key and check whether [k, k+blocks)
                // covers the target.
                let mut best: Option<(u32, u32, u32, u16)> = None;
                for i in 0..nodecount {
                    let eo = entries_off + i * nodesize;
                    if eo + 14 > buf.len() {
                        break;
                    }
                    let k = rd_u32(&buf, eo);
                    let nx = rd_u32(&buf, eo + 4);
                    let pv = rd_u32(&buf, eo + 8);
                    let bl = rd_u16(&buf, eo + 12);
                    if k > key {
                        break;
                    }
                    best = Some((k, nx, pv, bl));
                }
                if let Some((k, nx, pv, bl)) = best {
                    if key >= k && (key as u64) < k as u64 + bl as u64 {
                        return Ok(Some((nx, pv, bl as u32)));
                    }
                }
                return Ok(None);
            }
        }
    }

    /// Walk an object's directory by chasing the firstdirblock chain. Each
    /// ObjectContainer's `next` field points to the next container in the
    /// same parent directory.
    fn walk_directory_chain(
        &mut self,
        first_block: u32,
        mut cb: impl FnMut(&SfsObject),
    ) -> Result<(), FilesystemError> {
        let mut blk = first_block;
        let mut guard = 0u32;
        while blk != 0 {
            guard += 1;
            if guard > 1_000_000 {
                return Err(parse_err("SFS dir chain too long (loop?)"));
            }
            let buf = self.read_block(blk)?.to_vec();
            validate_block(&buf, OBJECTCONTAINER_ID, blk)?;
            // fsObjectContainer: 12B header + parent(4) + next(4) + previous(4)
            // = 24B prefix, then objects[].
            if buf.len() < 24 {
                return Err(parse_err("ObjectContainer too small"));
            }
            let next = rd_u32(&buf, 16);
            let mut off = 24usize;
            while off < buf.len() {
                match parse_object(&buf, off) {
                    Some((obj, consumed)) => {
                        cb(&obj);
                        off += consumed;
                    }
                    None => break,
                }
            }
            blk = next;
        }
        Ok(())
    }
}

impl<R: Read + Seek + Send> Filesystem for SfsFilesystem<R> {
    fn root(&mut self) -> Result<FileEntry, FilesystemError> {
        let mut fe = FileEntry::root();
        fe.location = ROOTNODE as u64;
        Ok(fe)
    }

    fn list_directory(&mut self, entry: &FileEntry) -> Result<Vec<FileEntry>, FilesystemError> {
        let parent_path = entry.path.clone();
        let node = entry.location as u32;
        // Find the ObjectContainer that holds this dir's fsObject, then
        // read its firstdirblock pointer.
        let firstdirblock = if node == ROOTNODE {
            // Root's object lives in the rootobjectcontainer per the docs;
            // its firstdirblock field is the first dir container chain.
            let obj_blk = self.root.rootobjectcontainer;
            let buf = self.read_block(obj_blk)?.to_vec();
            validate_block(&buf, OBJECTCONTAINER_ID, obj_blk)?;
            // The root entry is the first object in the rootobjectcontainer.
            // Its firstdirblock is at fsObject.size_or_firstdirblock (offset
            // 16 inside the object, so 24+16 within the block).
            if buf.len() < 24 + 21 {
                return Err(parse_err("root container too small"));
            }
            rd_u32(&buf, 24 + 16)
        } else {
            let obj_blk = self.lookup_object_block(node)?;
            let buf = self.read_block(obj_blk)?.to_vec();
            validate_block(&buf, OBJECTCONTAINER_ID, obj_blk)?;
            // Scan this container for the matching objectnode.
            let mut off = 24usize;
            let mut found: Option<u32> = None;
            while off < buf.len() {
                match parse_object(&buf, off) {
                    Some((obj, consumed)) => {
                        if obj.objectnode == node {
                            if !obj.is_dir() {
                                return Err(parse_err(format!(
                                    "objectnode {} is not a directory",
                                    node
                                )));
                            }
                            found = Some(obj.size_or_firstdirblock);
                            break;
                        }
                        off += consumed;
                    }
                    None => break,
                }
            }
            match found {
                Some(v) => v,
                None => {
                    return Err(parse_err(format!(
                        "objectnode {} not found in container",
                        node
                    )))
                }
            }
        };

        if firstdirblock == 0 {
            return Ok(Vec::new());
        }

        let mut raw: Vec<SfsObject> = Vec::new();
        self.walk_directory_chain(firstdirblock, |obj| raw.push(obj.clone()))?;

        let mut out: Vec<FileEntry> = Vec::with_capacity(raw.len());
        for obj in raw {
            if obj.is_hidden() {
                // Honor the OTYPE_HIDDEN bit — same as EXAMINE_NEXT on Amiga.
                continue;
            }
            let child_path = if parent_path == "/" {
                format!("/{}", obj.name)
            } else {
                format!("{}/{}", parent_path, obj.name)
            };
            let mut fe = if obj.is_dir() {
                FileEntry::new_directory(obj.name.clone(), child_path, obj.objectnode as u64)
            } else {
                let size = obj.size_or_firstdirblock as u64;
                FileEntry::new_file(obj.name.clone(), child_path, size, obj.objectnode as u64)
            };
            // For files we need to remember the first-data BLCK separately
            // (we use the `metadata1` slot if available; otherwise size+anode is enough
            // for non-fragmented reads via lookup, but practical files chain
            // through the extent btree — store start in a side map by tagging
            // the location with high bits would be hacky). For now stash
            // first-data block in fe.modified placeholder is wrong; instead
            // expose via metadata: amiga_first_data via fe via custom field.
            // FileEntry doesn't have an extra field, so we use `comment` only
            // when set; we'll set `comment` for SFS to the comment string and
            // re-derive first-data via lookup_object_block at read time.
            let _ = obj.data_or_hashtable;
            let _ = obj.protection;
            let _ = obj.datemodified;
            let _ = obj.comment;
            if obj.is_link() {
                // Mark with size 0; we don't follow softlinks yet.
                fe.size = 0;
            }
            out.push(fe);
        }
        Ok(out)
    }

    fn read_file(
        &mut self,
        entry: &FileEntry,
        max_bytes: usize,
    ) -> Result<Vec<u8>, FilesystemError> {
        let mut out: Vec<u8> = Vec::new();
        self.stream_file(entry, max_bytes as u64, &mut out)?;
        Ok(out)
    }

    fn write_file_to(
        &mut self,
        entry: &FileEntry,
        writer: &mut dyn Write,
    ) -> Result<u64, FilesystemError> {
        let max = entry.size;
        let mut adapter = WriteAdapter {
            inner: writer,
            written: 0,
        };
        self.stream_file(entry, max, &mut adapter)?;
        Ok(adapter.written)
    }

    fn volume_label(&self) -> Option<&str> {
        None
    }

    fn fs_type(&self) -> &str {
        "SFS"
    }

    fn total_size(&self) -> u64 {
        self.root.totalblocks as u64 * self.root.blocksize as u64
    }

    fn used_size(&self) -> u64 {
        let bs = self.root.blocksize as u64;
        self.total_size()
            .saturating_sub(self.free_blocks_cached * bs)
    }

    fn last_data_byte(&mut self) -> Result<u64, FilesystemError> {
        // SFS keeps a backup rootblock at `totalblocks - 1` that is always
        // marked allocated in the bitmap. On shrink it moves to the new
        // tail, so for the resize floor we ignore the last
        // SFS_BLOCKS_RESERVED_END block(s) and report the highest
        // non-backup-root allocated position, plus 1 block of headroom for
        // the relocated backup root at the new tail.
        match read_bitmap(self) {
            Ok(bitmap) => {
                let bs = self.root.blocksize as u64;
                let total = self.root.totalblocks as u64;
                // Highest block index to consider as "user data" — exclude
                // the trailing backup-root region.
                let last_user = total.saturating_sub(SFS_BLOCKS_RESERVED_END as u64);
                for byte_idx in (0..bitmap.len()).rev() {
                    let byte = bitmap[byte_idx];
                    if byte == 0xFF {
                        continue;
                    }
                    for bit in (0..8u8).rev() {
                        let block_idx = (byte_idx as u64) * 8 + bit as u64;
                        if block_idx >= last_user {
                            continue;
                        }
                        if (byte >> bit) & 1 == 0 {
                            // +1 for the byte after the highest allocated
                            // user block, +1 more block for the relocated
                            // backup root that lives at the new tail.
                            let floor_blocks = block_idx + 1 + SFS_BLOCKS_RESERVED_END as u64;
                            return Ok(floor_blocks * bs);
                        }
                    }
                }
                Ok(self.total_size())
            }
            Err(_) => Ok(self.total_size()),
        }
    }
}

impl<R: Read + Seek> SfsFilesystem<R> {
    fn stream_file(
        &mut self,
        entry: &FileEntry,
        max_bytes: u64,
        writer: &mut dyn Write,
    ) -> Result<u64, FilesystemError> {
        // Look up the object's container, scan for matching node, read
        // first-data BLCK and file size, then chain extents.
        let node = entry.location as u32;
        if node == ROOTNODE {
            return Err(parse_err("cannot read root as file"));
        }
        let container_blk = self.lookup_object_block(node)?;
        let container = self.read_block(container_blk)?.to_vec();
        validate_block(&container, OBJECTCONTAINER_ID, container_blk)?;

        let mut first_data: u32 = 0;
        let mut size: u64 = 0;
        let mut soft_link = false;
        let mut off = 24usize;
        while off < container.len() {
            match parse_object(&container, off) {
                Some((obj, consumed)) => {
                    if obj.objectnode == node {
                        if obj.is_dir() {
                            return Err(parse_err("cannot read directory as file"));
                        }
                        first_data = obj.data_or_hashtable;
                        size = obj.size_or_firstdirblock as u64;
                        if obj.is_link() {
                            soft_link = true;
                        }
                        break;
                    }
                    off += consumed;
                }
                None => break,
            }
        }
        if first_data == 0 || size == 0 {
            return Ok(0);
        }
        if soft_link {
            // SoftLink blocks: read the SLNK block content as the link
            // target. Surface as the file body.
            let blk = self.read_block(first_data)?.to_vec();
            validate_block(&blk, SOFTLINK_ID, first_data)?;
            if blk.len() < 24 {
                return Ok(0);
            }
            let target_end = blk[24..]
                .iter()
                .position(|&b| b == 0)
                .map(|i| 24 + i)
                .unwrap_or(blk.len());
            let n = (target_end - 24).min(max_bytes as usize);
            writer.write_all(&blk[24..24 + n])?;
            return Ok(n as u64);
        }

        let bs = self.root.blocksize as u64;
        let mut written: u64 = 0;
        let mut cur = first_data;
        let cap = size.min(max_bytes);
        let mut guard = 0u32;
        while cur != 0 && written < cap {
            guard += 1;
            if guard > 10_000_000 {
                return Err(parse_err("SFS extent walk too long (loop?)"));
            }
            let (next, _prev, blocks) = match self.lookup_extent(cur)? {
                Some(t) => t,
                None => break,
            };
            let extent_bytes = blocks as u64 * bs;
            let remaining = cap - written;
            let to_read = extent_bytes.min(remaining);
            // Stream the extent in 64 KiB chunks.
            let chunk = 64 * 1024usize;
            let mut done: u64 = 0;
            while done < to_read {
                let n = (chunk as u64).min(to_read - done) as usize;
                let off = self.partition_offset + cur as u64 * bs + done;
                self.reader.seek(SeekFrom::Start(off))?;
                let mut buf = vec![0u8; n];
                self.reader.read_exact(&mut buf)?;
                writer.write_all(&buf)?;
                done += n as u64;
            }
            written += to_read;
            cur = next;
        }
        Ok(written)
    }
}

struct WriteAdapter<'a> {
    inner: &'a mut dyn Write,
    written: u64,
}
impl<'a> Write for WriteAdapter<'a> {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        let n = self.inner.write(buf)?;
        self.written += n as u64;
        Ok(n)
    }
    fn flush(&mut self) -> std::io::Result<()> {
        self.inner.flush()
    }
}

/// Read the SFS bitmap into a flat byte vector. Bit `b` of byte `B`
/// (LSB-first within byte) represents block `B*8 + b`. Set bit = free.
fn read_bitmap<R: Read + Seek>(fs: &mut SfsFilesystem<R>) -> Result<Vec<u8>, FilesystemError> {
    let bs = fs.block_size() as u64;
    let total = fs.root.totalblocks;
    let total_bytes = (total as usize).div_ceil(8);
    let mut out = vec![0u8; total_bytes];

    // Bitmap blocks: the first lives at root.bitmapbase; subsequent ones
    // follow contiguously. Each block holds `(blocksize - 12) / 4` u32
    // words = `(blocksize - 12) * 2` sectors? No — `(blocksize - 12) * 8 / 4` blocks bits.
    let words_per_block = ((bs as usize) - 12) / 4;
    let bits_per_block = words_per_block * 32;
    let needed = (total as usize).div_ceil(bits_per_block);

    let mut base = fs.root.bitmapbase;
    let mut covered: u64 = 0;
    for _ in 0..needed {
        let buf = fs.read_block(base)?.to_vec();
        if rd_u32(&buf, 0) != BITMAP_ID {
            return Err(parse_err(format!("bitmap block {} bad id", base)));
        }
        // SFS bitmap blocks have a header but no ownblock-style validation
        // matching `validate_block` (the block has a checksum but it's
        // computed across the bitmap area too). Skip strict validation; we
        // still verify the id above.
        for w in 0..words_per_block {
            let o = 12 + w * 4;
            if o + 4 > buf.len() {
                break;
            }
            let word = rd_u32(&buf, o);
            if word == 0 {
                covered += 32;
                if covered >= total as u64 {
                    break;
                }
                continue;
            }
            for i in 0..32u32 {
                if (word >> (31 - i)) & 1 == 0 {
                    continue;
                }
                let global_blk = covered + i as u64;
                if global_blk >= total as u64 {
                    break;
                }
                let byte_idx = (global_blk / 8) as usize;
                let bit_off = (global_blk % 8) as u8;
                out[byte_idx] |= 1u8 << bit_off;
            }
            covered += 32;
            if covered >= total as u64 {
                break;
            }
        }
        base += 1;
        if covered >= total as u64 {
            break;
        }
    }
    Ok(out)
}

/// Layout-preserving compact reader for SFS. Allocated blocks are emitted
/// verbatim; free blocks are zeroed.
pub struct CompactSfsReader<R: Read + Seek> {
    reader: R,
    partition_offset: u64,
    partition_size: u64,
    blocksize: u32,
    /// Flat bitmap with one bit per block. Set bit = free.
    bitmap: Vec<u8>,
    position: u64,
    sec_buf: Vec<u8>,
    sec_loaded_for: Option<u32>,
}

impl<R: Read + Seek> CompactSfsReader<R> {
    pub fn new(
        mut reader: R,
        partition_offset: u64,
    ) -> Result<(Self, CompactResult), FilesystemError> {
        let mut fs = SfsFilesystem::open(&mut reader, partition_offset)?;
        let partition_size = fs.partition_size;
        let blocksize = fs.root.blocksize;
        let bitmap = read_bitmap(&mut fs).unwrap_or_default();

        let total_blocks = partition_size / blocksize as u64;
        let bitmap_bits = (bitmap.len() * 8) as u64;
        let mut free_blocks: u64 = 0;
        for byte in &bitmap {
            free_blocks += byte.count_ones() as u64;
        }
        let allocated = if bitmap.is_empty() {
            total_blocks
        } else {
            (bitmap_bits - free_blocks).min(total_blocks)
        };
        let result = CompactResult {
            original_size: partition_size,
            compacted_size: partition_size,
            data_size: allocated * blocksize as u64,
            clusters_used: allocated as u32,
        };
        Ok((
            CompactSfsReader {
                reader,
                partition_offset,
                partition_size,
                blocksize,
                bitmap,
                position: 0,
                sec_buf: vec![0u8; blocksize as usize],
                sec_loaded_for: None,
            },
            result,
        ))
    }

    fn block_is_allocated(&self, blk: u32) -> bool {
        if self.bitmap.is_empty() {
            return true;
        }
        let i = blk as usize;
        let byte = i / 8;
        let off = i % 8;
        if byte >= self.bitmap.len() {
            return true;
        }
        (self.bitmap[byte] >> off) & 1 == 0
    }
}

impl<R: Read + Seek> Read for CompactSfsReader<R> {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        if self.position >= self.partition_size || buf.is_empty() {
            return Ok(0);
        }
        let bs = self.blocksize as u64;
        let blk = (self.position / bs) as u32;
        let in_blk = (self.position % bs) as usize;
        let n = ((bs as usize) - in_blk).min(buf.len());

        if self.block_is_allocated(blk) {
            if self.sec_loaded_for != Some(blk) {
                let off = self.partition_offset + blk as u64 * bs;
                self.reader.seek(SeekFrom::Start(off))?;
                self.reader.read_exact(&mut self.sec_buf)?;
                self.sec_loaded_for = Some(blk);
            }
            buf[..n].copy_from_slice(&self.sec_buf[in_blk..in_blk + n]);
        } else {
            buf[..n].fill(0);
        }
        self.position += n as u64;
        Ok(n)
    }
}

// === Phase 8: editable SFS (allocator + EditableFilesystem impl) ===========
//
// **Atomicity model.** Mutations stage block-sized edits into `dirty`,
// keyed by block number. Reads consult dirty first, then the LRU cache,
// then disk. `sync_metadata` re-stamps each dirty block's checksum,
// bumps the root sequencenumber, and writes both root copies. Per-mutation
// snapshot/rollback (`SfsSnapshot`) means a partial failure leaves the
// in-memory volume identical to its pre-call state.
//
// **What we implement (minimum to support the staged-edits round-trip).**
//   - Data-block allocation: contiguous run via the BTMP bitmap.
//   - Admin-block allocation: scan AdminSpaceContainer.adminspace[].bits
//     for a clear bit. The default 32-block admin region (`SFS_BLOCKS_ADMIN`)
//     has 26 spare entries on a fresh blank volume — plenty for the test
//     surface.
//   - Object-node allocation: the leaf NDC produced by `create_blank_sfs`
//     has ~43 slots whose `fsObjectNode.data` field is 0 (free). We mark
//     a free slot by storing the new object's OBJC block in `data` and
//     return `nodenumber + slot_idx` as the new objectnode.
//   - Extent B-tree (BNDC): for a single-leaf btree we insert sorted
//     `fsExtentBNode` records on `create_file` and remove them on delete.
//     Splits/rebalance are deferred — the test image's leaf has room for
//     dozens of entries.
//   - ObjectContainer chain: each directory keeps a singly-linked OBJC
//     chain via fsObject.dir.firstdirblock → OBJC.next. Inserts splice
//     into the first OBJC with room, growing the chain by allocating a
//     fresh admin block when no existing OBJC fits.
//   - Soft-link / hardlink / set_blessed_folder etc. fall back to the
//     `EditableFilesystem` trait's default `Unsupported` errors.
//
// **What we DO NOT implement.**
//   - TRST/TRFA journal blocks for crash recovery. The TROK placeholder
//     at block 4 is left untouched; we rely on sync-boundary atomicity
//     by writing dirty buffers followed by both root copies with bumped
//     sequencenumber.
//   - B-tree splits (extent btree or object-node tree). DiskFull is
//     surfaced if the leaf overflows.
//   - HashTable maintenance — the dir.hashtable field stays zero on
//     fresh OBJC chains. Reading code in `walk_directory_chain` only
//     walks the OBJC chain, so this is functionally invisible.

const FS_EXTENTBNODE_SIZE: usize = 14;
const FS_OBJECTNODE_SIZE: usize = 10;

impl SfsRootBlock {
    /// Write the rootblock fields into the first portion of the block
    /// buffer. The `ownblock` field is left intact (callers stamp it).
    fn write_into(&self, buf: &mut [u8]) {
        assert!(buf.len() >= 116);
        write_u32(buf, 0, ROOTBLOCK_ID);
        // checksum at +4 stays zeroed; stamped before flush.
        // ownblock at +8 set by caller.
        write_u16(buf, 12, self.version);
        write_u16(buf, 14, self.sequencenumber);
        write_u32(buf, 16, self.datecreated);
        buf[20] = self.bits;
        write_u32(buf, 32, (self.firstbyte_full >> 32) as u32);
        write_u32(buf, 36, self.firstbyte_full as u32);
        write_u32(buf, 40, (self.lastbyte_full >> 32) as u32);
        write_u32(buf, 44, self.lastbyte_full as u32);
        write_u32(buf, 48, self.totalblocks);
        write_u32(buf, 52, self.blocksize);
        write_u32(buf, 96, self.bitmapbase);
        write_u32(buf, 100, self.adminspacecontainer);
        write_u32(buf, 104, self.rootobjectcontainer);
        write_u32(buf, 108, self.extentbnoderoot);
        write_u32(buf, 112, self.objectnoderoot);
    }
}

impl<R: Read + Seek> SfsFilesystem<R> {
    fn snapshot(&self) -> SfsSnapshot {
        SfsSnapshot {
            root: self.root.clone(),
            free_blocks_cached: self.free_blocks_cached,
            dirty: self.dirty.clone(),
            cache: self.cache.clone(),
        }
    }

    fn restore_snapshot(&mut self, snap: SfsSnapshot) {
        self.root = snap.root;
        self.free_blocks_cached = snap.free_blocks_cached;
        self.dirty = snap.dirty;
        self.cache = snap.cache;
    }

    /// Ensure block `blk` is staged in `dirty`. If absent, copy from the
    /// read cache (loading from disk first if necessary).
    fn ensure_dirty(&mut self, blk: u32) -> Result<(), FilesystemError> {
        if self.dirty.contains_key(&blk) {
            return Ok(());
        }
        let bytes = self.read_block(blk)?.to_vec();
        self.dirty.insert(blk, bytes);
        Ok(())
    }

    /// Allocate a contiguous run of `count` data blocks from the BTMP
    /// bitmap. Clears the bits in memory and returns the first block.
    fn alloc_data_blocks(&mut self, count: u32) -> Result<u32, FilesystemError> {
        if count == 0 {
            return Err(parse_err("alloc_data_blocks: count must be > 0"));
        }
        let bs = self.root.blocksize as u64;
        let words_per_block = ((bs as usize) - 12) / 4;
        let bits_per_block = words_per_block * 32;
        let total = self.root.totalblocks as usize;
        let needed_blocks = total.div_ceil(bits_per_block);

        // Pass 1: build a flat allocation bitmap from disk so we can scan
        // for a contiguous run without juggling cross-block boundaries.
        let bitmap = read_bitmap(self)?;
        let mut start: Option<usize> = None;
        let mut run: u32 = 0;
        for blk_idx in 0..total {
            let byte = bitmap[blk_idx / 8];
            let bit_set = (byte >> (blk_idx % 8)) & 1 == 1;
            if bit_set {
                if start.is_none() {
                    start = Some(blk_idx);
                    run = 1;
                } else {
                    run += 1;
                }
                if run >= count {
                    let first = start.unwrap() as u32;
                    self.toggle_bitmap(first, count, /*set_free=*/ false)?;
                    self.free_blocks_cached = self.free_blocks_cached.saturating_sub(count as u64);
                    let _ = needed_blocks;
                    return Ok(first);
                }
            } else {
                start = None;
                run = 0;
            }
        }
        Err(FilesystemError::DiskFull(format!(
            "SFS: no contiguous run of {} free data blocks",
            count
        )))
    }

    fn free_data_blocks(&mut self, first: u32, count: u32) -> Result<(), FilesystemError> {
        self.toggle_bitmap(first, count, /*set_free=*/ true)?;
        self.free_blocks_cached = self.free_blocks_cached.saturating_add(count as u64);
        Ok(())
    }

    /// Flip `count` contiguous bits in the BTMP bitmap. `set_free=true`
    /// marks them free (bit=1), false marks allocated (bit=0).
    fn toggle_bitmap(
        &mut self,
        first: u32,
        count: u32,
        set_free: bool,
    ) -> Result<(), FilesystemError> {
        let bs = self.root.blocksize as usize;
        let words_per_block = (bs - 12) / 4;
        let bits_per_block = words_per_block * 32;
        let base = self.root.bitmapbase;
        for i in 0..count {
            let global_blk = (first + i) as usize;
            let bm_seq = global_blk / bits_per_block;
            let within = global_blk % bits_per_block;
            let word_idx = within / 32;
            let bit = within % 32;
            let bm_blk = base + bm_seq as u32;
            self.ensure_dirty(bm_blk)?;
            let buf = self.dirty.get_mut(&bm_blk).unwrap();
            let o = 12 + word_idx * 4;
            let mut word = rd_u32(buf, o);
            let mask = 1u32 << (31 - bit as u32);
            if set_free {
                word |= mask;
            } else {
                word &= !mask;
            }
            buf[o..o + 4].copy_from_slice(&word.to_be_bytes());
        }
        Ok(())
    }

    /// Allocate one admin block by scanning the (single) AdminSpaceContainer's
    /// adminspace[0].bits for a clear bit. Returns the block number.
    fn alloc_admin_block(&mut self) -> Result<u32, FilesystemError> {
        let admc = self.root.adminspacecontainer;
        self.ensure_dirty(admc)?;
        let buf = self.dirty.get_mut(&admc).unwrap();
        // adminspace[0] sits at offset 28; bits field at offset 32.
        let space_start = rd_u32(buf, 28);
        let mut bits = rd_u32(buf, 32);
        for i in 0..32u32 {
            let mask = 1u32 << (31 - i);
            if bits & mask == 0 {
                bits |= mask;
                buf[32..36].copy_from_slice(&bits.to_be_bytes());
                let blk = space_start + i;
                // Zero the freshly allocated admin block in memory.
                let bs = self.root.blocksize as usize;
                self.dirty.insert(blk, vec![0u8; bs]);
                return Ok(blk);
            }
        }
        Err(FilesystemError::DiskFull(
            "SFS: no free admin-space slots".into(),
        ))
    }

    fn free_admin_block(&mut self, blk: u32) -> Result<(), FilesystemError> {
        let admc = self.root.adminspacecontainer;
        self.ensure_dirty(admc)?;
        let buf = self.dirty.get_mut(&admc).unwrap();
        let space_start = rd_u32(buf, 28);
        if blk < space_start || blk >= space_start + 32 {
            return Err(parse_err(format!(
                "free_admin_block: block {} outside admin region [{}, {})",
                blk,
                space_start,
                space_start + 32
            )));
        }
        let i = blk - space_start;
        let mask = 1u32 << (31 - i);
        let mut bits = rd_u32(buf, 32);
        bits &= !mask;
        buf[32..36].copy_from_slice(&bits.to_be_bytes());
        self.dirty.remove(&blk);
        self.cache.remove(&blk);
        Ok(())
    }

    /// Allocate a new object-node by finding a free fsObjectNode slot in
    /// the leaf NodeContainer (data==0). Stores `data` = `obj_blk` and
    /// returns the new objectnode number.
    fn alloc_object_node(&mut self, obj_blk: u32) -> Result<u32, FilesystemError> {
        let nc_blk = self.root.objectnoderoot;
        self.ensure_dirty(nc_blk)?;
        let buf = self.dirty.get_mut(&nc_blk).unwrap();
        let nodenumber = rd_u32(buf, 12);
        let nodes = rd_u32(buf, 16);
        if nodes != 1 {
            return Err(parse_err(
                "alloc_object_node: only leaf NodeContainer supported",
            ));
        }
        let bs = buf.len();
        let max_slots = (bs - 20) / FS_OBJECTNODE_SIZE;
        for slot in 0..max_slots {
            let o = 20 + slot * FS_OBJECTNODE_SIZE;
            if o + 4 > bs {
                break;
            }
            let data = rd_u32(buf, o);
            if data == 0 {
                buf[o..o + 4].copy_from_slice(&obj_blk.to_be_bytes());
                // next + hash16 stay zero.
                return Ok(nodenumber + slot as u32);
            }
        }
        Err(FilesystemError::DiskFull(
            "SFS: object-node leaf full (tree growth not implemented)".into(),
        ))
    }

    fn free_object_node(&mut self, objectnode: u32) -> Result<(), FilesystemError> {
        let nc_blk = self.root.objectnoderoot;
        self.ensure_dirty(nc_blk)?;
        let buf = self.dirty.get_mut(&nc_blk).unwrap();
        let nodenumber = rd_u32(buf, 12);
        let nodes = rd_u32(buf, 16);
        if nodes != 1 {
            return Err(parse_err(
                "free_object_node: only leaf NodeContainer supported",
            ));
        }
        if objectnode < nodenumber {
            return Err(parse_err(format!(
                "free_object_node: {} below nodenumber {}",
                objectnode, nodenumber
            )));
        }
        let slot = (objectnode - nodenumber) as usize;
        let o = 20 + slot * FS_OBJECTNODE_SIZE;
        if o + FS_OBJECTNODE_SIZE > buf.len() {
            return Err(parse_err(format!(
                "free_object_node: slot {} out of range",
                slot
            )));
        }
        for b in &mut buf[o..o + FS_OBJECTNODE_SIZE] {
            *b = 0;
        }
        Ok(())
    }

    /// Insert an extent record `(key=start_block, blocks=count)` into the
    /// extent B-tree leaf. Records stay sorted by key. next/prev are
    /// initialized to zero; we don't maintain the doubly-linked chain
    /// because our reader only consults the btree to look up extents by
    /// key.
    fn extent_btree_insert(&mut self, key: u32, blocks: u32) -> Result<(), FilesystemError> {
        let leaf_blk = self.root.extentbnoderoot;
        self.ensure_dirty(leaf_blk)?;
        let buf = self.dirty.get_mut(&leaf_blk).unwrap();
        let bs = buf.len();
        let mut nodecount = rd_u16(buf, 12) as usize;
        let isleaf = buf[14];
        let nodesize = buf[15] as usize;
        if isleaf == 0 {
            return Err(parse_err(
                "extent_btree_insert: only single-leaf BNDC supported",
            ));
        }
        if nodesize != FS_EXTENTBNODE_SIZE {
            return Err(parse_err(format!(
                "extent_btree_insert: unexpected nodesize {}",
                nodesize
            )));
        }
        let entries_off = 16;
        let max = (bs - entries_off) / nodesize;
        if nodecount >= max {
            return Err(FilesystemError::DiskFull(
                "SFS: extent B-tree leaf full (splits not implemented)".into(),
            ));
        }
        // Find insertion point (sorted by key).
        let mut idx = nodecount;
        for i in 0..nodecount {
            let o = entries_off + i * nodesize;
            let k = rd_u32(buf, o);
            if key < k {
                idx = i;
                break;
            }
        }
        // Shift later entries right by one nodesize.
        let shift_from = entries_off + idx * nodesize;
        let shift_end = entries_off + nodecount * nodesize;
        if shift_from < shift_end {
            buf.copy_within(shift_from..shift_end, shift_from + nodesize);
        }
        // Stamp the new record.
        let o = shift_from;
        write_u32(buf, o, key);
        write_u32(buf, o + 4, 0); // next
        write_u32(buf, o + 8, 0); // prev
        write_u16(buf, o + 12, blocks as u16);
        nodecount += 1;
        write_u16(buf, 12, nodecount as u16);
        Ok(())
    }

    fn extent_btree_remove(&mut self, key: u32) -> Result<(), FilesystemError> {
        let leaf_blk = self.root.extentbnoderoot;
        self.ensure_dirty(leaf_blk)?;
        let buf = self.dirty.get_mut(&leaf_blk).unwrap();
        let bs = buf.len();
        let mut nodecount = rd_u16(buf, 12) as usize;
        let isleaf = buf[14];
        let nodesize = buf[15] as usize;
        if isleaf == 0 {
            return Err(parse_err(
                "extent_btree_remove: only single-leaf BNDC supported",
            ));
        }
        let entries_off = 16;
        let mut found: Option<usize> = None;
        for i in 0..nodecount {
            let o = entries_off + i * nodesize;
            if rd_u32(buf, o) == key {
                found = Some(i);
                break;
            }
        }
        let idx = found.ok_or_else(|| {
            parse_err(format!(
                "extent_btree_remove: key {} not found in leaf",
                key
            ))
        })?;
        let from = entries_off + (idx + 1) * nodesize;
        let to = entries_off + nodecount * nodesize;
        if from < to {
            buf.copy_within(from..to, entries_off + idx * nodesize);
        }
        // Zero the now-unused trailing slot to keep the tail clean.
        let last = entries_off + (nodecount - 1) * nodesize;
        for b in &mut buf[last..last + nodesize] {
            *b = 0;
        }
        nodecount -= 1;
        write_u16(buf, 12, nodecount as u16);
        let _ = bs;
        Ok(())
    }

    // Find the OBJC block containing the fsObject for `objectnode` in the
    // directory chain starting at `first_obj_blk`. Returns the block number
    // plus the byte offset of that fsObject within the block. Currently
    // unused; kept for future filesystem-resize work.
    //
    // fn find_object_in_chain(
    //     &mut self,
    //     first_obj_blk: u32,
    //     objectnode: u32,
    // ) -> Result<(u32, usize), FilesystemError> {
    //     let mut blk = first_obj_blk;
    //     while blk != 0 {
    //         let buf = self.read_block(blk)?.to_vec();
    //         validate_block(&buf, OBJECTCONTAINER_ID, blk)?;
    //         let next = rd_u32(&buf, 16);
    //         let mut off = 24usize;
    //         while off < buf.len() {
    //             match parse_object(&buf, off) {
    //                 Some((obj, consumed)) => {
    //                     if obj.objectnode == objectnode {
    //                         return Ok((blk, off));
    //                     }
    //                     off += consumed;
    //                 }
    //                 None => break,
    //             }
    //         }
    //         blk = next;
    //     }
    //     Err(parse_err(format!(
    //         "find_object_in_chain: objectnode {} not found",
    //         objectnode
    //     )))
    // }

    /// Locate the parent directory's fsObject within the root OBJC chain.
    /// For ROOTNODE this is the fsObject embedded in the root OBJC at
    /// offset 24; for any other parent we walk the object-node tree
    /// followed by the matching OBJC.
    fn locate_dir_object(&mut self, parent_node: u32) -> Result<(u32, usize), FilesystemError> {
        if parent_node == ROOTNODE {
            return Ok((self.root.rootobjectcontainer, 24));
        }
        let blk = self.lookup_object_block(parent_node)?;
        let buf = self.read_block(blk)?.to_vec();
        validate_block(&buf, OBJECTCONTAINER_ID, blk)?;
        let mut off = 24usize;
        while off < buf.len() {
            match parse_object(&buf, off) {
                Some((obj, consumed)) => {
                    if obj.objectnode == parent_node {
                        return Ok((blk, off));
                    }
                    off += consumed;
                }
                None => break,
            }
        }
        Err(parse_err(format!(
            "locate_dir_object: parent {} not found in container at {}",
            parent_node, blk
        )))
    }

    /// Find an OBJC in the directory chain with enough room for `need`
    /// bytes, or allocate a new one and link it. Returns the block.
    fn ensure_dir_chain_room(
        &mut self,
        parent_dir_blk: u32,
        parent_dir_obj_off: usize,
        parent_node: u32,
        need: usize,
    ) -> Result<u32, FilesystemError> {
        // Read the parent dir's fsObject to find firstdirblock.
        let parent_buf = self.read_block(parent_dir_blk)?.to_vec();
        let firstdir = rd_u32(&parent_buf, parent_dir_obj_off + 16);
        let bs = self.root.blocksize as usize;

        // Walk the chain looking for an OBJC with `need` bytes of free tail.
        if firstdir != 0 {
            let mut blk = firstdir;
            while blk != 0 {
                let buf = self.read_block(blk)?.to_vec();
                validate_block(&buf, OBJECTCONTAINER_ID, blk)?;
                let next = rd_u32(&buf, 16);
                let mut off = 24usize;
                while off < bs {
                    match parse_object(&buf, off) {
                        Some((_obj, consumed)) => off += consumed,
                        None => break,
                    }
                }
                if bs - off >= need {
                    return Ok(blk);
                }
                if next == 0 {
                    break;
                }
                blk = next;
            }
        }

        // No existing block fits — allocate a fresh OBJC from admin space
        // and link it (either as firstdirblock or as the chain tail's next).
        let new_blk = self.alloc_admin_block()?;
        {
            let buf = self.dirty.get_mut(&new_blk).unwrap();
            write_u32(buf, 0, OBJECTCONTAINER_ID);
            write_u32(buf, 8, new_blk); // ownblock
            write_u32(buf, 12, parent_node); // parent NODE
            write_u32(buf, 16, 0); // next
            write_u32(buf, 20, 0); // previous (could chase the chain tail and link)
        }
        if firstdir == 0 {
            // Update parent fsObject.dir.firstdirblock = new_blk.
            self.ensure_dirty(parent_dir_blk)?;
            let pb = self.dirty.get_mut(&parent_dir_blk).unwrap();
            write_u32(pb, parent_dir_obj_off + 16, new_blk);
        } else {
            // Walk the chain in-memory and set tail.next = new_blk.
            let mut blk = firstdir;
            loop {
                self.ensure_dirty(blk)?;
                let buf = self.dirty.get_mut(&blk).unwrap();
                let next = rd_u32(buf, 16);
                if next == 0 {
                    write_u32(buf, 16, new_blk);
                    // Also stamp new block's `previous`.
                    let new = self.dirty.get_mut(&new_blk).unwrap();
                    write_u32(new, 20, blk);
                    break;
                }
                blk = next;
            }
        }
        Ok(new_blk)
    }
}

/// Encode an fsObject for write. Returns the encoded bytes (length is even).
fn build_object(
    objectnode: u32,
    protection: u32,
    data_or_ht: u32,
    size_or_fdb: u32,
    datemodified: u32,
    bits: u8,
    name: &str,
    comment: &str,
) -> Result<Vec<u8>, FilesystemError> {
    let name_b = name.as_bytes();
    let cmt_b = comment.as_bytes();
    if name.is_empty() || name_b.len() > 30 {
        return Err(parse_err(format!(
            "build_object: name '{}' must be 1..30 bytes",
            name
        )));
    }
    if cmt_b.len() > 79 {
        return Err(parse_err("build_object: comment exceeds 79 bytes"));
    }
    let unaligned = 25 + name_b.len() + 1 + cmt_b.len() + 1;
    let aligned = (unaligned + 1) & !1;
    let mut buf = vec![0u8; aligned];
    write_u32(&mut buf, 4, objectnode);
    write_u32(&mut buf, 8, protection);
    write_u32(&mut buf, 12, data_or_ht);
    write_u32(&mut buf, 16, size_or_fdb);
    write_u32(&mut buf, 20, datemodified);
    buf[24] = bits;
    buf[25..25 + name_b.len()].copy_from_slice(name_b);
    // NUL terminator after name + before comment already 0.
    let cmt_pos = 25 + name_b.len() + 1;
    buf[cmt_pos..cmt_pos + cmt_b.len()].copy_from_slice(cmt_b);
    Ok(buf)
}

impl<R: Read + Write + Seek + Send> SfsFilesystem<R> {
    fn do_create_directory(
        &mut self,
        parent: &FileEntry,
        name: &str,
    ) -> Result<FileEntry, FilesystemError> {
        check_no_duplicate(self, parent, name)?;
        let parent_node = parent.location as u32;
        let (parent_dir_blk, parent_dir_obj_off) = self.locate_dir_object(parent_node)?;

        // Reserve room in the parent's OBJC chain first; the chain block
        // that ends up holding our fsObject is the same block we'll
        // associate with the new directory's objectnode.
        let obj_bytes_probe = build_object(
            1,
            FIBF_READ | FIBF_WRITE | FIBF_EXECUTE | FIBF_DELETE,
            0,
            0,
            0,
            OTYPE_DIR,
            name,
            "",
        )?;
        let chain_blk = self.ensure_dir_chain_room(
            parent_dir_blk,
            parent_dir_obj_off,
            parent_node,
            obj_bytes_probe.len(),
        )?;
        let new_node = self.alloc_object_node(chain_blk)?;

        // Final fsObject carries the real objectnode. hashtable=0,
        // firstdirblock=0 → empty directory.
        let obj_bytes = build_object(
            new_node,
            FIBF_READ | FIBF_WRITE | FIBF_EXECUTE | FIBF_DELETE,
            0,
            0,
            0,
            OTYPE_DIR,
            name,
            "",
        )?;
        self.splice_object_into_block(chain_blk, &obj_bytes)?;

        let path = if parent.path == "/" {
            format!("/{}", name)
        } else {
            format!("{}/{}", parent.path, name)
        };
        Ok(FileEntry::new_directory(
            name.to_string(),
            path,
            new_node as u64,
        ))
    }

    fn do_create_file(
        &mut self,
        parent: &FileEntry,
        name: &str,
        data: &mut dyn std::io::Read,
        data_len: u64,
    ) -> Result<FileEntry, FilesystemError> {
        check_no_duplicate(self, parent, name)?;
        let parent_node = parent.location as u32;
        let (parent_dir_blk, parent_dir_obj_off) = self.locate_dir_object(parent_node)?;

        let bs = self.root.blocksize as u64;
        let (first_data, blocks) = if data_len == 0 {
            (0u32, 0u32)
        } else {
            let n = data_len.div_ceil(bs) as u32;
            let first = self.alloc_data_blocks(n)?;
            (first, n)
        };

        // Stream data bytes into dirty entries, zero-padding the tail.
        if blocks > 0 {
            let mut written: u64 = 0;
            for i in 0..blocks {
                let mut buf = vec![0u8; bs as usize];
                let remaining = data_len - written;
                let to_read = remaining.min(bs) as usize;
                data.read_exact(&mut buf[..to_read])?;
                self.dirty.insert(first_data + i, buf);
                written += to_read as u64;
            }
            self.extent_btree_insert(first_data, blocks)?;
        }

        // Reserve room in parent's chain; the block that ends up
        // holding our fsObject is the one we associate with the new
        // file's objectnode.
        let obj_bytes_probe = build_object(
            1,
            FIBF_READ | FIBF_WRITE | FIBF_EXECUTE | FIBF_DELETE,
            first_data,
            data_len as u32,
            0,
            0,
            name,
            "",
        )?;
        let chain_blk = self.ensure_dir_chain_room(
            parent_dir_blk,
            parent_dir_obj_off,
            parent_node,
            obj_bytes_probe.len(),
        )?;
        let new_node = self.alloc_object_node(chain_blk)?;
        let obj_bytes = build_object(
            new_node,
            FIBF_READ | FIBF_WRITE | FIBF_EXECUTE | FIBF_DELETE,
            first_data,
            data_len as u32,
            0,
            0,
            name,
            "",
        )?;
        self.splice_object_into_block(chain_blk, &obj_bytes)?;

        let path = if parent.path == "/" {
            format!("/{}", name)
        } else {
            format!("{}/{}", parent.path, name)
        };
        Ok(FileEntry::new_file(
            name.to_string(),
            path,
            data_len,
            new_node as u64,
        ))
    }

    fn do_rename(
        &mut self,
        parent: &FileEntry,
        entry: &FileEntry,
        new_name: &str,
    ) -> Result<(), FilesystemError> {
        let parent_node = parent.location as u32;
        let entry_node = entry.location as u32;

        // Locate the entry's fsObject within its OBJC chain block and
        // parse all of its fields so we can reconstruct it with only the
        // name changed. The objectnode (= identity), the data/extent
        // pointers, and the file data are all left untouched.
        let chain_blk = self.lookup_object_block(entry_node)?;
        let cur = self.read_block(chain_blk)?.to_vec();
        let mut off = 24usize;
        let mut found: Option<(usize, SfsObject, usize)> = None;
        while off < cur.len() {
            match parse_object(&cur, off) {
                Some((obj, consumed)) => {
                    if obj.objectnode == entry_node {
                        found = Some((off, obj, consumed));
                        break;
                    }
                    off += consumed;
                }
                None => break,
            }
        }
        let (obj_off, obj, consumed) = found.ok_or_else(|| {
            parse_err(format!(
                "rename: object {} not found in block {}",
                entry_node, chain_blk
            ))
        })?;

        if new_name.as_bytes() == obj.name.as_bytes() {
            return Ok(());
        }

        // Reject a collision with a *different* entry. SFS folds case, so a
        // case-only self-rename matches `entry` itself — allow that.
        {
            let kids = self.list_directory(parent)?;
            if kids
                .iter()
                .any(|c| c.name.eq_ignore_ascii_case(new_name) && c.location != entry.location)
            {
                return Err(FilesystemError::AlreadyExists(new_name.to_string()));
            }
        }

        // Build the replacement object bytes (validates the name 1..30).
        let new_bytes = build_object(
            obj.objectnode,
            obj.protection,
            obj.data_or_hashtable,
            obj.size_or_firstdirblock,
            obj.datemodified,
            obj.bits,
            new_name,
            &obj.comment,
        )?;

        if new_bytes.len() == consumed {
            // Same encoded length — overwrite the object bytes in place.
            // ensure_dirty so the checksum is re-stamped at flush time.
            self.ensure_dirty(chain_blk)?;
            let buf = self.dirty.get_mut(&chain_blk).unwrap();
            buf[obj_off..obj_off + consumed].copy_from_slice(&new_bytes);
            return Ok(());
        }

        // Different length — splice the old object out of its block, then
        // place the rebuilt object. Try the same block first; if it no
        // longer has room, grow the parent's OBJC chain like create does.
        self.ensure_dirty(chain_blk)?;
        let buf = self.dirty.get_mut(&chain_blk).unwrap();
        let end = buf.len();
        buf.copy_within(obj_off + consumed..end, obj_off);
        let new_tail = end - consumed;
        for b in &mut buf[new_tail..end] {
            *b = 0;
        }

        match self.splice_object_into_block(chain_blk, &new_bytes) {
            Ok(()) => Ok(()),
            Err(FilesystemError::DiskFull(_)) => {
                // Old block can't fit the longer object; place it in a
                // block in the parent's chain with room (allocating a new
                // OBJC if needed) and re-home the objectnode mapping.
                let (parent_dir_blk, parent_dir_obj_off) = self.locate_dir_object(parent_node)?;
                let target_blk = self.ensure_dir_chain_room(
                    parent_dir_blk,
                    parent_dir_obj_off,
                    parent_node,
                    new_bytes.len(),
                )?;
                self.splice_object_into_block(target_blk, &new_bytes)?;
                // Repoint the objectnode's NodeContainer entry at the new
                // OBJC block so future lookups find it.
                self.repoint_object_node(entry_node, target_blk)?;
                Ok(())
            }
            Err(e) => Err(e),
        }
    }

    /// Update the leaf NodeContainer entry for `objectnode` so its
    /// `fsObjectNode.data` points at `new_obj_blk`. Mirrors
    /// `alloc_object_node`'s slot math.
    fn repoint_object_node(
        &mut self,
        objectnode: u32,
        new_obj_blk: u32,
    ) -> Result<(), FilesystemError> {
        let nc_blk = self.root.objectnoderoot;
        self.ensure_dirty(nc_blk)?;
        let buf = self.dirty.get_mut(&nc_blk).unwrap();
        let nodenumber = rd_u32(buf, 12);
        let nodes = rd_u32(buf, 16);
        if nodes != 1 {
            return Err(parse_err(
                "repoint_object_node: only leaf NodeContainer supported",
            ));
        }
        if objectnode < nodenumber {
            return Err(parse_err(format!(
                "repoint_object_node: {} below nodenumber {}",
                objectnode, nodenumber
            )));
        }
        let slot = (objectnode - nodenumber) as usize;
        let o = 20 + slot * FS_OBJECTNODE_SIZE;
        if o + 4 > buf.len() {
            return Err(parse_err(format!(
                "repoint_object_node: slot {} out of range",
                slot
            )));
        }
        buf[o..o + 4].copy_from_slice(&new_obj_blk.to_be_bytes());
        Ok(())
    }

    fn do_delete_entry(
        &mut self,
        parent: &FileEntry,
        entry: &FileEntry,
    ) -> Result<(), FilesystemError> {
        let parent_node = parent.location as u32;
        let entry_node = entry.location as u32;
        // The entry's fsObject lives in the parent's OBJC chain (the
        // block that `alloc_object_node` recorded as
        // `fsObjectNode.data`). Look up that block directly.
        let chain_blk = self.lookup_object_block(entry_node)?;
        let cur = self.read_block(chain_blk)?.to_vec();
        // Find the fsObject for entry_node within chain_blk's entries.
        let mut off = 24usize;
        let mut found: Option<(usize, SfsObject)> = None;
        while off < cur.len() {
            match parse_object(&cur, off) {
                Some((obj, consumed)) => {
                    if obj.objectnode == entry_node {
                        found = Some((off, obj));
                        break;
                    }
                    off += consumed;
                }
                None => break,
            }
        }
        let (obj_off, obj) = found.ok_or_else(|| {
            parse_err(format!(
                "delete_entry: object {} not found in block {}",
                entry_node, chain_blk
            ))
        })?;
        // Re-parse to recover the byte length for the splice-out below.
        let consumed = parse_object(&cur, obj_off).map(|(_o, c)| c).unwrap();

        if obj.is_dir() {
            // Must be empty. With the simplified layout, a directory has
            // no child OBJCs (firstdirblock=0) and no separate hashtable
            // until kids are added; the only way it's non-empty is if
            // some other entry in the object-node tree claims `entry_node`
            // as its parent. Cheap check: scan every chain block for any
            // fsObject in some directory whose parent is the entry.
            // For our minimal implementation, the firstdirblock check
            // is sufficient because the only way a dir gains children
            // is by ensure_dir_chain_room allocating a new OBJC and
            // wiring it via firstdirblock.
            if obj.size_or_firstdirblock != 0 {
                let dir_first = obj.size_or_firstdirblock;
                let buf = self.read_block(dir_first)?.to_vec();
                if buf.len() > 24 && rd_u32(&buf, 24 + 4) != 0 {
                    return Err(parse_err(format!(
                        "directory '{}' is not empty",
                        entry.name
                    )));
                }
                // Free the (now-empty) firstdirblock OBJC.
                let _ = self.free_admin_block(dir_first);
            }
            if obj.data_or_hashtable != 0 {
                let _ = self.free_admin_block(obj.data_or_hashtable);
            }
        } else {
            // File: free data blocks + extent record.
            let bs = self.root.blocksize as u64;
            let size = obj.size_or_firstdirblock as u64;
            if obj.data_or_hashtable != 0 && size > 0 {
                let blocks = size.div_ceil(bs) as u32;
                self.free_data_blocks(obj.data_or_hashtable, blocks)?;
                let _ = self.extent_btree_remove(obj.data_or_hashtable);
            }
        }

        // Splice the fsObject out of the chain block.
        self.ensure_dirty(chain_blk)?;
        let buf = self.dirty.get_mut(&chain_blk).unwrap();
        let end = buf.len();
        buf.copy_within(obj_off + consumed..end, obj_off);
        let new_tail = end - consumed;
        for b in &mut buf[new_tail..end] {
            *b = 0;
        }

        self.free_object_node(entry_node)?;

        // If the chain block is now empty (no fsObjects) and it isn't
        // the parent's root OBJC, unlink + free it.
        let parent_root = if parent_node == ROOTNODE {
            self.root.rootobjectcontainer
        } else {
            self.lookup_object_block(parent_node)?
        };
        if chain_blk != parent_root {
            let post_buf = self.read_block(chain_blk)?.to_vec();
            let is_empty = post_buf.len() <= 24 || rd_u32(&post_buf, 24 + 4) == 0;
            if is_empty {
                self.unlink_chain_block(parent_node, parent_root, chain_blk)?;
                let _ = self.free_admin_block(chain_blk);
            }
        }
        Ok(())
    }

    /// Remove `chain_blk` from the directory's OBJC chain. The chain is
    /// rooted at the parent fsObject's `firstdirblock` field (or, for the
    /// root, at `self.root.rootobjectcontainer` whose chain head we don't
    /// rewrite). Walks the chain by `next` pointer and snips out the
    /// matching block.
    fn unlink_chain_block(
        &mut self,
        parent_node: u32,
        _parent_root: u32,
        chain_blk: u32,
    ) -> Result<(), FilesystemError> {
        // Find the parent's fsObject so we can read/update firstdirblock.
        let (parent_blk, parent_off) = self.locate_dir_object(parent_node)?;
        let firstdir = {
            let pb = self.read_block(parent_blk)?.to_vec();
            rd_u32(&pb, parent_off + 16)
        };
        if firstdir == 0 {
            return Ok(());
        }
        // Read the chain block's next/prev.
        let cb = self.read_block(chain_blk)?.to_vec();
        let next = rd_u32(&cb, 16);
        let prev = rd_u32(&cb, 20);
        if firstdir == chain_blk {
            // Update parent's firstdirblock to skip this one.
            self.ensure_dirty(parent_blk)?;
            let pb = self.dirty.get_mut(&parent_blk).unwrap();
            write_u32(pb, parent_off + 16, next);
        } else if prev != 0 {
            // Rewrite the previous block's next pointer.
            self.ensure_dirty(prev)?;
            let pb = self.dirty.get_mut(&prev).unwrap();
            write_u32(pb, 16, next);
        }
        if next != 0 {
            self.ensure_dirty(next)?;
            let nb = self.dirty.get_mut(&next).unwrap();
            write_u32(nb, 20, prev);
        }
        Ok(())
    }

    /// Append `obj_bytes` into the entries area of `blk`. Caller must have
    /// verified room via `ensure_dir_chain_room`.
    fn splice_object_into_block(
        &mut self,
        blk: u32,
        obj_bytes: &[u8],
    ) -> Result<(), FilesystemError> {
        self.ensure_dirty(blk)?;
        let buf = self.dirty.get_mut(&blk).unwrap();
        let bs = buf.len();
        // Find end of existing entries.
        let mut off = 24usize;
        while off < bs {
            match parse_object(buf, off) {
                Some((_obj, consumed)) => off += consumed,
                None => break,
            }
        }
        if off + obj_bytes.len() > bs {
            return Err(FilesystemError::DiskFull(
                "SFS: OBJC block out of room for new entry".into(),
            ));
        }
        buf[off..off + obj_bytes.len()].copy_from_slice(obj_bytes);
        Ok(())
    }

    fn do_sync_metadata(&mut self) -> Result<(), FilesystemError> {
        // Bump sequence number so the two root copies can be ordered.
        self.root.sequencenumber = self.root.sequencenumber.wrapping_add(1);

        let bs = self.root.blocksize as usize;
        let totalblocks = self.root.totalblocks;
        let primary_blk: u32 = 0;
        let backup_blk: u32 = totalblocks - 1;
        let mut prim = vec![0u8; bs];
        self.root.write_into(&mut prim);
        write_u32(&mut prim, 8, primary_blk);
        self.dirty.insert(primary_blk, prim);
        let mut backup = vec![0u8; bs];
        self.root.write_into(&mut backup);
        write_u32(&mut backup, 8, backup_blk);
        self.dirty.insert(backup_blk, backup);

        // Flush every dirty block. Metadata blocks (recognized by the
        // first 4 bytes matching a known SFS block id) get their
        // checksum re-stamped; data blocks are flushed verbatim because
        // they don't carry a block header.
        let mut keys: Vec<u32> = self.dirty.keys().copied().collect();
        keys.sort_unstable();
        let bs64 = bs as u64;
        for k in keys {
            let mut buf = self.dirty.remove(&k).unwrap();
            if is_metadata_block(&buf) {
                stamp_checksum(&mut buf);
            }
            let off = self.partition_offset + k as u64 * bs64;
            self.reader.seek(SeekFrom::Start(off))?;
            self.reader.write_all(&buf)?;
            self.cache.insert(k, buf);
        }
        self.reader.flush()?;
        Ok(())
    }
}

fn check_no_duplicate<R: Read + Seek + Send>(
    fs: &mut SfsFilesystem<R>,
    parent: &FileEntry,
    name: &str,
) -> Result<(), FilesystemError> {
    let kids = fs.list_directory(parent)?;
    if kids.iter().any(|c| c.name.eq_ignore_ascii_case(name)) {
        return Err(FilesystemError::AlreadyExists(name.to_string()));
    }
    Ok(())
}

impl<R: Read + Write + Seek + Send> super::filesystem::EditableFilesystem for SfsFilesystem<R> {
    fn create_file(
        &mut self,
        parent: &FileEntry,
        name: &str,
        data: &mut dyn std::io::Read,
        data_len: u64,
        _options: &super::filesystem::CreateFileOptions,
    ) -> Result<FileEntry, FilesystemError> {
        let snap = self.snapshot();
        match self.do_create_file(parent, name, data, data_len) {
            Ok(fe) => Ok(fe),
            Err(e) => {
                self.restore_snapshot(snap);
                Err(e)
            }
        }
    }

    fn create_directory(
        &mut self,
        parent: &FileEntry,
        name: &str,
        _options: &super::filesystem::CreateDirectoryOptions,
    ) -> Result<FileEntry, FilesystemError> {
        let snap = self.snapshot();
        match self.do_create_directory(parent, name) {
            Ok(fe) => Ok(fe),
            Err(e) => {
                self.restore_snapshot(snap);
                Err(e)
            }
        }
    }

    fn delete_entry(
        &mut self,
        parent: &FileEntry,
        entry: &FileEntry,
    ) -> Result<(), FilesystemError> {
        let snap = self.snapshot();
        match self.do_delete_entry(parent, entry) {
            Ok(()) => Ok(()),
            Err(e) => {
                self.restore_snapshot(snap);
                Err(e)
            }
        }
    }

    fn rename(
        &mut self,
        parent: &FileEntry,
        entry: &FileEntry,
        new_name: &str,
    ) -> Result<(), FilesystemError> {
        if new_name == entry.name {
            return Ok(());
        }
        let snap = self.snapshot();
        match self.do_rename(parent, entry, new_name) {
            Ok(()) => Ok(()),
            Err(e) => {
                self.restore_snapshot(snap);
                Err(e)
            }
        }
    }

    fn sync_metadata(&mut self) -> Result<(), FilesystemError> {
        self.do_sync_metadata()
    }

    fn free_space(&mut self) -> Result<u64, FilesystemError> {
        Ok(self.free_blocks_cached * self.root.blocksize as u64)
    }
}

// === Phase 8: formatter for a blank SFS volume =============================
//
// Produces a minimum mountable empty SFS volume so the write path has a
// known-good starting state for round-trip tests. Mirrors the layout
// `filesystemmain.c::ACTION_FORMAT` emits for a no-recycled disk:
//
//   block 0                       — root block (SFS\0)
//   block 1 (block_adminspace)    — AdminSpaceContainer (ADMC)
//   block 2 (block_root)          — root ObjectContainer (OBJC) holding the
//                                   root directory's fsObject
//   block 3                       — root HashTable (HTAB)
//   block 4                       — transaction-OK placeholder (TROK)
//   block 5 (block_extentbnoderoot) — empty extent B-tree leaf (BNDC)
//   block 6 (block_objectnoderoot)  — leaf NodeContainer (NDC ) with the
//                                     reserved object-node slots
//   blocks 33..(33+blocks_bitmap-1) — bitmap blocks (BTMP)
//   block totalblocks-1            — backup root block
//
// `blocks_admin = 32` reserves a 32-block admin region starting at block 1.
// The AdminSpaceContainer's `adminspace[0].bits = 0xFC000000` records the
// 6 already-allocated blocks (root OBJC, hashtable, transaction, extent
// btree leaf, object-node leaf) within that region. We don't emit the
// `.recycled` directory.
//
// **Checksum convention.** Every metadata block ends with
// `sum_all_longs.wrapping_add(1) == 0`. We compute by zeroing the
// checksum slot at offset 4 first, summing the rest, then storing
// `0u32.wrapping_sub(sum).wrapping_sub(1)`.

const SFS_BLOCKS_RESERVED_START: u32 = 1;
const SFS_BLOCKS_RESERVED_END: u32 = 1;
const SFS_BLOCKS_ADMIN: u32 = 32;
/// AdminSpace bits showing the first six blocks of the admin region as
/// allocated: ADMC, root OBJC, root HTAB, TROK, BNDC, NDC.
const SFS_ADMIN_INITIAL_BITS: u32 = 0xFC000000;

const FIBF_READ: u32 = 1 << 3;
const FIBF_WRITE: u32 = 1 << 2;
const FIBF_EXECUTE: u32 = 1 << 1;
const FIBF_DELETE: u32 = 1 << 0;

fn write_u16(buf: &mut [u8], o: usize, v: u16) {
    buf[o..o + 2].copy_from_slice(&v.to_be_bytes());
}
fn write_u32(buf: &mut [u8], o: usize, v: u32) {
    buf[o..o + 4].copy_from_slice(&v.to_be_bytes());
}

/// Compute and stamp the SFS block checksum. The checksum slot at offset 4
/// is zeroed first; the function then walks every u32 word in the block
/// and stores `-(sum + 1)` at offset 4 so a later `sum.wrapping_add(1)`
/// over all longs yields 0 (matches `CALCCHECKSUM` in
/// `SFSsaveextents/asmsupport.s`).
/// True if the buffer's first 4 bytes match a known SFS metadata-block
/// id, i.e. the block carries an `fsBlockHeader` with a checksum we
/// need to maintain. Data blocks (allocated from the BTMP bitmap) carry
/// no header and return false.
fn is_metadata_block(buf: &[u8]) -> bool {
    if buf.len() < 4 {
        return false;
    }
    let id = rd_u32(buf, 0);
    matches!(
        id,
        ROOTBLOCK_ID
            | OBJECTCONTAINER_ID
            | NODECONTAINER_ID
            | BNODECONTAINER_ID
            | BITMAP_ID
            | HASHTABLE_ID
            | SOFTLINK_ID
    ) || id == u32::from_be_bytes(*b"ADMC")
        || id == u32::from_be_bytes(*b"TROK")
        || id == u32::from_be_bytes(*b"TRST")
        || id == u32::from_be_bytes(*b"TRFA")
}

fn stamp_checksum(buf: &mut [u8]) {
    write_u32(buf, 4, 0);
    let mut sum: u32 = 0;
    for o in (0..buf.len()).step_by(4) {
        sum = sum.wrapping_add(rd_u32(buf, o));
    }
    let chk = 0u32.wrapping_sub(sum).wrapping_sub(1);
    write_u32(buf, 4, chk);
}

/// Write one fsObject into `buf[off..]`. Returns the number of bytes
/// consumed (aligned even). Layout matches `parse_object`.
fn encode_object(
    buf: &mut [u8],
    off: usize,
    objectnode: u32,
    protection: u32,
    data_or_ht: u32,
    size_or_fdb: u32,
    datemodified: u32,
    bits: u8,
    name: &str,
    comment: &str,
) -> usize {
    let name_bytes = name.as_bytes();
    let cmt_bytes = comment.as_bytes();
    let header_len = 25;
    let unaligned = header_len + name_bytes.len() + 1 + cmt_bytes.len() + 1;
    let aligned = (unaligned + 1) & !1;
    // owneruid + ownergid at off..off+4 stay 0.
    write_u32(buf, off + 4, objectnode);
    write_u32(buf, off + 8, protection);
    write_u32(buf, off + 12, data_or_ht);
    write_u32(buf, off + 16, size_or_fdb);
    write_u32(buf, off + 20, datemodified);
    buf[off + 24] = bits;
    let name_start = off + 25;
    buf[name_start..name_start + name_bytes.len()].copy_from_slice(name_bytes);
    buf[name_start + name_bytes.len()] = 0;
    let cmt_start = name_start + name_bytes.len() + 1;
    buf[cmt_start..cmt_start + cmt_bytes.len()].copy_from_slice(cmt_bytes);
    buf[cmt_start + cmt_bytes.len()] = 0;
    aligned
}

/// Create a blank SFS volume of `total_blocks` 512-byte blocks. The volume
/// mounts with diskname `name` (truncated to 30 bytes) and no `.recycled`
/// directory.
pub fn create_blank_sfs(total_blocks: u32, name: &str) -> Result<Vec<u8>, FilesystemError> {
    let blocksize: u32 = 512;
    if total_blocks < 64 {
        return Err(parse_err(format!(
            "SFS formatter: total_blocks {} too small (need >= 64)",
            total_blocks
        )));
    }
    let blocks_inbitmap = (blocksize - 12) * 8;
    let blocks_bitmap = total_blocks.div_ceil(blocks_inbitmap);
    let blocks_admin = SFS_BLOCKS_ADMIN;
    let reserved_start = SFS_BLOCKS_RESERVED_START;
    let reserved_end = SFS_BLOCKS_RESERVED_END;

    let block_adminspace = reserved_start;
    let block_root = reserved_start + 1;
    let block_hashtable = block_root + 1;
    let block_transaction = block_root + 2;
    let block_extentbnoderoot = block_root + 3;
    let block_objectnoderoot = block_root + 4;
    let block_bitmapbase = block_adminspace + blocks_admin;

    if block_bitmapbase + blocks_bitmap + reserved_end > total_blocks {
        return Err(parse_err(
            "SFS formatter: layout doesn't fit (admin+bitmap+reserved > total)".to_string(),
        ));
    }

    let total_size = total_blocks as usize * blocksize as usize;
    let mut img = vec![0u8; total_size];
    let bs = blocksize as usize;
    let block_off = |b: u32| -> usize { b as usize * bs };

    // === Root block (block 0 + duplicated at totalblocks-1) ===
    let mut rb_buf = vec![0u8; bs];
    write_u32(&mut rb_buf, 0, ROOTBLOCK_ID);
    // checksum at 4 stamped below
    write_u32(&mut rb_buf, 8, 0); // ownblock = 0 for the primary root
    write_u16(&mut rb_buf, 12, STRUCTURE_VERSION);
    write_u16(&mut rb_buf, 14, 0); // sequencenumber
    write_u32(&mut rb_buf, 16, 0); // datecreated
                                   // bits @ 20: ROOTBITS_CASESENSITIVE=0, ROOTBITS_RECYCLED=0 (no .recycled).
                                   // pad1 @ 21, pad2 @ 22, reserved1 @ 24..32 stay zero.
                                   // firstbyteh/firstbyte at 32/36: 0 (single-partition file image)
                                   // lastbyteh/lastbyte at 40/44: last byte offset of partition
    let last_byte = (total_blocks as u64 * blocksize as u64).saturating_sub(1);
    write_u32(&mut rb_buf, 40, (last_byte >> 32) as u32);
    write_u32(&mut rb_buf, 44, last_byte as u32);
    write_u32(&mut rb_buf, 48, total_blocks);
    write_u32(&mut rb_buf, 52, blocksize);
    // reserved2 (8) at 56, reserved3 (32) at 64.
    write_u32(&mut rb_buf, 96, block_bitmapbase);
    write_u32(&mut rb_buf, 100, block_adminspace);
    write_u32(&mut rb_buf, 104, block_root);
    write_u32(&mut rb_buf, 108, block_extentbnoderoot);
    write_u32(&mut rb_buf, 112, block_objectnoderoot);
    stamp_checksum(&mut rb_buf);
    img[block_off(0)..block_off(0) + bs].copy_from_slice(&rb_buf);

    // Backup root at totalblocks-1: identical except ownblock.
    let mut rb2 = rb_buf.clone();
    write_u32(&mut rb2, 8, total_blocks - 1);
    stamp_checksum(&mut rb2);
    let backup_off = block_off(total_blocks - 1);
    img[backup_off..backup_off + bs].copy_from_slice(&rb2);

    // === AdminSpaceContainer at block 1 ===
    {
        let off = block_off(block_adminspace);
        let blk = &mut img[off..off + bs];
        write_u32(blk, 0, u32::from_be_bytes(*b"ADMC"));
        write_u32(blk, 8, block_adminspace); // ownblock
                                             // next/previous BLCK at 12/16: 0
                                             // bits at 24 = blocks_admin (per filesystemmain.c:874)
        blk[24] = blocks_admin as u8;
        // adminspace[0]: space at 28..32, bits at 32..36
        write_u32(blk, 28, block_adminspace);
        write_u32(blk, 32, SFS_ADMIN_INITIAL_BITS);
        stamp_checksum(blk);
    }

    // === Root ObjectContainer at block 2 ===
    let datecreated = 0u32;
    {
        let off = block_off(block_root);
        let blk = &mut img[off..off + bs];
        write_u32(blk, 0, OBJECTCONTAINER_ID);
        write_u32(blk, 8, block_root); // ownblock
                                       // parent NODE at 12: 0 (root has no parent)
                                       // next/previous BLCK at 16/20: 0
        let object_off = 24usize;
        encode_object(
            blk,
            object_off,
            ROOTNODE,
            FIBF_READ | FIBF_WRITE | FIBF_EXECUTE | FIBF_DELETE,
            block_hashtable,
            0,
            datecreated,
            OTYPE_DIR,
            name,
            "",
        );
        // The trailing bytes of an OBJC carry the fsRootInfo block at the
        // very end (last sizeof(fsRootInfo) bytes of the block). Initialize
        // it so future format-parity tools can find it.
        let ri_size = 32usize; // fsRootInfo: 4*5 = 20 + 12 reserved = 32-ish
        let ri_off = bs - ri_size;
        // freeblocks at fsRootInfo offset 8 (after deletedblocks+deletedfiles)
        let freeblocks =
            total_blocks - blocks_admin - reserved_start - reserved_end - blocks_bitmap;
        write_u32(blk, ri_off + 8, freeblocks);
        write_u32(blk, ri_off + 12, datecreated);
        stamp_checksum(blk);
    }

    // === Root HashTable at block 3 ===
    {
        let off = block_off(block_hashtable);
        let blk = &mut img[off..off + bs];
        write_u32(blk, 0, HASHTABLE_ID);
        write_u32(blk, 8, block_hashtable);
        write_u32(blk, 12, ROOTNODE); // parent NODE
                                      // hashentry[] all zero — empty.
        stamp_checksum(blk);
    }

    // === Transaction-OK placeholder at block 4 ===
    {
        let off = block_off(block_transaction);
        let blk = &mut img[off..off + bs];
        // TRANSACTIONOK_ID = MAKE_ID('T','R','O','K')
        write_u32(blk, 0, u32::from_be_bytes(*b"TROK"));
        write_u32(blk, 8, block_transaction);
        stamp_checksum(blk);
    }

    // === Extent B-tree root (empty leaf) at block 5 ===
    {
        let off = block_off(block_extentbnoderoot);
        let blk = &mut img[off..off + bs];
        write_u32(blk, 0, BNODECONTAINER_ID);
        write_u32(blk, 8, block_extentbnoderoot);
        // BTreeContainer at offset 12: nodecount(u16)=0, isleaf(u8)=1,
        // nodesize(u8)=sizeof(fsExtentBNode)=14
        write_u16(blk, 12, 0);
        blk[14] = 1; // isleaf
        blk[15] = 14; // nodesize
        stamp_checksum(blk);
    }

    // === Object-node root NodeContainer (leaf) at block 6 ===
    {
        let off = block_off(block_objectnoderoot);
        let blk = &mut img[off..off + bs];
        write_u32(blk, 0, NODECONTAINER_ID);
        write_u32(blk, 8, block_objectnoderoot);
        write_u32(blk, 12, 1); // nodenumber: objectnode 0 is reserved
        write_u32(blk, 16, 1); // nodes==1 → leaf NodeContainer
                               // node[0] is fsObjectNode (10 bytes): data BLCK + next + hash16.
                               // ObjectNode 1 = root → data = block_root, hash16/next zero.
        write_u32(blk, 20, block_root);
        // Slots 2..6 reserved (filesystemmain.c writes data=-1 for reserved
        // slots, so we emit 0xFFFFFFFF too).
        for slot in 1..6 {
            write_u32(blk, 20 + slot * 10, 0xFFFF_FFFF);
        }
        stamp_checksum(blk);
    }

    // === Bitmap blocks at blocks 33..(33+blocks_bitmap-1) ===
    // The bitmap covers all `total_blocks`. Bits 1 = free, 0 = allocated.
    // Allocated regions: blocks 0..reserved_start, the admin region,
    // the bitmap region itself, and the trailing reserved-end block.
    {
        let mut alloc = vec![false; total_blocks as usize];
        for b in 0..reserved_start {
            alloc[b as usize] = true;
        }
        for b in block_adminspace..block_adminspace + blocks_admin {
            alloc[b as usize] = true;
        }
        for b in block_bitmapbase..block_bitmapbase + blocks_bitmap {
            alloc[b as usize] = true;
        }
        for b in (total_blocks - reserved_end)..total_blocks {
            alloc[b as usize] = true;
        }
        for bm_idx in 0..blocks_bitmap {
            let off = block_off(block_bitmapbase + bm_idx);
            let blk = &mut img[off..off + bs];
            write_u32(blk, 0, BITMAP_ID);
            write_u32(blk, 8, block_bitmapbase + bm_idx);
            // bitmap[] at offset 12. words_per_block = (bs-12)/4.
            let words_per_block = (bs - 12) / 4;
            let bits_per_block = words_per_block * 32;
            let base_block = bm_idx as usize * bits_per_block;
            for w in 0..words_per_block {
                let mut word: u32 = 0;
                for i in 0..32 {
                    let global_blk = base_block + w * 32 + i;
                    if global_blk >= total_blocks as usize {
                        break;
                    }
                    if !alloc[global_blk] {
                        word |= 1u32 << (31 - i as u32);
                    }
                }
                write_u32(blk, 12 + w * 4, word);
            }
            stamp_checksum(blk);
        }
    }

    Ok(img)
}

/// In-place SFS resize. Operates directly on the underlying device/file at
/// raw-I/O level, matching the convention used by `resize_hfs_in_place` and
/// friends. Auto-detects by checking the rootblock id at the partition
/// offset and silently skips non-SFS volumes.
///
/// Phase S.1 implements shrink-to-floor only: the new size must be at least
/// `last_data_byte()` (as reported by the inspect-tab minimum-size column).
/// Shrinking below that floor would require relocating user-data blocks,
/// which is deferred indefinitely as a design choice — see
/// `docs/amiga_resize.md` Phase S.3. Grow is implemented in Phase S.2.
///
/// Mechanics for shrink:
///   1. Verify the block at `new_total - 1` is free (so the relocated
///      backup rootblock can land there).
///   2. Mark that block allocated in the bitmap.
///   3. Update primary rootblock fields (`totalblocks`, `lastbyte_full`,
///      bump `sequencenumber`).
///   4. Write the new backup rootblock at the new tail.
///   5. Write the updated primary rootblock at block 0.
///
/// The block at the OLD `totalblocks - 1` (former backup root) sits past
/// the new volume tail after step 3 and is no longer relevant — the
/// underlying bytes are left untouched. The partition table is responsible
/// for trimming/extending the actual partition extent; this function only
/// rewrites filesystem metadata.
pub fn resize_sfs_in_place(
    device: &mut (impl Read + Write + Seek),
    partition_offset: u64,
    new_size_bytes: u64,
    log: &mut impl FnMut(&str),
) -> anyhow::Result<()> {
    // --- 1. Probe rootblock at block 0 ---
    device.seek(SeekFrom::Start(partition_offset))?;
    let mut probe = [0u8; 512];
    if device.read_exact(&mut probe).is_err() {
        return Ok(());
    }
    if rd_u32(&probe, 0) != ROOTBLOCK_ID {
        return Ok(());
    }
    let blocksize = rd_u32(&probe, 52) as u64;
    if !(512..=65536).contains(&blocksize) || !blocksize.is_multiple_of(512) {
        log(&format!(
            "SFS resize: rootblock blocksize {} out of range, skipping",
            blocksize
        ));
        return Ok(());
    }

    // Re-read at proper block size.
    let mut prim_buf = vec![0u8; blocksize as usize];
    device.seek(SeekFrom::Start(partition_offset))?;
    device.read_exact(&mut prim_buf)?;
    let root = SfsRootBlock::parse(&prim_buf, 0)
        .map_err(|e| anyhow::anyhow!("SFS resize: rootblock parse failed: {e}"))?;

    let old_total = root.totalblocks;
    let new_total_u64 = new_size_bytes / blocksize;
    if new_total_u64 == 0 || new_total_u64 > u32::MAX as u64 {
        anyhow::bail!(
            "SFS resize: target {} bytes resolves to {} blocks, out of u32 range",
            new_size_bytes,
            new_total_u64,
        );
    }
    let new_total = new_total_u64 as u32;
    if new_total == old_total {
        log("SFS resize: no size change, skipping");
        return Ok(());
    }
    if new_total > old_total {
        return grow_sfs_in_place(device, partition_offset, &root, new_total, log);
    }

    // --- 2. Shrink path. Sanity-check minimums. ---
    if new_total < SFS_BLOCKS_RESERVED_START + SFS_BLOCKS_ADMIN + 1 + SFS_BLOCKS_RESERVED_END {
        anyhow::bail!(
            "SFS resize: target {} blocks below the metadata-region minimum",
            new_total,
        );
    }
    let new_backup_blk = new_total - 1;

    // Read the full bitmap to verify the floor invariant: every allocated
    // block must lie at index < new_backup_blk, except the OLD backup root
    // at old_total - 1 which we ignore (it sits past the new tail).
    let bm = read_bitmap_raw(device, partition_offset, &root)?;
    let highest_alloc = find_highest_allocated_excluding_old_backup(&bm, old_total);
    if let Some(h) = highest_alloc {
        if h >= new_backup_blk {
            anyhow::bail!(
                "SFS resize: highest allocated user block {} >= new backup root position {}, \
                 cannot shrink to {} blocks. Shrink to at least {} blocks (~{} bytes), or use \
                 a larger target.",
                h,
                new_backup_blk,
                new_total,
                h + 1 + SFS_BLOCKS_RESERVED_END,
                (h as u64 + 1 + SFS_BLOCKS_RESERVED_END as u64) * blocksize,
            );
        }
    }

    // --- 3. Mark new backup-root block allocated in bitmap on disk. ---
    set_bitmap_bit_on_disk(
        device,
        partition_offset,
        &root,
        new_backup_blk,
        /*set_free=*/ false,
    )?;

    // --- 4. Build the updated rootblock (primary + backup) and stamp. ---
    let mut new_root = root.clone();
    new_root.totalblocks = new_total;
    new_root.lastbyte_full = new_total as u64 * blocksize - 1;
    new_root.sequencenumber = new_root.sequencenumber.wrapping_add(1);

    let bs = blocksize as usize;
    let mut prim_out = vec![0u8; bs];
    new_root.write_into(&mut prim_out);
    write_u32(&mut prim_out, 8, 0);
    stamp_checksum(&mut prim_out);

    let mut backup_out = vec![0u8; bs];
    new_root.write_into(&mut backup_out);
    write_u32(&mut backup_out, 8, new_backup_blk);
    stamp_checksum(&mut backup_out);

    // Write backup first, primary last — the primary is the commit point.
    device.seek(SeekFrom::Start(
        partition_offset + new_backup_blk as u64 * blocksize,
    ))?;
    device.write_all(&backup_out)?;
    device.seek(SeekFrom::Start(partition_offset))?;
    device.write_all(&prim_out)?;
    device.flush()?;

    log(&format!(
        "SFS resize: shrunk from {} to {} blocks ({} -> {} bytes), backup root relocated to {}",
        old_total,
        new_total,
        old_total as u64 * blocksize,
        new_total as u64 * blocksize,
        new_backup_blk,
    ));
    Ok(())
}

/// Read the SFS bitmap directly via raw I/O (no SfsFilesystem instance).
/// Returns one bit per block in `[0..root.totalblocks)`, set = free.
fn read_bitmap_raw(
    device: &mut (impl Read + Seek),
    partition_offset: u64,
    root: &SfsRootBlock,
) -> anyhow::Result<Vec<u8>> {
    let bs = root.blocksize as u64;
    let total = root.totalblocks as usize;
    let total_bytes = total.div_ceil(8);
    let mut out = vec![0u8; total_bytes];
    let words_per_block = ((bs as usize) - 12) / 4;
    let bits_per_block = words_per_block * 32;
    let needed = total.div_ceil(bits_per_block);
    let mut buf = vec![0u8; bs as usize];
    let mut covered: usize = 0;
    for i in 0..needed {
        let blk = root.bitmapbase + i as u32;
        device.seek(SeekFrom::Start(partition_offset + blk as u64 * bs))?;
        device.read_exact(&mut buf)?;
        if rd_u32(&buf, 0) != BITMAP_ID {
            anyhow::bail!("SFS resize: bitmap block {} has bad id", blk);
        }
        for w in 0..words_per_block {
            let word = rd_u32(&buf, 12 + w * 4);
            for i in 0..32u32 {
                let global_blk = covered + (w * 32) + i as usize;
                if global_blk >= total {
                    break;
                }
                if (word >> (31 - i)) & 1 == 1 {
                    out[global_blk / 8] |= 1u8 << (global_blk % 8);
                }
            }
        }
        covered += bits_per_block;
        if covered >= total {
            break;
        }
    }
    Ok(out)
}

/// Find the highest allocated block (bit clear) in the bitmap, ignoring
/// the backup-root region at `[old_total - SFS_BLOCKS_RESERVED_END, old_total)`.
fn find_highest_allocated_excluding_old_backup(bm: &[u8], old_total: u32) -> Option<u32> {
    let cutoff = old_total.saturating_sub(SFS_BLOCKS_RESERVED_END);
    for blk in (0..cutoff).rev() {
        let byte = bm[(blk / 8) as usize];
        let bit = (byte >> (blk % 8)) & 1;
        if bit == 0 {
            return Some(blk);
        }
    }
    None
}

/// Grow path. Symmetric to shrink: free the old backup-root block, free
/// the newly-claimed tail range, allocate the new backup-root position,
/// write the new backup root, then commit by writing the primary.
///
/// Refuses to grow past the capacity of the existing bitmap chain — i.e.
/// when adding more bitmap blocks would be required. Extending the bitmap
/// chain itself means relocating whatever currently sits at
/// `bitmapbase + old_blocks_bitmap`, which is user data on any non-blank
/// volume and falls into Phase S.3 territory.
fn grow_sfs_in_place(
    device: &mut (impl Read + Write + Seek),
    partition_offset: u64,
    root: &SfsRootBlock,
    new_total: u32,
    log: &mut impl FnMut(&str),
) -> anyhow::Result<()> {
    let blocksize = root.blocksize as u64;
    let old_total = root.totalblocks;
    let bs = blocksize as usize;
    let words_per_block = (bs - 12) / 4;
    let bits_per_block = words_per_block * 32;
    let old_blocks_bitmap = (old_total as usize).div_ceil(bits_per_block);
    let new_blocks_bitmap = (new_total as usize).div_ceil(bits_per_block);
    if new_blocks_bitmap != old_blocks_bitmap {
        anyhow::bail!(
            "SFS resize: growing from {} to {} blocks needs {} bitmap blocks (have {}). \
             Adding bitmap blocks requires relocating user data and is not implemented \
             (Phase S.3). Maximum grow without bitmap extension: {} blocks (~{} bytes).",
            old_total,
            new_total,
            new_blocks_bitmap,
            old_blocks_bitmap,
            old_blocks_bitmap * bits_per_block,
            old_blocks_bitmap as u64 * bits_per_block as u64 * blocksize,
        );
    }

    let old_backup_blk = old_total - 1;
    let new_backup_blk = new_total - 1;
    if new_backup_blk == old_backup_blk {
        // Equal totals were handled by the caller; this would imply
        // new_total == old_total. Defensive guard.
        log("SFS resize: grow no-op, skipping");
        return Ok(());
    }

    // Mark bits free for: old_backup_blk + every block in [old_total, new_total - 1).
    // Mark bit allocated for: new_backup_blk.
    set_bitmap_range_on_disk(device, partition_offset, root, old_backup_blk, 1, true)?;
    if new_total - 1 > old_total {
        // Range [old_total, new_total - 1) is newly visible — bits there are
        // currently 0 (allocated) because the formatter zero-fills beyond
        // total_blocks. Mark them free.
        set_bitmap_range_on_disk(
            device,
            partition_offset,
            root,
            old_total,
            new_backup_blk - old_total,
            true,
        )?;
    }
    set_bitmap_range_on_disk(device, partition_offset, root, new_backup_blk, 1, false)?;

    // Update root copies and commit.
    let mut new_root = root.clone();
    new_root.totalblocks = new_total;
    new_root.lastbyte_full = new_total as u64 * blocksize - 1;
    new_root.sequencenumber = new_root.sequencenumber.wrapping_add(1);

    let mut prim_out = vec![0u8; bs];
    new_root.write_into(&mut prim_out);
    write_u32(&mut prim_out, 8, 0);
    stamp_checksum(&mut prim_out);

    let mut backup_out = vec![0u8; bs];
    new_root.write_into(&mut backup_out);
    write_u32(&mut backup_out, 8, new_backup_blk);
    stamp_checksum(&mut backup_out);

    // Zero any garbage at the new backup-root position before stamping the
    // backup, so leftover bytes from before the grow don't leak into the
    // checksum. (The position was free in the new bitmap before this op.)
    device.seek(SeekFrom::Start(
        partition_offset + new_backup_blk as u64 * blocksize,
    ))?;
    device.write_all(&backup_out)?;
    device.seek(SeekFrom::Start(partition_offset))?;
    device.write_all(&prim_out)?;
    device.flush()?;

    log(&format!(
        "SFS resize: grew from {} to {} blocks ({} -> {} bytes), backup root relocated to {}",
        old_total,
        new_total,
        old_total as u64 * blocksize,
        new_total as u64 * blocksize,
        new_backup_blk,
    ));
    Ok(())
}

/// Toggle a contiguous range of `count` bitmap bits starting at block
/// `first`. Batches per bitmap-block: reads each affected bitmap block
/// once, applies all bit edits in that block, writes it back with a
/// fresh checksum.
fn set_bitmap_range_on_disk(
    device: &mut (impl Read + Write + Seek),
    partition_offset: u64,
    root: &SfsRootBlock,
    first: u32,
    count: u32,
    set_free: bool,
) -> anyhow::Result<()> {
    if count == 0 {
        return Ok(());
    }
    let bs = root.blocksize as u64;
    let words_per_block = ((bs as usize) - 12) / 4;
    let bits_per_block = words_per_block * 32;
    let last = first
        .checked_add(count - 1)
        .ok_or_else(|| anyhow::anyhow!("SFS resize: bitmap range overflow"))?;
    let first_bm_seq = (first as usize) / bits_per_block;
    let last_bm_seq = (last as usize) / bits_per_block;
    let mut buf = vec![0u8; bs as usize];
    for bm_seq in first_bm_seq..=last_bm_seq {
        let bm_blk = root.bitmapbase + bm_seq as u32;
        let bm_off = partition_offset + bm_blk as u64 * bs;
        device.seek(SeekFrom::Start(bm_off))?;
        device.read_exact(&mut buf)?;
        if rd_u32(&buf, 0) != BITMAP_ID {
            anyhow::bail!("SFS resize: bitmap block {} has bad id", bm_blk);
        }
        let range_start = (bm_seq * bits_per_block) as u32;
        let range_end = range_start.saturating_add(bits_per_block as u32);
        let lo = first.max(range_start);
        let hi = (last + 1).min(range_end);
        for blk in lo..hi {
            let within = (blk as usize) - bm_seq * bits_per_block;
            let word_idx = within / 32;
            let bit = within % 32;
            let o = 12 + word_idx * 4;
            let mut word = rd_u32(&buf, o);
            let mask = 1u32 << (31 - bit as u32);
            if set_free {
                word |= mask;
            } else {
                word &= !mask;
            }
            buf[o..o + 4].copy_from_slice(&word.to_be_bytes());
        }
        stamp_checksum(&mut buf);
        device.seek(SeekFrom::Start(bm_off))?;
        device.write_all(&buf)?;
    }
    Ok(())
}

/// Toggle a single bitmap bit on disk via raw I/O. Read-modify-write on
/// the affected bitmap block, then re-stamp its checksum.
fn set_bitmap_bit_on_disk(
    device: &mut (impl Read + Write + Seek),
    partition_offset: u64,
    root: &SfsRootBlock,
    blk: u32,
    set_free: bool,
) -> anyhow::Result<()> {
    let bs = root.blocksize as u64;
    let words_per_block = ((bs as usize) - 12) / 4;
    let bits_per_block = words_per_block * 32;
    let bm_seq = (blk as usize) / bits_per_block;
    let within = (blk as usize) % bits_per_block;
    let word_idx = within / 32;
    let bit = within % 32;
    let bm_blk = root.bitmapbase + bm_seq as u32;

    let mut buf = vec![0u8; bs as usize];
    device.seek(SeekFrom::Start(partition_offset + bm_blk as u64 * bs))?;
    device.read_exact(&mut buf)?;
    if rd_u32(&buf, 0) != BITMAP_ID {
        anyhow::bail!("SFS resize: bitmap block {} has bad id", bm_blk);
    }
    let o = 12 + word_idx * 4;
    let mut word = rd_u32(&buf, o);
    let mask = 1u32 << (31 - bit as u32);
    if set_free {
        word |= mask;
    } else {
        word &= !mask;
    }
    buf[o..o + 4].copy_from_slice(&word.to_be_bytes());
    stamp_checksum(&mut buf);
    device.seek(SeekFrom::Start(partition_offset + bm_blk as u64 * bs))?;
    device.write_all(&buf)?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn id_constants_are_correct() {
        assert_eq!(ROOTBLOCK_ID, 0x53465300);
        assert_eq!(OBJECTCONTAINER_ID, 0x4F424A43);
        assert_eq!(BITMAP_ID, 0x42544D50);
        assert_eq!(BNODECONTAINER_ID, 0x424E4443);
        assert_eq!(NODECONTAINER_ID, 0x4E444320);
    }

    #[test]
    fn blank_sfs_round_trips_through_reader() {
        // 4 MiB blank volume: 8192 blocks @ 512 bytes.
        let img = create_blank_sfs(8192, "BlankSFS").expect("format");
        assert_eq!(img.len(), 8192 * 512);
        let cur = std::io::Cursor::new(img);
        let mut fs = SfsFilesystem::open(cur, 0).expect("open");
        let root = fs.root().expect("root");
        let kids = fs.list_directory(&root).expect("list");
        assert!(kids.is_empty(), "blank SFS root should be empty");
        assert_eq!(fs.total_size(), 8192 * 512);
        // free_blocks_cached is computed at open via read_bitmap.
        let used = fs.used_size();
        let free = fs.total_size() - used;
        // Sanity: free should be at least the data area we left unallocated.
        // total - admin(32) - bitmap(N) - reserved_start(1) - reserved_end(1)
        // = 8192 - 32 - 1 - 1 - 1 = 8157 (one BM block covers all 8192 bits).
        assert!(free >= 8000 * 512);
    }

    #[test]
    fn sfs_write_round_trip_create_dir_and_file() {
        use super::super::filesystem::{
            CreateDirectoryOptions, CreateFileOptions, EditableFilesystem,
        };
        let img = create_blank_sfs(8192, "WriteTest").expect("format");
        let cur = std::io::Cursor::new(img);
        let mut fs = SfsFilesystem::open(cur, 0).expect("open");
        let root = fs.root().expect("root");

        let dir_fe = fs
            .create_directory(&root, "MyDir", &CreateDirectoryOptions::default())
            .expect("create_directory");
        assert!(dir_fe.is_directory());

        let payload: &[u8] = b"hello SFS write\n";
        let mut p = std::io::Cursor::new(payload);
        let file_fe = fs
            .create_file(
                &root,
                "readme.txt",
                &mut p,
                payload.len() as u64,
                &CreateFileOptions::default(),
            )
            .expect("create_file");
        assert_eq!(file_fe.size, payload.len() as u64);

        EditableFilesystem::sync_metadata(&mut fs).expect("sync");

        // Reopen the mutated image and verify.
        let img2 = fs.reader.into_inner();
        let cur2 = std::io::Cursor::new(img2);
        let mut fs2 = SfsFilesystem::open(cur2, 0).expect("reopen");
        let root2 = fs2.root().expect("root2");
        let kids = fs2.list_directory(&root2).expect("list");
        assert_eq!(kids.len(), 2, "expected 2 children, got {:?}", kids);
        let names: Vec<&str> = kids.iter().map(|c| c.name.as_str()).collect();
        assert!(names.contains(&"MyDir"));
        assert!(names.contains(&"readme.txt"));

        let f = kids.iter().find(|c| c.name == "readme.txt").unwrap();
        let data = fs2.read_file(f, payload.len() * 2).expect("read");
        assert_eq!(data, payload);
        let d = kids.iter().find(|c| c.name == "MyDir").unwrap();
        let sub_kids = fs2.list_directory(d).expect("list dir");
        assert!(sub_kids.is_empty());
    }

    /// `rename` round-trips: the new name is listed, the old is gone, and
    /// the entry keeps its objectnode (identity) plus its file contents. A
    /// different-length new name exercises the splice-out + re-place path.
    #[test]
    fn sfs_rename_file_round_trips() {
        use super::super::filesystem::{CreateFileOptions, EditableFilesystem};
        let img = create_blank_sfs(8192, "Ren").expect("format");
        let cur = std::io::Cursor::new(img);
        let mut fs = SfsFilesystem::open(cur, 0).expect("open");
        let root = fs.root().expect("root");

        let payload: &[u8] = b"keep this content";
        let mut p = std::io::Cursor::new(payload);
        let file_fe = fs
            .create_file(
                &root,
                "old.txt",
                &mut p,
                payload.len() as u64,
                &CreateFileOptions::default(),
            )
            .expect("create_file");
        let orig_node = file_fe.location;
        fs.rename(&root, &file_fe, "much-longer-name.txt")
            .expect("rename");
        EditableFilesystem::sync_metadata(&mut fs).expect("sync");

        let img2 = fs.reader.into_inner();
        let cur2 = std::io::Cursor::new(img2);
        let mut fs2 = SfsFilesystem::open(cur2, 0).expect("reopen");
        let root2 = fs2.root().expect("root");
        let kids = fs2.list_directory(&root2).expect("list");
        assert!(kids.iter().all(|c| c.name != "old.txt"), "old name gone");
        let renamed = kids
            .iter()
            .find(|c| c.name == "much-longer-name.txt")
            .expect("new name listed");
        assert_eq!(renamed.location, orig_node, "objectnode preserved");
        let data = fs2.read_file(renamed, payload.len() * 2).expect("read");
        assert_eq!(data, payload, "contents preserved");
    }

    #[test]
    fn sfs_write_round_trip_delete_entry() {
        use super::super::filesystem::{
            CreateDirectoryOptions, CreateFileOptions, EditableFilesystem,
        };
        let img = create_blank_sfs(8192, "X").expect("format");
        let cur = std::io::Cursor::new(img);
        let mut fs = SfsFilesystem::open(cur, 0).expect("open");
        let root = fs.root().expect("root");

        let payload: &[u8] = b"to be deleted";
        let mut p = std::io::Cursor::new(payload);
        let file_fe = fs
            .create_file(
                &root,
                "tmp.txt",
                &mut p,
                payload.len() as u64,
                &CreateFileOptions::default(),
            )
            .expect("create_file");
        let dir_fe = fs
            .create_directory(&root, "doomed", &CreateDirectoryOptions::default())
            .expect("create_directory");
        EditableFilesystem::sync_metadata(&mut fs).expect("sync after create");
        let free_after_create = EditableFilesystem::free_space(&mut fs).expect("free");

        let root_again = fs.root().expect("root");
        EditableFilesystem::delete_entry(&mut fs, &root_again, &file_fe).expect("del file");
        EditableFilesystem::delete_entry(&mut fs, &root_again, &dir_fe).expect("del dir");
        EditableFilesystem::sync_metadata(&mut fs).expect("sync after delete");
        let free_after_delete = EditableFilesystem::free_space(&mut fs).expect("free2");
        assert!(
            free_after_delete > free_after_create,
            "free_space should grow after deletes (after={} before={})",
            free_after_delete,
            free_after_create
        );

        let img2 = fs.reader.into_inner();
        let cur2 = std::io::Cursor::new(img2);
        let mut fs2 = SfsFilesystem::open(cur2, 0).expect("reopen");
        let root2 = fs2.root().expect("root2");
        let kids = fs2.list_directory(&root2).expect("list");
        assert!(kids.is_empty(), "root should be empty, got {:?}", kids);
    }

    #[test]
    fn sfs_create_directory_rolls_back_on_duplicate() {
        use super::super::filesystem::{CreateDirectoryOptions, EditableFilesystem};
        let img = create_blank_sfs(8192, "X").expect("format");
        let cur = std::io::Cursor::new(img);
        let mut fs = SfsFilesystem::open(cur, 0).expect("open");
        let root = fs.root().expect("root");
        fs.create_directory(&root, "Dir", &CreateDirectoryOptions::default())
            .expect("first");
        let before_free = EditableFilesystem::free_space(&mut fs).expect("free");
        let res = fs.create_directory(&root, "Dir", &CreateDirectoryOptions::default());
        assert!(matches!(res, Err(FilesystemError::AlreadyExists(_))));
        let after_free = EditableFilesystem::free_space(&mut fs).expect("free2");
        assert_eq!(
            before_free, after_free,
            "rollback should restore free space"
        );
    }

    #[test]
    fn sfs_last_data_byte_excludes_backup_root() {
        // A blank 4 MiB SFS volume has the backup root allocated at block
        // totalblocks - 1, plus admin + bitmap blocks at the start. The
        // resize floor must report the highest user-data allocation
        // (which on a blank volume is the bitmap region near block 33),
        // NOT the backup root at the very end.
        use super::super::filesystem::Filesystem;
        let img = create_blank_sfs(8192, "FloorTest").expect("format");
        let cur = std::io::Cursor::new(img);
        let mut fs = SfsFilesystem::open(cur, 0).expect("open");
        let total = fs.total_size();
        let floor = fs.last_data_byte().expect("last_data_byte");
        // Blank volume floor should be a small fraction of total size —
        // the metadata at the front plus 1 block of headroom, not the
        // full partition.
        assert!(
            floor < total / 4,
            "blank SFS floor {floor} should be far less than total {total}"
        );
        assert!(
            floor > 0,
            "floor should report at least the metadata region"
        );
    }

    #[test]
    fn sfs_last_data_byte_grows_with_file_data() {
        use super::super::filesystem::{CreateFileOptions, EditableFilesystem, Filesystem};
        let img = create_blank_sfs(8192, "GrowTest").expect("format");
        let cur = std::io::Cursor::new(img);
        let mut fs = SfsFilesystem::open(cur, 0).expect("open");
        let floor_empty = fs.last_data_byte().expect("floor empty");

        // Write a small file. Its data block(s) will be allocated past
        // the metadata region but still well below total.
        let root = fs.root().expect("root");
        let payload = vec![0xABu8; 4096];
        let mut p = std::io::Cursor::new(&payload);
        fs.create_file(
            &root,
            "f.bin",
            &mut p,
            payload.len() as u64,
            &CreateFileOptions::default(),
        )
        .expect("create_file");
        EditableFilesystem::sync_metadata(&mut fs).expect("sync");

        let floor_after = fs.last_data_byte().expect("floor after");
        assert!(
            floor_after >= floor_empty,
            "floor should not decrease after writing data: empty={floor_empty} after={floor_after}"
        );
    }

    #[test]
    fn sfs_resize_shrink_round_trip() {
        // Format a 4 MiB SFS volume, drop a small file, shrink to the
        // floor reported by last_data_byte, and verify the file survives
        // when the volume is reopened at its smaller size.
        use super::super::filesystem::{CreateFileOptions, EditableFilesystem, Filesystem};

        let bs: u64 = 512;
        let img = create_blank_sfs(8192, "ShrinkRT").expect("format");
        let mut cur = std::io::Cursor::new(img);
        let mut fs = SfsFilesystem::open(&mut cur, 0).expect("open");
        let root = fs.root().expect("root");
        let payload = b"This is a small payload that survives an SFS shrink.\n".to_vec();
        let mut p = std::io::Cursor::new(&payload);
        fs.create_file(
            &root,
            "test.txt",
            &mut p,
            payload.len() as u64,
            &CreateFileOptions::default(),
        )
        .expect("create_file");
        EditableFilesystem::sync_metadata(&mut fs).expect("sync");
        let floor = fs.last_data_byte().expect("floor");
        // Pick a shrink target a few blocks above the floor for safety.
        let target_bytes = floor + 4 * bs;
        let target_blocks = (target_bytes / bs) as u32;
        drop(fs);

        // Reset cursor and run the resize.
        cur.set_position(0);
        let mut log_lines: Vec<String> = Vec::new();
        resize_sfs_in_place(&mut cur, 0, target_blocks as u64 * bs, &mut |s| {
            log_lines.push(s.to_string())
        })
        .expect("resize");
        assert!(
            log_lines.iter().any(|l| l.contains("shrunk")),
            "expected a shrink log line, got: {:?}",
            log_lines
        );

        // Reopen the smaller volume and verify totalblocks + file contents.
        cur.set_position(0);
        // Truncate the cursor's backing buffer to the new partition size
        // so SfsFilesystem::open clamps partition_size correctly.
        let img2 = cur.into_inner();
        let new_size = target_blocks as usize * bs as usize;
        let mut img2: Vec<u8> = img2[..new_size].to_vec();
        let cur2 = std::io::Cursor::new(&mut img2);
        let mut fs2 = SfsFilesystem::open(cur2, 0).expect("reopen shrunk");
        assert_eq!(
            fs2.root.totalblocks, target_blocks,
            "totalblocks did not update after shrink"
        );
        let root2 = fs2.root().expect("root2");
        let kids = fs2.list_directory(&root2).expect("list");
        let f = kids
            .iter()
            .find(|c| c.name == "test.txt")
            .expect("test.txt should be present after shrink");
        let data = fs2.read_file(f, payload.len() * 2).expect("read");
        assert_eq!(data, payload, "file contents corrupted by shrink");
    }

    #[test]
    fn sfs_resize_refuses_shrink_below_floor() {
        use super::super::filesystem::{CreateFileOptions, EditableFilesystem, Filesystem};
        let bs: u64 = 512;
        let img = create_blank_sfs(8192, "BelowFloor").expect("format");
        let mut cur = std::io::Cursor::new(img);
        let mut fs = SfsFilesystem::open(&mut cur, 0).expect("open");
        let root = fs.root().expect("root");
        let payload = vec![0xAAu8; 4096];
        let mut p = std::io::Cursor::new(&payload);
        fs.create_file(
            &root,
            "big.bin",
            &mut p,
            payload.len() as u64,
            &CreateFileOptions::default(),
        )
        .expect("create_file");
        EditableFilesystem::sync_metadata(&mut fs).expect("sync");
        let floor = fs.last_data_byte().expect("floor");
        drop(fs);

        cur.set_position(0);
        // Pick a target well below the floor.
        let target_bytes = floor.saturating_sub(8 * bs);
        let res = resize_sfs_in_place(&mut cur, 0, target_bytes, &mut |_| {});
        assert!(
            res.is_err(),
            "shrink below floor should error, got: {:?}",
            res
        );
    }

    #[test]
    fn sfs_resize_grow_round_trip() {
        // Format a small SFS volume, write a file, grow the volume by a
        // few hundred blocks (within the existing bitmap's capacity),
        // reopen, verify the file survives and the volume reports the
        // new total.
        use super::super::filesystem::{CreateFileOptions, EditableFilesystem, Filesystem};

        let bs: u64 = 512;
        // 4096 blocks @ 512 = 2 MiB. With bs=512, one bitmap block covers
        // (512-12)/4 * 32 = 4000 bits, so a 4096-block volume has 2 bitmap
        // blocks and capacity for up to 8000 blocks total without extending
        // the bitmap chain. Grow target: 6000 blocks (~3 MiB).
        let img = create_blank_sfs(4096, "GrowRT").expect("format");
        let mut backing: Vec<u8> = img;
        // Pad backing buffer to fit the post-grow volume.
        backing.resize(6000 * bs as usize, 0);

        let mut cur = std::io::Cursor::new(&mut backing);
        let mut fs = SfsFilesystem::open(&mut cur, 0).expect("open");
        let root = fs.root().expect("root");
        let payload = b"Survives grow.\n".to_vec();
        let mut p = std::io::Cursor::new(&payload);
        fs.create_file(
            &root,
            "g.txt",
            &mut p,
            payload.len() as u64,
            &CreateFileOptions::default(),
        )
        .expect("create_file");
        EditableFilesystem::sync_metadata(&mut fs).expect("sync");
        drop(fs);

        cur.set_position(0);
        let mut log_lines: Vec<String> = Vec::new();
        resize_sfs_in_place(&mut cur, 0, 6000 * bs, &mut |s| {
            log_lines.push(s.to_string())
        })
        .expect("grow");
        assert!(
            log_lines.iter().any(|l| l.contains("grew")),
            "expected a grow log line, got: {:?}",
            log_lines
        );

        cur.set_position(0);
        let mut fs2 = SfsFilesystem::open(&mut cur, 0).expect("reopen grown");
        let new_root = fs2.root.clone();
        assert_eq!(new_root.totalblocks, 6000);
        let root2 = fs2.root().expect("root2");
        let kids = fs2.list_directory(&root2).expect("list");
        let f = kids
            .iter()
            .find(|c| c.name == "g.txt")
            .expect("file should survive grow");
        let data = fs2.read_file(f, payload.len() * 2).expect("read");
        assert_eq!(data, payload);
        drop(fs2);
        // Sanity: new tail blocks should now be free.
        let bm = read_bitmap_raw(&mut cur, 0, &new_root).expect("bm");
        for blk in 4096..5999u32 {
            let byte = bm[(blk / 8) as usize];
            let bit_set = (byte >> (blk % 8)) & 1 == 1;
            assert!(bit_set, "block {blk} should be free in the grown bitmap");
        }
        // New backup root should be marked allocated.
        let backup = 5999u32;
        let byte = bm[(backup / 8) as usize];
        let bit_set = (byte >> (backup % 8)) & 1 == 1;
        assert!(
            !bit_set,
            "new backup root block {backup} should be allocated"
        );
    }

    #[test]
    fn sfs_resize_refuses_grow_past_bitmap_capacity() {
        // A 4096-block volume has 2 bitmap blocks covering 8000 blocks.
        // Asking to grow to 8001+ blocks would need a 3rd bitmap block,
        // which Phase S.2 refuses.
        let bs: u64 = 512;
        let img = create_blank_sfs(4096, "TooBig").expect("format");
        let mut backing = img;
        backing.resize(10000 * bs as usize, 0);
        let mut cur = std::io::Cursor::new(&mut backing);
        let res = resize_sfs_in_place(&mut cur, 0, 10000 * bs, &mut |_| {});
        assert!(res.is_err(), "grow past bitmap capacity should fail");
        let msg = res.unwrap_err().to_string();
        assert!(
            msg.contains("bitmap block") || msg.contains("Phase S.3"),
            "expected bitmap-capacity error message, got: {msg}"
        );
    }

    #[test]
    fn sfs_resize_skips_non_sfs() {
        // A buffer with non-SFS bytes at sector 0 should be a no-op.
        let mut buf = vec![0u8; 64 * 1024];
        buf[0..4].copy_from_slice(b"NTFS");
        let mut cur = std::io::Cursor::new(&mut buf);
        let mut messages: Vec<String> = Vec::new();
        resize_sfs_in_place(&mut cur, 0, 32 * 1024, &mut |s| {
            messages.push(s.to_string())
        })
        .expect("non-sfs should be a clean skip");
        assert!(
            messages.is_empty() || !messages.iter().any(|m| m.contains("shrunk")),
            "non-SFS resize should not log a shrink: {:?}",
            messages
        );
    }

    #[test]
    fn parse_object_minimal() {
        // Minimal: objectnode=42, dir bit set, name="hi", no comment.
        let mut e = vec![0u8; 32];
        e[4..8].copy_from_slice(&42u32.to_be_bytes());
        e[24] = OTYPE_DIR;
        e[25] = b'h';
        e[26] = b'i';
        e[27] = 0; // name NUL
        e[28] = 0; // comment NUL
        let (obj, consumed) = parse_object(&e, 0).expect("parses");
        assert_eq!(obj.objectnode, 42);
        assert!(obj.is_dir());
        assert_eq!(obj.name, "hi");
        // Aligned to even — name(2)+NUL+empty-comment+NUL = bytes 25..29 = 4
        // consumed bytes => total 25+4 = 29 → align to 30.
        assert_eq!(consumed, 30);
    }
}
