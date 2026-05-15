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
    if buf.len() < 12 || buf.len() % 4 != 0 {
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

pub struct SfsFilesystem<R: Read + Seek> {
    reader: R,
    partition_offset: u64,
    partition_size: u64,
    root: SfsRootBlock,
    cache: HashMap<u32, BlockBuf>,
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
        if !(512..=65536).contains(&blocksize) || blocksize % 512 != 0 {
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
        let shifts_block = (self.root.blocksize as u32).trailing_zeros();
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
        match read_bitmap(self) {
            Ok(bitmap) => {
                // Find highest allocated block (bit cleared).
                let bs = self.root.blocksize as u64;
                for byte_idx in (0..bitmap.len()).rev() {
                    let byte = bitmap[byte_idx];
                    if byte == 0xFF {
                        continue;
                    }
                    for bit in (0..8u8).rev() {
                        if (byte >> bit) & 1 == 0 {
                            let block_idx = (byte_idx as u64) * 8 + bit as u64;
                            return Ok((block_idx + 1) * bs);
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
    let total_bytes = ((total as usize) + 7) / 8;
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

        let total_blocks = (partition_size / blocksize as u64) as u64;
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
