//! APFS (Apple File System) — read-only browse driver.
//!
//! Scope (see `docs/apfs_support_plan.md`): **read-only browse** of
//! **unencrypted** APFS volumes. Snapshots, FileVault encryption, and any
//! write / shrink / fsck support are explicitly deferred — the trait's
//! `Unsupported` / layout-preserving defaults cover those for now.
//!
//! ## On-disk model (the layers a single `read_file` walks through)
//!
//! 1. **NXSB** — container superblock (block 0, plus copies in the checkpoint
//!    descriptor ring). The newest valid copy names the container object map
//!    and the list of volume object-ids.
//! 2. **omap** — object map B-tree. Almost every object reference is a
//!    *virtual* oid that must be resolved to a physical block through an omap.
//! 3. **APSB** — volume superblock, one per volume. Names the volume's own
//!    omap plus its catalog (root) tree.
//! 4. **Catalog (FS) tree** — a virtual B-tree of `j_*` records keyed by
//!    `(obj_id, kind)`: inodes, directory entries, data-stream ids, and file
//!    extents.
//! 5. **File extents** — `(logical_addr, length, phys_block)` triples that map
//!    a file's bytes onto physical blocks.
//!
//! Cross-cutting: every object block opens with an [`ObjPhys`] header whose
//! first 8 bytes are a Fletcher-64 checksum over the rest of the block, all
//! multi-byte fields are little-endian, and the block size (`nx_block_size`,
//! usually 4096) is *not* assumed to be 512.
//!
//! Reference: Apple's *Apple File System Reference* (the `nx_*` / `apfs_*` /
//! `j_*` / `btree_node_phys` / `omap_*` struct names used below match it).

use std::io::{Read, Seek, SeekFrom};

use super::entry::FileEntry;
use super::filesystem::{Filesystem, FilesystemError};

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

/// `NXSB` — container superblock magic, at offset 32 of block 0 (little-endian
/// `u32` = `0x4253_584E`). We keep it as the ASCII bytes for a direct compare.
const NX_MAGIC: &[u8; 4] = b"NXSB";

/// `APSB` — volume superblock magic, at offset 32 of a volume superblock.
const APFS_MAGIC: &[u8; 4] = b"APSB";

/// Object-type mask over the low 16 bits of `o_type`.
const OBJECT_TYPE_MASK: u32 = 0x0000_ffff;

// Object types we recognize (low 16 bits of `obj_phys.o_type`).
const OBJECT_TYPE_NX_SUPERBLOCK: u32 = 0x0001;

// B-tree node flags (`btn_flags`). Leaf-ness is read from `btn_level == 0`
// rather than a flag, which is the layout-independent test.
const BTNODE_ROOT: u16 = 0x0001;
const BTNODE_FIXED_KV_SIZE: u16 = 0x0004;

/// Size of the `btree_info_t` trailer stored at the end of a B-tree root node.
const BTREE_INFO_SIZE: usize = 40;
/// Size of an `omap_key` (oid + xid) and the leaf `omap_val`.
const OMAP_KEY_SIZE: usize = 16;
const OMAP_VAL_SIZE: usize = 16;
/// Guard against a corrupt tree sending us into an unbounded walk.
const MAX_BTREE_NODES: usize = 1_000_000;

/// Largest container we will map: NXSB `nx_max_file_systems` bounds the volume
/// array at 100 on current APFS.
const NX_MAX_FILE_SYSTEMS: usize = 100;

// FS-tree record kinds — the top 4 bits of a `j_key.obj_id_and_type`.
const OBJ_TYPE_SHIFT: u32 = 60;
const OBJ_ID_MASK: u64 = 0x0fff_ffff_ffff_ffff;
const APFS_TYPE_INODE: u8 = 3;
const APFS_TYPE_XATTR: u8 = 4;
const APFS_TYPE_FILE_EXTENT: u8 = 8;
const APFS_TYPE_DIR_REC: u8 = 9;

// Directory-entry file kinds (`j_drec_val.flags & DREC_TYPE_MASK`), mirroring
// the BSD `DT_*` values.
const DREC_TYPE_MASK: u16 = 0x000f;
const DT_DIR: u16 = 4;
const DT_LNK: u16 = 10;

/// Inode number of the volume root directory (`ROOT_DIR_INO_NUM`).
const ROOT_DIR_INO: u64 = 2;

/// Extended-field type carrying a file's data-stream record (`j_dstream_t`),
/// whose first field is the logical file size.
const INO_EXT_TYPE_DSTREAM: u8 = 8;

// Volume incompatible-feature bits that select the *hashed* directory-record
// key layout (`j_drec_hashed_key`) over the plain one.
const APFS_INCOMPAT_CASE_INSENSITIVE: u64 = 0x0000_0001;
const APFS_INCOMPAT_NORMALIZATION_INSENSITIVE: u64 = 0x0000_0008;

/// Length mask over `j_drec_hashed_key.name_len_and_hash` (low 10 bits =
/// name length including the trailing NUL).
const J_DREC_LEN_MASK: u32 = 0x0000_03ff;

// ---------------------------------------------------------------------------
// Fletcher-64 (APFS variant)
// ---------------------------------------------------------------------------

/// APFS Fletcher-64 checksum over an object block.
///
/// The stored checksum lives in the first 8 bytes of the block (`o_cksum`); the
/// value it holds is computed over the *rest* of the block (`block[8..]`),
/// treated as a stream of little-endian 32-bit words. `block.len()` and the
/// remaining length are always 4-byte multiples for real APFS objects.
fn fletcher64(block: &[u8]) -> u64 {
    const MOD: u64 = 0xffff_ffff;
    let mut sum1: u64 = 0;
    let mut sum2: u64 = 0;
    // Checksum covers everything after the 8-byte o_cksum field.
    for chunk in block[8..].chunks_exact(4) {
        let val = u32::from_le_bytes([chunk[0], chunk[1], chunk[2], chunk[3]]) as u64;
        sum1 = (sum1 + val) % MOD;
        sum2 = (sum2 + sum1) % MOD;
    }
    let c1 = MOD - ((sum1 + sum2) % MOD);
    let c2 = MOD - ((sum1 + c1) % MOD);
    (c2 << 32) | c1
}

/// True when a block's stored `o_cksum` matches a freshly computed Fletcher-64.
/// Used to reject stale checkpoint copies and detect corruption.
fn checksum_valid(block: &[u8]) -> bool {
    if block.len() < 8 {
        return false;
    }
    let stored = u64::from_le_bytes(block[0..8].try_into().unwrap());
    fletcher64(block) == stored
}

// ---------------------------------------------------------------------------
// obj_phys — the 32-byte header on every object block
// ---------------------------------------------------------------------------

/// The `obj_phys_t` header prefixing every APFS object block.
#[derive(Debug, Clone, Copy)]
struct ObjPhys {
    #[allow(dead_code)]
    oid: u64,
    xid: u64,
    /// Low 16 bits of `o_type` — the object type (`OBJECT_TYPE_*`).
    obj_type: u32,
    #[allow(dead_code)]
    subtype: u32,
}

impl ObjPhys {
    fn parse(block: &[u8]) -> Result<Self, FilesystemError> {
        if block.len() < 32 {
            return Err(FilesystemError::Parse("object block too small".into()));
        }
        Ok(Self {
            oid: u64::from_le_bytes(block[8..16].try_into().unwrap()),
            xid: u64::from_le_bytes(block[16..24].try_into().unwrap()),
            obj_type: u32::from_le_bytes(block[24..28].try_into().unwrap()) & OBJECT_TYPE_MASK,
            subtype: u32::from_le_bytes(block[28..32].try_into().unwrap()),
        })
    }
}

// ---------------------------------------------------------------------------
// Little-endian field readers (keep the parse code terse and bounds-checked)
// ---------------------------------------------------------------------------

fn rd_u16(b: &[u8], off: usize) -> u16 {
    u16::from_le_bytes([b[off], b[off + 1]])
}
fn rd_u32(b: &[u8], off: usize) -> u32 {
    u32::from_le_bytes([b[off], b[off + 1], b[off + 2], b[off + 3]])
}
fn rd_u64(b: &[u8], off: usize) -> u64 {
    u64::from_le_bytes(b[off..off + 8].try_into().unwrap())
}

// ---------------------------------------------------------------------------
// btree_node_phys — the generic B-tree node, shared by the omap and FS trees
// ---------------------------------------------------------------------------

/// A parsed view over one `btree_node_phys` block: it locates the table of
/// contents plus the key and value areas so callers can pull out the i-th
/// (key, value) byte slice regardless of which tree the node belongs to.
///
/// Two layouts are handled: **fixed** KV (`BTNODE_FIXED_KV_SIZE`, used by the
/// omap — the toc is an array of `(key_off, val_off)` u16 pairs) and
/// **variable** KV (the FS/catalog tree — the toc is an array of
/// `(key_off, key_len, val_off, val_len)` u16 quads). In both, key offsets are
/// measured forward from the start of the key area and value offsets backward
/// from the end of the value area (which excludes the root node's info trailer).
struct BtreeNode {
    block: Vec<u8>,
    level: u16,
    nkeys: usize,
    fixed: bool,
    toc_start: usize,
    key_area: usize,
    val_area_end: usize,
}

impl BtreeNode {
    fn parse(block: Vec<u8>) -> Result<Self, FilesystemError> {
        if block.len() < 56 {
            return Err(FilesystemError::Parse("btree node too small".into()));
        }
        let flags = rd_u16(&block, 32);
        let level = rd_u16(&block, 34);
        let nkeys = rd_u32(&block, 36) as usize;
        let toc_off = rd_u16(&block, 40) as usize;
        let toc_len = rd_u16(&block, 42) as usize;
        let toc_start = 56 + toc_off;
        let key_area = toc_start + toc_len;
        let is_root = flags & BTNODE_ROOT != 0;
        let val_area_end = block
            .len()
            .saturating_sub(if is_root { BTREE_INFO_SIZE } else { 0 });
        if key_area > block.len() || val_area_end > block.len() || key_area > val_area_end {
            return Err(FilesystemError::Parse(
                "btree node areas out of range".into(),
            ));
        }
        Ok(Self {
            block,
            level,
            nkeys,
            fixed: flags & BTNODE_FIXED_KV_SIZE != 0,
            toc_start,
            key_area,
            val_area_end,
        })
    }

    fn is_leaf(&self) -> bool {
        self.level == 0
    }

    /// The i-th entry for a **fixed**-KV node. `val_size` differs between leaf
    /// (record) and index (an 8-byte child oid) nodes, so the caller supplies
    /// it. Returns `(key, value)` byte slices, bounds-checked.
    fn entry_fixed(
        &self,
        i: usize,
        key_size: usize,
        val_size: usize,
    ) -> Result<(&[u8], &[u8]), FilesystemError> {
        let toc = self.toc_start + i * 4;
        if toc + 4 > self.block.len() {
            return Err(FilesystemError::Parse("btree toc out of range".into()));
        }
        let koff = rd_u16(&self.block, toc) as usize;
        let voff = rd_u16(&self.block, toc + 2) as usize;
        let kstart = self.key_area + koff;
        let vstart = self
            .val_area_end
            .checked_sub(voff)
            .ok_or_else(|| FilesystemError::Parse("btree value offset underflow".into()))?;
        self.slice_pair(kstart, key_size, vstart, val_size)
    }

    /// The i-th entry for a **variable**-KV node (self-describing key/value
    /// lengths in the toc). Used by the Phase 3 catalog (FS-tree) walk.
    #[allow(dead_code)]
    fn entry_var(&self, i: usize) -> Result<(&[u8], &[u8]), FilesystemError> {
        let toc = self.toc_start + i * 8;
        if toc + 8 > self.block.len() {
            return Err(FilesystemError::Parse("btree toc out of range".into()));
        }
        let koff = rd_u16(&self.block, toc) as usize;
        let klen = rd_u16(&self.block, toc + 2) as usize;
        let voff = rd_u16(&self.block, toc + 4) as usize;
        let vlen = rd_u16(&self.block, toc + 6) as usize;
        let kstart = self.key_area + koff;
        let vstart = self
            .val_area_end
            .checked_sub(voff)
            .ok_or_else(|| FilesystemError::Parse("btree value offset underflow".into()))?;
        self.slice_pair(kstart, klen, vstart, vlen)
    }

    fn slice_pair(
        &self,
        kstart: usize,
        klen: usize,
        vstart: usize,
        vlen: usize,
    ) -> Result<(&[u8], &[u8]), FilesystemError> {
        let kend = kstart + klen;
        let vend = vstart + vlen;
        if kend > self.block.len() || vend > self.block.len() {
            return Err(FilesystemError::Parse("btree entry out of range".into()));
        }
        Ok((&self.block[kstart..kend], &self.block[vstart..vend]))
    }
}

// ---------------------------------------------------------------------------
// omap_phys + omap resolution
// ---------------------------------------------------------------------------

/// A container/volume object map, flattened to `oid -> physical block address`.
///
/// The omap B-tree is keyed by `(oid, xid)`; for a single-state (no-snapshot)
/// read we keep, per oid, the mapping with the highest xid. Volumes here are
/// tiny, so collecting the whole tree up front is cheaper and simpler than a
/// per-lookup descent.
struct Omap {
    map: std::collections::HashMap<u64, u64>,
}

impl Omap {
    fn resolve(&self, oid: u64) -> Option<u64> {
        self.map.get(&oid).copied()
    }
}

// ---------------------------------------------------------------------------
// A volume (APSB) within the container
// ---------------------------------------------------------------------------

/// The fields of `apfs_superblock_t` (APSB) this driver needs.
struct VolumeInfo {
    name: String,
    /// Physical block of the volume's own object map (`omap_phys_t`). The
    /// catalog walk resolves the root tree's virtual oid through it.
    omap_oid: u64,
    /// Virtual oid of the volume's root (catalog) FS tree; resolved via the
    /// volume omap.
    root_tree_oid: u64,
    /// Blocks currently allocated to the volume (`apfs_fs_alloc_count`).
    alloc_count: u64,
    /// True when directory records use the hashed key layout
    /// (`j_drec_hashed_key`) — the case here for case-insensitive or
    /// normalization-insensitive volumes, which is the macOS default.
    drec_hashed: bool,
}

// ---------------------------------------------------------------------------
// NXSB — container superblock
// ---------------------------------------------------------------------------

/// The fields of `nx_superblock_t` this driver needs. Offsets are from the
/// start of the object block (the `obj_phys` header occupies 0..32).
#[derive(Debug, Clone)]
struct NxSuperblock {
    block_size: u32,
    block_count: u64,
    /// Checkpoint descriptor ring: base block and length in blocks. We scan it
    /// for the newest valid NXSB copy.
    xp_desc_base: u64,
    xp_desc_blocks: u32,
    /// Physical block of the container object map (`omap_phys_t`).
    omap_oid: u64,
    /// Virtual oids of the container's volumes; resolved via the container omap.
    fs_oids: Vec<u64>,
}

impl NxSuperblock {
    fn parse(block: &[u8]) -> Result<Self, FilesystemError> {
        if block.len() < 192 || &block[32..36] != NX_MAGIC {
            return Err(FilesystemError::Parse("not an NXSB superblock".into()));
        }
        let block_size = rd_u32(block, 36);
        let block_count = rd_u64(block, 40);
        // nx_xp_desc_blocks @104, nx_xp_desc_base @112 (paddr).
        let xp_desc_blocks = rd_u32(block, 104) & 0x7fff_ffff; // high bit = "tree", not a ring
        let xp_desc_base = rd_u64(block, 112);
        // nx_omap_oid @160.
        let omap_oid = rd_u64(block, 160);
        // nx_max_file_systems @180, nx_fs_oid[] @184.
        let max_fs = (rd_u32(block, 180) as usize).min(NX_MAX_FILE_SYSTEMS);
        let mut fs_oids = Vec::new();
        for i in 0..max_fs {
            let off = 184 + i * 8;
            if off + 8 > block.len() {
                break;
            }
            let oid = rd_u64(block, off);
            if oid != 0 {
                fs_oids.push(oid);
            }
        }
        Ok(Self {
            block_size,
            block_count,
            xp_desc_base,
            xp_desc_blocks,
            omap_oid,
            fs_oids,
        })
    }
}

// ---------------------------------------------------------------------------
// The filesystem handle
// ---------------------------------------------------------------------------

pub struct ApfsFilesystem<R: Read + Seek + Send> {
    reader: R,
    partition_offset: u64,
    block_size: u32,
    block_count: u64,
    /// Authoritative container superblock (newest valid checkpoint copy).
    nxsb: NxSuperblock,
    /// Volumes discovered in the container, in `nx_fs_oid[]` order.
    volumes: Vec<VolumeInfo>,
    /// Index into `volumes` of the volume currently being browsed.
    active_vol: usize,
    /// Lazily-built catalog for the active volume (parsed on first browse).
    catalog: Option<Catalog>,
}

/// The active volume's decoded catalog, cached after the first browse.
///
/// Built by a single walk of the FS (catalog) tree. `children` powers
/// `list_directory` (a parent-inode oid maps to its directory entries) and
/// `inodes` supplies per-file metadata (size, and — in Phase 4 — the
/// data-stream id used to gather file extents).
#[derive(Default)]
struct Catalog {
    /// parent inode oid -> its directory entries.
    children: std::collections::HashMap<u64, Vec<ChildRec>>,
    /// inode oid -> inode metadata.
    inodes: std::collections::HashMap<u64, InodeRec>,
}

/// One directory entry (`j_drec_*` key/value pair).
struct ChildRec {
    name: String,
    /// Child inode oid (`j_drec_val.file_id`).
    oid: u64,
    /// Directory-entry file kind (`DT_*`).
    dtype: u16,
}

/// Per-inode metadata pulled from a `j_inode_val` record.
struct InodeRec {
    /// Logical file size from the data-stream extended field (0 for
    /// directories / forkless inodes).
    size: u64,
    /// `j_inode_val.private_id` — the id file extents are keyed on. Used in
    /// Phase 4 to read file data.
    #[allow(dead_code)]
    private_id: u64,
    /// `j_inode_val.mode` (the `S_IF*` file-type bits live here).
    #[allow(dead_code)]
    mode: u16,
}

impl<R: Read + Seek + Send> ApfsFilesystem<R> {
    /// Open an APFS container at `partition_offset` within `reader`.
    ///
    /// Reads block 0 to learn the block size, then scans the checkpoint
    /// descriptor ring for the newest valid NXSB (block 0 alone can be stale).
    pub fn open(mut reader: R, partition_offset: u64) -> Result<Self, FilesystemError> {
        // Block 0 is read at 4 KiB — the standard APFS block size — which is
        // enough to hold the whole superblock regardless of the real value.
        let mut b0 = vec![0u8; 4096];
        reader.seek(SeekFrom::Start(partition_offset))?;
        reader.read_exact(&mut b0)?;
        let sb0 = NxSuperblock::parse(&b0)?;
        let block_size = sb0.block_size;
        if !(512..=65536).contains(&block_size) || !block_size.is_power_of_two() {
            return Err(FilesystemError::Parse(format!(
                "implausible APFS block size {block_size}"
            )));
        }

        let mut fs = Self {
            reader,
            partition_offset,
            block_size,
            block_count: sb0.block_count,
            nxsb: sb0,
            volumes: Vec::new(),
            active_vol: 0,
            catalog: None,
        };

        // Prefer the newest valid superblock from the checkpoint descriptor
        // ring; fall back to the block-0 copy if the scan finds nothing newer.
        if let Some(newest) = fs.find_latest_superblock()? {
            fs.nxsb = newest;
        }

        // Resolve the container object map, then walk each volume's superblock.
        let container_omap = fs.load_omap(fs.nxsb.omap_oid)?;
        let fs_oids = fs.nxsb.fs_oids.clone();
        for vol_oid in fs_oids {
            let Some(apsb_paddr) = container_omap.resolve(vol_oid) else {
                continue;
            };
            match fs.parse_volume(apsb_paddr) {
                Ok(v) => fs.volumes.push(v),
                // A single unreadable volume shouldn't sink the whole container.
                Err(_) => continue,
            }
        }
        Ok(fs)
    }

    /// Load an object map (`omap_phys_t` at `omap_oid`) and flatten its B-tree
    /// into an `oid -> paddr` table.
    fn load_omap(&mut self, omap_oid: u64) -> Result<Omap, FilesystemError> {
        let omap_block = self.read_block(omap_oid)?;
        if omap_block.len() < 56 {
            return Err(FilesystemError::Parse("omap_phys too small".into()));
        }
        // omap_phys.om_tree_oid @48 — physical block of the omap B-tree root.
        let tree_root = rd_u64(&omap_block, 48);
        let mut map = std::collections::HashMap::new();
        let mut best_xid: std::collections::HashMap<u64, u64> = std::collections::HashMap::new();
        let mut stack = vec![tree_root];
        let mut visited = 0usize;
        while let Some(paddr) = stack.pop() {
            visited += 1;
            if visited > MAX_BTREE_NODES {
                return Err(FilesystemError::Parse("omap B-tree too large".into()));
            }
            let node = BtreeNode::parse(self.read_block(paddr)?)?;
            if !node.fixed {
                return Err(FilesystemError::Parse("omap node is not fixed-KV".into()));
            }
            let leaf = node.is_leaf();
            let val_size = if leaf { OMAP_VAL_SIZE } else { 8 };
            for i in 0..node.nkeys {
                let (k, v) = node.entry_fixed(i, OMAP_KEY_SIZE, val_size)?;
                if leaf {
                    let oid = rd_u64(k, 0);
                    let xid = rd_u64(k, 8);
                    // omap_val.ov_paddr @8.
                    let paddr = rd_u64(v, 8);
                    let slot = best_xid.entry(oid).or_insert(0);
                    if xid >= *slot {
                        *slot = xid;
                        map.insert(oid, paddr);
                    }
                } else {
                    // Index node: value is the child node's physical block.
                    stack.push(rd_u64(v, 0));
                }
            }
        }
        Ok(Omap { map })
    }

    /// Parse a volume superblock (APSB) at `apsb_paddr`.
    fn parse_volume(&mut self, apsb_paddr: u64) -> Result<VolumeInfo, FilesystemError> {
        let block = self.read_block(apsb_paddr)?;
        if block.len() < 960 || &block[32..36] != APFS_MAGIC {
            return Err(FilesystemError::Parse(
                "not an APSB volume superblock".into(),
            ));
        }
        // apfs_incompatible_features @56, apfs_fs_alloc_count @88,
        // apfs_omap_oid @128, apfs_root_tree_oid @136, apfs_volname[256] @704.
        let incompat = rd_u64(&block, 56);
        let alloc_count = rd_u64(&block, 88);
        let omap_oid = rd_u64(&block, 128);
        let root_tree_oid = rd_u64(&block, 136);
        let name_bytes = &block[704..704 + 256];
        let name_end = name_bytes.iter().position(|&b| b == 0).unwrap_or(256);
        let name = String::from_utf8_lossy(&name_bytes[..name_end]).into_owned();
        let drec_hashed = incompat
            & (APFS_INCOMPAT_CASE_INSENSITIVE | APFS_INCOMPAT_NORMALIZATION_INSENSITIVE)
            != 0;
        Ok(VolumeInfo {
            name,
            omap_oid,
            root_tree_oid,
            alloc_count,
            drec_hashed,
        })
    }

    /// The volume currently selected for browsing, if the container has one.
    fn active_volume(&self) -> Option<&VolumeInfo> {
        self.volumes.get(self.active_vol)
    }

    /// Ensure the active volume's catalog is built and return a reference to it.
    fn catalog(&mut self) -> Result<&Catalog, FilesystemError> {
        if self.catalog.is_none() {
            let cat = self.build_catalog()?;
            self.catalog = Some(cat);
        }
        Ok(self.catalog.as_ref().unwrap())
    }

    /// Walk the active volume's FS (catalog) tree once, decoding inode and
    /// directory-record entries into a [`Catalog`].
    ///
    /// The FS tree is a *virtual* B-tree: its root and every index child are
    /// virtual oids resolved through the volume's own object map. Leaf nodes
    /// hold the `j_*` records we care about.
    fn build_catalog(&mut self) -> Result<Catalog, FilesystemError> {
        let vol = self
            .active_volume()
            .ok_or_else(|| FilesystemError::Unsupported("APFS container has no volume".into()))?;
        let root_tree_oid = vol.root_tree_oid;
        let vol_omap_oid = vol.omap_oid;
        let hashed = vol.drec_hashed;

        let vol_omap = self.load_omap(vol_omap_oid)?;
        let root_paddr = vol_omap
            .resolve(root_tree_oid)
            .ok_or_else(|| FilesystemError::Parse("root FS tree oid unresolved in omap".into()))?;

        let mut cat = Catalog::default();
        let mut stack = vec![root_paddr];
        let mut visited = 0usize;
        while let Some(paddr) = stack.pop() {
            visited += 1;
            if visited > MAX_BTREE_NODES {
                return Err(FilesystemError::Parse("FS B-tree too large".into()));
            }
            let node = BtreeNode::parse(self.read_block(paddr)?)?;
            let leaf = node.is_leaf();
            for i in 0..node.nkeys {
                let (key, val) = node.entry_var(i)?;
                if leaf {
                    Self::decode_record(&mut cat, key, val, hashed);
                } else {
                    // Index node: value is an 8-byte virtual child oid.
                    if val.len() >= 8 {
                        if let Some(child) = vol_omap.resolve(rd_u64(val, 0)) {
                            stack.push(child);
                        }
                    }
                }
            }
        }
        Ok(cat)
    }

    /// Decode one FS-tree leaf record into the catalog. Unknown / not-yet-used
    /// record kinds (xattrs, extents, sibling links, …) are skipped here; the
    /// ones Phase 4 needs are added there.
    fn decode_record(cat: &mut Catalog, key: &[u8], val: &[u8], hashed: bool) {
        if key.len() < 8 {
            return;
        }
        let id_and_type = rd_u64(key, 0);
        let kind = (id_and_type >> OBJ_TYPE_SHIFT) as u8;
        let obj_id = id_and_type & OBJ_ID_MASK;
        match kind {
            APFS_TYPE_INODE => {
                if let Some(rec) = decode_inode(val) {
                    cat.inodes.insert(obj_id, rec);
                }
            }
            APFS_TYPE_DIR_REC => {
                if let Some(child) = decode_drec(key, val, hashed) {
                    cat.children.entry(obj_id).or_default().push(child);
                }
            }
            // APFS_TYPE_FILE_EXTENT / APFS_TYPE_XATTR are handled in Phase 4.
            APFS_TYPE_FILE_EXTENT | APFS_TYPE_XATTR => {}
            _ => {}
        }
    }

    /// One-line summary of the container's headline fields. Used by the
    /// `apfs_dump` example during driver bring-up; also the seam the later
    /// phases hang volume enumeration off of.
    pub fn debug_summary(&self) -> String {
        let vols: Vec<String> = self
            .volumes
            .iter()
            .map(|v| {
                format!(
                    "{{name={:?} omap={} root_tree={} alloc={}}}",
                    v.name, v.omap_oid, v.root_tree_oid, v.alloc_count
                )
            })
            .collect();
        format!(
            "block_size={} block_count={} container_omap_oid={} fs_oids={:?} volumes=[{}]",
            self.block_size,
            self.block_count,
            self.nxsb.omap_oid,
            self.nxsb.fs_oids,
            vols.join(", "),
        )
    }

    /// Read a single object block by physical block address.
    fn read_block(&mut self, paddr: u64) -> Result<Vec<u8>, FilesystemError> {
        let mut buf = vec![0u8; self.block_size as usize];
        let pos = self.partition_offset + paddr * self.block_size as u64;
        self.reader.seek(SeekFrom::Start(pos))?;
        self.reader.read_exact(&mut buf)?;
        Ok(buf)
    }

    /// Scan the checkpoint descriptor ring for the NXSB copy with the highest
    /// transaction id (`xid`) and a valid checksum. Returns `None` when the
    /// ring holds nothing better than the block-0 copy we already have.
    fn find_latest_superblock(&mut self) -> Result<Option<NxSuperblock>, FilesystemError> {
        let base = self.nxsb.xp_desc_base;
        let count = self.nxsb.xp_desc_blocks as u64;
        if base == 0 || count == 0 || count > 1_000_000 {
            return Ok(None);
        }
        let mut best: Option<(u64, NxSuperblock)> = None;
        for i in 0..count {
            let block = match self.read_block(base + i) {
                Ok(b) => b,
                Err(_) => continue,
            };
            // Only NXSB copies with a valid checksum are candidates; the ring
            // also holds checkpoint_map_phys objects we skip.
            let hdr = match ObjPhys::parse(&block) {
                Ok(h) => h,
                Err(_) => continue,
            };
            if hdr.obj_type != OBJECT_TYPE_NX_SUPERBLOCK {
                continue;
            }
            if &block[32..36] != NX_MAGIC || !checksum_valid(&block) {
                continue;
            }
            if let Ok(sb) = NxSuperblock::parse(&block) {
                if best.as_ref().map(|(x, _)| hdr.xid > *x).unwrap_or(true) {
                    best = Some((hdr.xid, sb));
                }
            }
        }
        Ok(best.map(|(_, sb)| sb))
    }
}

/// Decode a `j_inode_val` into the metadata we keep.
fn decode_inode(val: &[u8]) -> Option<InodeRec> {
    // Fixed part of j_inode_val is 92 bytes; private_id @8, mode @80.
    if val.len() < 92 {
        return None;
    }
    let private_id = rd_u64(val, 8);
    let mode = rd_u16(val, 80);
    let size = inode_dstream_size(val).unwrap_or(0);
    Some(InodeRec {
        size,
        private_id,
        mode,
    })
}

/// Pull the logical file size out of an inode's `INO_EXT_TYPE_DSTREAM` extended
/// field, if present (directories and forkless inodes have none → `None`).
///
/// The xfield blob begins at offset 92 of `j_inode_val`: a 4-byte header
/// (`xf_num_exts`, `xf_used_data`), then `xf_num_exts` × 4-byte `x_field`
/// descriptors, then the data area where each field's payload is padded up to
/// an 8-byte boundary. The dstream payload is a `j_dstream_t` whose first u64
/// is the logical size.
fn inode_dstream_size(val: &[u8]) -> Option<u64> {
    let xf = val.get(92..)?;
    if xf.len() < 4 {
        return None;
    }
    let num = rd_u16(xf, 0) as usize;
    let mut desc = 4usize;
    let mut data = 4 + num.checked_mul(4)?;
    for _ in 0..num {
        if desc + 4 > xf.len() {
            return None;
        }
        let x_type = xf[desc];
        let x_size = rd_u16(xf, desc + 2) as usize;
        if x_type == INO_EXT_TYPE_DSTREAM {
            // j_dstream.size is the first u64 of the payload.
            if data + 8 <= xf.len() {
                return Some(rd_u64(xf, data));
            }
            return None;
        }
        data += (x_size + 7) & !7; // 8-byte aligned payloads
        desc += 4;
    }
    None
}

/// Decode a directory-record (`j_drec_*`) key/value pair into a [`ChildRec`].
///
/// The key layout depends on the volume: hashed volumes store
/// `name_len_and_hash` (a u32 whose low 10 bits are the NUL-terminated name
/// length) before the name; plain volumes store a bare u16 length.
fn decode_drec(key: &[u8], val: &[u8], hashed: bool) -> Option<ChildRec> {
    // j_key header is 8 bytes; the name descriptor follows.
    let name_bytes = if hashed {
        if key.len() < 12 {
            return None;
        }
        let name_len = (rd_u32(key, 8) & J_DREC_LEN_MASK) as usize; // includes trailing NUL
        let end = 12usize.checked_add(name_len)?.min(key.len());
        key.get(12..end)?
    } else {
        if key.len() < 10 {
            return None;
        }
        let name_len = rd_u16(key, 8) as usize; // includes trailing NUL
        let end = 10usize.checked_add(name_len)?.min(key.len());
        key.get(10..end)?
    };
    // Strip the trailing NUL and decode as UTF-8 (APFS names are UTF-8).
    let trimmed = name_bytes.split(|&b| b == 0).next().unwrap_or(name_bytes);
    let name = String::from_utf8_lossy(trimmed).into_owned();
    if name.is_empty() {
        return None;
    }
    // j_drec_val: file_id @0 (child inode oid), flags @16 (kind in low bits).
    if val.len() < 18 {
        return None;
    }
    let oid = rd_u64(val, 0);
    let dtype = rd_u16(val, 16) & DREC_TYPE_MASK;
    Some(ChildRec { name, oid, dtype })
}

impl<R: Read + Seek + Send> Filesystem for ApfsFilesystem<R> {
    fn root(&mut self) -> Result<FileEntry, FilesystemError> {
        // The APFS volume root directory is inode 2; carry that as `location`
        // so `list_directory` can find its children.
        Ok(FileEntry::new_directory(
            "/".into(),
            "/".into(),
            ROOT_DIR_INO,
        ))
    }

    fn list_directory(&mut self, entry: &FileEntry) -> Result<Vec<FileEntry>, FilesystemError> {
        if !entry.is_directory() {
            return Err(FilesystemError::NotADirectory(entry.path.clone()));
        }
        let parent_oid = entry.location;
        let parent_path = entry.path.trim_end_matches('/').to_string();
        let cat = self.catalog()?;
        let mut out = Vec::new();
        if let Some(children) = cat.children.get(&parent_oid) {
            for child in children {
                let path = format!("{parent_path}/{}", child.name);
                let mut fe = if child.dtype == DT_DIR {
                    FileEntry::new_directory(child.name.clone(), path, child.oid)
                } else if child.dtype == DT_LNK {
                    // Symlink target is read from an xattr in Phase 4; leave it
                    // empty for now so the entry still lists.
                    FileEntry::new_symlink(child.name.clone(), path, 0, child.oid, String::new())
                } else {
                    let size = cat.inodes.get(&child.oid).map(|i| i.size).unwrap_or(0);
                    FileEntry::new_file(child.name.clone(), path, size, child.oid)
                };
                // Carry a directory's own inode size as 0; files already set.
                if fe.is_directory() {
                    fe.size = 0;
                }
                out.push(fe);
            }
        }
        out.sort_by_key(|e| e.name.to_lowercase());
        Ok(out)
    }

    fn read_file(
        &mut self,
        _entry: &FileEntry,
        _max_bytes: usize,
    ) -> Result<Vec<u8>, FilesystemError> {
        Err(FilesystemError::Unsupported(
            "APFS file read not implemented yet".into(),
        ))
    }

    fn fs_type(&self) -> &str {
        "APFS"
    }

    fn volume_label(&self) -> Option<&str> {
        self.active_volume().map(|v| v.name.as_str())
    }

    fn total_size(&self) -> u64 {
        self.block_count * self.block_size as u64
    }

    fn used_size(&self) -> u64 {
        // Blocks allocated to the active volume. (A multi-volume container
        // shares free space; this reports the browsed volume's own usage.)
        self.active_volume()
            .map(|v| v.alloc_count * self.block_size as u64)
            .unwrap_or(0)
    }
}

// ---------------------------------------------------------------------------
// Detection
// ---------------------------------------------------------------------------

/// True when `buf` (a block-0 read of at least 36 bytes) holds an APFS
/// container superblock: the `NXSB` magic at offset 32 with a sane block size.
///
/// The checksum is deliberately *not* required here — detection runs on a
/// single 512/4096-byte probe read and must stay cheap; `open` does the full
/// checksum-validated checkpoint scan.
pub fn detect_apfs(buf: &[u8]) -> bool {
    if buf.len() < 40 || &buf[32..36] != NX_MAGIC {
        return false;
    }
    let block_size = rd_u32(buf, 36);
    (512..=65536).contains(&block_size) && block_size.is_power_of_two()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn detect_rejects_zeros_and_junk() {
        assert!(!detect_apfs(&[0u8; 512]));
        assert!(!detect_apfs(&[0xabu8; 512]));
    }

    #[test]
    fn detect_accepts_minimal_nxsb() {
        let mut b = vec![0u8; 512];
        b[32..36].copy_from_slice(NX_MAGIC);
        b[36..40].copy_from_slice(&4096u32.to_le_bytes());
        assert!(detect_apfs(&b));
        // implausible block size rejected
        b[36..40].copy_from_slice(&777u32.to_le_bytes());
        assert!(!detect_apfs(&b));
    }

    #[test]
    fn fletcher64_roundtrips_on_a_crafted_block() {
        // Build a 4096-byte block, compute its checksum, store it, verify.
        let mut block = vec![0u8; 4096];
        for (i, byte) in block.iter_mut().enumerate().skip(8) {
            *byte = (i * 7) as u8;
        }
        let ck = fletcher64(&block);
        block[0..8].copy_from_slice(&ck.to_le_bytes());
        assert!(checksum_valid(&block));
        // Flip a byte -> checksum must fail.
        block[100] ^= 0xff;
        assert!(!checksum_valid(&block));
    }
}
