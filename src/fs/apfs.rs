//! APFS (Apple File System) — read-only browse driver.
//!
//! Scope (see `docs/apfs_support_plan.md`): **read-only browse** of APFS
//! volumes, including **FileVault-encrypted** volumes (unlocked with the volume
//! password or personal recovery key — see [`apfs_crypto`] and
//! [`ApfsFilesystem::open_with_passphrase`]). Snapshots and any write / shrink /
//! fsck support are explicitly deferred — the trait's `Unsupported` /
//! layout-preserving defaults cover those for now.
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

use std::io::{Read, Seek, SeekFrom, Write};

use super::apfs_crypto;
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

/// Length mask over `j_file_extent_val.len_and_flags` (low 56 bits = extent
/// length in bytes).
const J_FILE_EXTENT_LEN_MASK: u64 = 0x00ff_ffff_ffff_ffff;

/// `j_xattr_val.flags` bit: the attribute data lives in a separate data
/// stream (`j_xattr_dstream`), not inline in `xdata`.
const XATTR_DATA_STREAM: u16 = 0x0001;

/// `j_xattr_val.flags` bit: the attribute data is embedded inline in `xdata`
/// (as opposed to living in a separate data stream).
const XATTR_DATA_EMBEDDED: u16 = 0x0002;

/// Extended-attribute name that stores a symlink's target path.
const SYMLINK_XATTR_NAME: &[u8] = b"com.apple.fs.symlink";

/// Extended-attribute name under which classic-Mac files keep their resource
/// fork on APFS. Its logical size becomes `FileEntry::resource_fork_size`.
const RESOURCE_FORK_XATTR_NAME: &[u8] = b"com.apple.ResourceFork";

/// Extended-attribute name holding the 32-byte Finder info (FInfo + FXInfo);
/// the type/creator `OSType`s and Finder flags come from its first 10 bytes.
const FINDER_INFO_XATTR_NAME: &[u8] = b"com.apple.FinderInfo";

/// Cap a single file read/extract so a corrupt extent list can't make us
/// allocate or stream absurd amounts. 64 GiB is far above any real vintage-Mac
/// APFS file yet bounds the damage.
const MAX_FILE_BYTES: u64 = 64 * 1024 * 1024 * 1024;

// --- FileVault encryption ---
/// `apfs_fs_flags` bit 0: the volume is **not** encrypted. When clear, the
/// volume's catalog and file data are AES-XTS-encrypted with the VEK.
const APFS_FS_UNENCRYPTED: u64 = 0x0000_0001;
/// Keybag entry tags. `KB_TAG_VOLUME_KEY` holds a volume's wrapped VEK (in the
/// container keybag); `KB_TAG_VOLUME_UNLOCK_RECORDS` holds a `prange` to the
/// volume keybag (in the container keybag) or a crypto user's KEK blob (in the
/// volume keybag).
const KB_TAG_VOLUME_KEY: u16 = 2;
const KB_TAG_VOLUME_UNLOCK_RECORDS: u16 = 3;
/// APFS keybags use 512-byte AES-XTS data units, like the rest of the volume.
const XTS_UNIT: usize = 512;

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
    /// lengths in the toc). Used by the catalog (FS-tree) walk.
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
    /// Volume UUID (`apfs_vol_uuid`) — the AES-XTS key for the volume keybag.
    uuid: [u8; 16],
    /// True when the volume is FileVault-encrypted (catalog + data ciphered).
    encrypted: bool,
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
    /// Container UUID (`nx_uuid`) — the AES-XTS key for the container keybag.
    uuid: [u8; 16],
    /// `nx_keylocker` prange: the container keybag's block + length. Zero when
    /// the container has no encrypted volumes. The keybag is a single block in
    /// practice; the length is kept for on-disk fidelity.
    keylocker_paddr: u64,
    #[allow(dead_code)]
    keylocker_blocks: u64,
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
        // nx_uuid @72; nx_keylocker prange @1296 (paddr + block count).
        let mut uuid = [0u8; 16];
        uuid.copy_from_slice(&block[72..88]);
        let (keylocker_paddr, keylocker_blocks) = if block.len() >= 1312 {
            (rd_u64(block, 1296), rd_u64(block, 1304))
        } else {
            (0, 0)
        };
        Ok(Self {
            block_size,
            block_count,
            xp_desc_base,
            xp_desc_blocks,
            omap_oid,
            fs_oids,
            uuid,
            keylocker_paddr,
            keylocker_blocks,
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
    /// Volume encryption key for the active volume, once unlocked. `None` for
    /// unencrypted volumes and for encrypted volumes opened without a
    /// passphrase (which then browse as "locked").
    vek: Option<[u8; 32]>,
}

/// The active volume's decoded catalog, cached after the first browse.
///
/// Built by a single walk of the FS (catalog) tree. `children` powers
/// `list_directory` (a parent-inode oid maps to its directory entries) and
/// `inodes` supplies per-file metadata (size, plus the data-stream id used to
/// gather file extents).
#[derive(Default)]
struct Catalog {
    /// parent inode oid -> its directory entries.
    children: std::collections::HashMap<u64, Vec<ChildRec>>,
    /// inode oid -> inode metadata.
    inodes: std::collections::HashMap<u64, InodeRec>,
    /// data-stream id (`j_inode_val.private_id`) -> file extents. Sorted by
    /// logical address at read time.
    extents: std::collections::HashMap<u64, Vec<Extent>>,
    /// inode oid -> symlink target path (from the `com.apple.fs.symlink`
    /// extended attribute).
    symlinks: std::collections::HashMap<u64, String>,
    /// inode oid -> resource fork (from the `com.apple.ResourceFork` extended
    /// attribute). Only present for files that actually carry a non-empty
    /// resource fork.
    resource_forks: std::collections::HashMap<u64, ResourceFork>,
    /// inode oid -> Mac type/creator/flags (from the `com.apple.FinderInfo`
    /// extended attribute). Only present for files that carry one.
    finder_info: std::collections::HashMap<u64, FinderInfo>,
}

/// Type/creator `OSType`s and Finder flags decoded from a `com.apple.FinderInfo`
/// xattr's leading FInfo bytes.
struct FinderInfo {
    type_code: [u8; 4],
    creator: [u8; 4],
    flags: u16,
}

/// A file's resource fork, as stored in its `com.apple.ResourceFork` xattr.
enum ResourceFork {
    /// Small fork stored inline in the xattr value — the bytes verbatim.
    Embedded(Vec<u8>),
    /// Larger fork stored as its own data stream (`j_xattr_dstream`). The
    /// bytes live in `FILE_EXTENT` records keyed by `stream_id` (already in
    /// `Catalog::extents`); `size` is the logical length.
    Stream { stream_id: u64, size: u64 },
}

impl ResourceFork {
    fn size(&self) -> u64 {
        match self {
            ResourceFork::Embedded(b) => b.len() as u64,
            ResourceFork::Stream { size, .. } => *size,
        }
    }
}

/// One `j_file_extent` record: a run of `length` bytes at logical offset
/// `logical_addr` in the file, stored starting at physical block `phys_block`
/// (0 = a sparse hole → reads as zeros).
#[derive(Clone, Copy)]
struct Extent {
    logical_addr: u64,
    length: u64,
    phys_block: u64,
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
    /// `j_inode_val.private_id` — the id file extents are keyed on.
    private_id: u64,
    /// `j_inode_val.mode` (the `S_IF*` file-type bits live here).
    #[allow(dead_code)]
    mode: u16,
}

impl<R: Read + Seek + Send> ApfsFilesystem<R> {
    /// Open an APFS container at `partition_offset`, without a passphrase.
    /// Encrypted volumes open in a "locked" state (container/volume metadata is
    /// plaintext, so the label shows, but browsing errors until unlocked).
    pub fn open(reader: R, partition_offset: u64) -> Result<Self, FilesystemError> {
        Self::open_with_passphrase(reader, partition_offset, None)
    }

    /// Open an APFS container, unlocking the first (browsed) volume with
    /// `passphrase` when it is FileVault-encrypted.
    ///
    /// Reads block 0 to learn the block size, then scans the checkpoint
    /// descriptor ring for the newest valid NXSB (block 0 alone can be stale).
    /// A wrong passphrase on an encrypted volume is an error; `None` on an
    /// encrypted volume leaves it locked (browse then reports "passphrase
    /// required"); a passphrase on an unencrypted volume is ignored.
    pub fn open_with_passphrase(
        mut reader: R,
        partition_offset: u64,
        passphrase: Option<&str>,
    ) -> Result<Self, FilesystemError> {
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
            vek: None,
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

        // Unlock the active (browsed) volume if it is encrypted and a
        // passphrase was supplied.
        if fs.active_volume().map(|v| v.encrypted).unwrap_or(false) {
            if let Some(pass) = passphrase {
                fs.vek = Some(fs.unlock_active_volume(pass)?);
            }
        }
        Ok(fs)
    }

    /// Derive the active volume's VEK from `passphrase` by walking the keybag
    /// chain: container keybag (keyed by the container UUID) → the wrapped VEK
    /// and the volume keybag location → volume keybag (keyed by the volume
    /// UUID) → the crypto user's KEK blob → PBKDF2 → unwrap KEK → unwrap VEK.
    fn unlock_active_volume(&mut self, passphrase: &str) -> Result<[u8; 32], FilesystemError> {
        let vol = self
            .active_volume()
            .ok_or_else(|| FilesystemError::Unsupported("no volume to unlock".into()))?;
        let vol_uuid = vol.uuid;
        let cont_uuid = self.nxsb.uuid;
        let kl_paddr = self.nxsb.keylocker_paddr;
        if kl_paddr == 0 {
            return Err(FilesystemError::Parse(
                "encrypted volume but the container has no keybag".into(),
            ));
        }

        // Container keybag → wrapped VEK (tag 2) + volume keybag prange (tag 3).
        let cont_kb = self.read_keybag(kl_paddr, &cont_uuid)?;
        let vek_blob = cont_kb
            .iter()
            .find(|e| e.tag == KB_TAG_VOLUME_KEY && e.uuid == vol_uuid)
            .map(|e| e.data.clone())
            .ok_or_else(|| FilesystemError::Parse("no wrapped VEK for volume".into()))?;
        let vol_kb_prange = cont_kb
            .iter()
            .find(|e| e.tag == KB_TAG_VOLUME_UNLOCK_RECORDS && e.uuid == vol_uuid)
            .map(|e| e.data.clone())
            .ok_or_else(|| FilesystemError::Parse("no volume-keybag record".into()))?;
        if vol_kb_prange.len() < 8 {
            return Err(FilesystemError::Parse("bad volume-keybag prange".into()));
        }
        let vol_kb_paddr = rd_u64(&vol_kb_prange, 0);

        // Volume keybag → the crypto user's KEK blob (tag 3).
        let vol_kb = self.read_keybag(vol_kb_paddr, &vol_uuid)?;
        let kek_blob = vol_kb
            .iter()
            .find(|e| e.tag == KB_TAG_VOLUME_UNLOCK_RECORDS)
            .map(|e| e.data.clone())
            .ok_or_else(|| FilesystemError::Parse("no KEK blob in volume keybag".into()))?;

        // Unwrap KEK from the passphrase, then unwrap the VEK from the KEK.
        let (wrapped_kek, salt, iters) = parse_kek_blob(&kek_blob)
            .ok_or_else(|| FilesystemError::Parse("malformed KEK blob".into()))?;
        let derived = apfs_crypto::pbkdf2_hmac_sha256(passphrase.as_bytes(), &salt, iters, 32);
        let derived: [u8; 32] = derived.try_into().unwrap();
        let kek = apfs_crypto::aes256_key_unwrap(&derived, &wrapped_kek).ok_or_else(|| {
            FilesystemError::InvalidData("wrong APFS passphrase (KEK unwrap failed)".into())
        })?;
        let kek: [u8; 32] = kek
            .try_into()
            .map_err(|_| FilesystemError::Parse("unexpected KEK length".into()))?;
        let wrapped_vek = parse_vek_blob(&vek_blob)
            .ok_or_else(|| FilesystemError::Parse("malformed VEK blob".into()))?;
        let vek = apfs_crypto::aes256_key_unwrap(&kek, &wrapped_vek)
            .ok_or_else(|| FilesystemError::Parse("VEK unwrap failed".into()))?;
        vek.try_into()
            .map_err(|_| FilesystemError::Parse("unexpected VEK length".into()))
    }

    /// Read and AES-XTS-decrypt a keybag at physical block `paddr`, keyed by
    /// `uuid` (`UUID‖UUID`), then parse its entries.
    fn read_keybag(
        &mut self,
        paddr: u64,
        uuid: &[u8; 16],
    ) -> Result<Vec<KeybagEntry>, FilesystemError> {
        let mut block = self.read_block(paddr)?;
        let mut key = [0u8; 32];
        key[..16].copy_from_slice(uuid);
        key[16..].copy_from_slice(uuid);
        let first_sector = paddr * (self.block_size as u64 / XTS_UNIT as u64);
        apfs_crypto::xts_decrypt(&key, first_sector as u128, XTS_UNIT, &mut block);
        Ok(parse_keybag(&block))
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
        // apfs_omap_oid @128, apfs_root_tree_oid @136, apfs_vol_uuid @240,
        // apfs_fs_flags @264, apfs_volname[256] @704.
        let incompat = rd_u64(&block, 56);
        let alloc_count = rd_u64(&block, 88);
        let omap_oid = rd_u64(&block, 128);
        let root_tree_oid = rd_u64(&block, 136);
        let mut uuid = [0u8; 16];
        uuid.copy_from_slice(&block[240..256]);
        let fs_flags = rd_u64(&block, 264);
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
            uuid,
            encrypted: fs_flags & APFS_FS_UNENCRYPTED == 0,
        })
    }

    /// The volume currently selected for browsing, if the container has one.
    fn active_volume(&self) -> Option<&VolumeInfo> {
        self.volumes.get(self.active_vol)
    }

    /// True when the active volume is encrypted but not yet unlocked.
    fn active_locked(&self) -> bool {
        self.active_volume().map(|v| v.encrypted).unwrap_or(false) && self.vek.is_none()
    }

    /// Ensure the active volume's catalog is built and return a reference to it.
    fn catalog(&mut self) -> Result<&Catalog, FilesystemError> {
        if self.active_locked() {
            return Err(FilesystemError::Unsupported(
                "APFS volume is encrypted (FileVault) — a passphrase or personal \
                 recovery key is required to browse it"
                    .into(),
            ));
        }
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
            // FS-tree nodes are VEK-encrypted on FileVault volumes (the omap,
            // read above, is not); read_block_decrypted no-ops when unencrypted.
            let node = BtreeNode::parse(self.read_block_decrypted(paddr)?)?;
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

    /// Decode one FS-tree leaf record into the catalog. Record kinds we don't
    /// consume (sibling links, dir-stats, crypto-state, …) are skipped.
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
            APFS_TYPE_FILE_EXTENT => {
                // key: j_key(8) + logical_addr u64 @8; obj_id == the file's
                // data-stream id (private_id).
                if key.len() >= 16 && val.len() >= 16 {
                    let logical_addr = rd_u64(key, 8);
                    let length = rd_u64(val, 0) & J_FILE_EXTENT_LEN_MASK;
                    let phys_block = rd_u64(val, 8);
                    cat.extents.entry(obj_id).or_default().push(Extent {
                        logical_addr,
                        length,
                        phys_block,
                    });
                }
            }
            APFS_TYPE_XATTR => {
                if let Some(target) = decode_symlink_xattr(key, val) {
                    cat.symlinks.insert(obj_id, target);
                } else if let Some(rf) = decode_resource_fork_xattr(key, val) {
                    if rf.size() > 0 {
                        cat.resource_forks.insert(obj_id, rf);
                    }
                } else if let Some(fi) = decode_finder_info_xattr(key, val) {
                    cat.finder_info.insert(obj_id, fi);
                }
            }
            _ => {}
        }
    }

    /// One-line summary of the container's headline fields (block geometry,
    /// container omap, and the enumerated volumes). Used by the `apfs_dump`
    /// developer example; not part of the browse API.
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

    /// Resolve a file inode to its logical size and logical-address-sorted
    /// extent list, ready for streaming.
    fn gather_file(&mut self, oid: u64) -> Result<(u64, Vec<Extent>), FilesystemError> {
        let cat = self.catalog()?;
        let inode = cat
            .inodes
            .get(&oid)
            .ok_or_else(|| FilesystemError::NotFound(format!("APFS inode {oid}")))?;
        let size = inode.size;
        let pid = inode.private_id;
        let mut extents = cat.extents.get(&pid).cloned().unwrap_or_default();
        extents.sort_by_key(|e| e.logical_addr);
        Ok((size, extents))
    }

    /// Stream up to `limit` bytes of a file's data (bounded by `size`) to
    /// `writer`, honoring sparse holes (unmapped ranges and `phys_block == 0`
    /// extents emit zeros). Returns the number of bytes written.
    fn stream_extents(
        &mut self,
        extents: &[Extent],
        size: u64,
        limit: u64,
        writer: &mut dyn Write,
    ) -> Result<u64, FilesystemError> {
        let limit = limit.min(size).min(MAX_FILE_BYTES);
        let mut written: u64 = 0;
        let mut logical_pos: u64 = 0;
        for ext in extents {
            if written >= limit {
                break;
            }
            // Sparse gap before this extent -> zeros.
            if ext.logical_addr > logical_pos {
                let gap = (ext.logical_addr - logical_pos).min(limit - written);
                written += write_zeros(writer, gap)?;
                logical_pos = ext.logical_addr;
                if written >= limit {
                    break;
                }
            }
            let take = ext.length.min(limit - written);
            if ext.phys_block == 0 {
                written += write_zeros(writer, take)?;
            } else {
                self.stream_phys(ext.phys_block, take, writer)?;
                written += take;
            }
            logical_pos = logical_pos.saturating_add(ext.length);
        }
        // Sparse tail up to the requested limit.
        if written < limit {
            written += write_zeros(writer, limit - written)?;
        }
        Ok(written)
    }

    /// Stream `byte_len` bytes starting at physical block `phys_block` to
    /// `writer`. Reads are issued at block-aligned offsets in block-multiple
    /// sizes (so raw character devices, which reject unaligned reads, are
    /// happy) while only `byte_len` bytes are forwarded.
    fn stream_phys(
        &mut self,
        phys_block: u64,
        byte_len: u64,
        writer: &mut dyn Write,
    ) -> Result<(), FilesystemError> {
        let bs = self.block_size as u64;
        // ~1 MiB scratch, rounded to a whole number of blocks.
        let chunk_blocks = (1024 * 1024 / bs).max(1);
        let mut buf = vec![0u8; (chunk_blocks * bs) as usize];
        let mut remaining = byte_len;
        let mut blk = phys_block;
        while remaining > 0 {
            let need_blocks = remaining.div_ceil(bs).min(chunk_blocks);
            let read_bytes = (need_blocks * bs) as usize;
            let pos = self.partition_offset + blk * bs;
            self.reader.seek(SeekFrom::Start(pos))?;
            self.reader.read_exact(&mut buf[..read_bytes])?;
            // On an encrypted volume, decrypt each data block with the VEK,
            // tweak = the block's physical sector index.
            if let Some(vek) = self.vek {
                let units = bs / XTS_UNIT as u64;
                for j in 0..need_blocks {
                    let start = (j * bs) as usize;
                    let first_sector = (blk + j) * units;
                    apfs_crypto::xts_decrypt(
                        &vek,
                        first_sector as u128,
                        XTS_UNIT,
                        &mut buf[start..start + bs as usize],
                    );
                }
            }
            let to_write = remaining.min(read_bytes as u64) as usize;
            writer.write_all(&buf[..to_write])?;
            remaining -= to_write as u64;
            blk += need_blocks;
        }
        Ok(())
    }

    /// Read a single object block by physical block address (plaintext — used
    /// for container/omap/APSB metadata, which stays unencrypted).
    fn read_block(&mut self, paddr: u64) -> Result<Vec<u8>, FilesystemError> {
        let mut buf = vec![0u8; self.block_size as usize];
        let pos = self.partition_offset + paddr * self.block_size as u64;
        self.reader.seek(SeekFrom::Start(pos))?;
        self.reader.read_exact(&mut buf)?;
        Ok(buf)
    }

    /// Read an object block, decrypting it with the VEK when the active volume
    /// is unlocked-encrypted. Used for FS-tree (catalog) nodes, which are
    /// ciphered on FileVault volumes; a no-op otherwise.
    fn read_block_decrypted(&mut self, paddr: u64) -> Result<Vec<u8>, FilesystemError> {
        let mut buf = self.read_block(paddr)?;
        if let Some(vek) = self.vek {
            let first_sector = paddr * (self.block_size as u64 / XTS_UNIT as u64);
            apfs_crypto::xts_decrypt(&vek, first_sector as u128, XTS_UNIT, &mut buf);
        }
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

// ---------------------------------------------------------------------------
// Keybag + DER (BER TLV) parsing for the FileVault unlock chain
// ---------------------------------------------------------------------------

/// One decrypted keybag entry (`keybag_entry_t`).
struct KeybagEntry {
    uuid: [u8; 16],
    tag: u16,
    data: Vec<u8>,
}

/// Parse a decrypted keybag block (`kb_locker` at obj_phys offset 32) into its
/// entries. Each `keybag_entry_t` is `uuid[16] tag:u16 keylen:u16 pad[4]
/// keydata[keylen]`, padded to a 16-byte boundary.
fn parse_keybag(block: &[u8]) -> Vec<KeybagEntry> {
    let mut out = Vec::new();
    if block.len() < 48 {
        return out;
    }
    let nkeys = rd_u16(block, 34) as usize;
    let mut p = 48usize;
    for _ in 0..nkeys {
        if p + 24 > block.len() {
            break;
        }
        let mut uuid = [0u8; 16];
        uuid.copy_from_slice(&block[p..p + 16]);
        let tag = rd_u16(block, p + 16);
        let keylen = rd_u16(block, p + 18) as usize;
        let end = (p + 24 + keylen).min(block.len());
        let data = block[p + 24..end].to_vec();
        out.push(KeybagEntry { uuid, tag, data });
        p += (24 + keylen + 15) & !15; // 16-byte aligned entries
    }
    out
}

/// Minimal BER/DER TLV split: return each top-level `(tag, value)` in `b`.
/// APFS KEK/VEK blobs use only definite-length short/long forms.
fn der_children(b: &[u8]) -> Vec<(u8, &[u8])> {
    let mut out = Vec::new();
    let mut p = 0usize;
    while p + 2 <= b.len() {
        let tag = b[p];
        let mut len = b[p + 1] as usize;
        let mut hdr = 2;
        if len & 0x80 != 0 {
            let n = len & 0x7f;
            if n == 0 || n > 4 || p + 2 + n > b.len() {
                break;
            }
            len = 0;
            for i in 0..n {
                len = (len << 8) | b[p + 2 + i] as usize;
            }
            hdr = 2 + n;
        }
        let end = (p + hdr + len).min(b.len());
        out.push((tag, &b[p + hdr..end]));
        p = end;
    }
    out
}

/// The DER context tag holding the inner KEK/VEK record inside the blob.
const DER_TAG_KEY_RECORD: u8 = 0xa3;
/// Field tags within the KEK/VEK record.
const DER_TAG_WRAPPED_KEY: u8 = 0x83;
const DER_TAG_ITERATIONS: u8 = 0x84;
const DER_TAG_SALT: u8 = 0x85;

/// Descend a KEK/VEK blob to the inner record (`0x30` SEQUENCE → `0xA3`).
fn key_record(blob: &[u8]) -> Option<Vec<(u8, Vec<u8>)>> {
    let outer = der_children(blob)
        .into_iter()
        .find(|(t, _)| *t == 0x30)?
        .1
        .to_vec();
    let record = der_children(&outer)
        .into_iter()
        .find(|(t, _)| *t == DER_TAG_KEY_RECORD)?
        .1
        .to_vec();
    Some(
        der_children(&record)
            .into_iter()
            .map(|(t, v)| (t, v.to_vec()))
            .collect(),
    )
}

/// Parse a KEK blob → `(wrapped_kek, salt, iterations)`.
fn parse_kek_blob(blob: &[u8]) -> Option<(Vec<u8>, Vec<u8>, u32)> {
    let fields = key_record(blob)?;
    let get = |tag: u8| {
        fields
            .iter()
            .find(|(t, _)| *t == tag)
            .map(|(_, v)| v.clone())
    };
    let wrapped = get(DER_TAG_WRAPPED_KEY)?;
    let salt = get(DER_TAG_SALT)?;
    let iters_bytes = get(DER_TAG_ITERATIONS)?;
    let mut iters = 0u32;
    for b in &iters_bytes {
        iters = (iters << 8) | *b as u32;
    }
    Some((wrapped, salt, iters))
}

/// Parse a VEK blob → the wrapped VEK (unwrapped by the KEK, no PBKDF2).
fn parse_vek_blob(blob: &[u8]) -> Option<Vec<u8>> {
    let fields = key_record(blob)?;
    fields
        .iter()
        .find(|(t, _)| *t == DER_TAG_WRAPPED_KEY)
        .map(|(_, v)| v.clone())
}

/// Write `count` zero bytes to `writer` in bounded chunks. Returns `count`.
fn write_zeros(writer: &mut dyn Write, count: u64) -> Result<u64, FilesystemError> {
    let zeros = [0u8; 8192];
    let mut remaining = count;
    while remaining > 0 {
        let n = remaining.min(zeros.len() as u64) as usize;
        writer.write_all(&zeros[..n])?;
        remaining -= n as u64;
    }
    Ok(count)
}

/// Decode a `com.apple.fs.symlink` extended-attribute record into its target
/// path. Returns `None` for any other xattr or a non-embedded value.
fn decode_symlink_xattr(key: &[u8], val: &[u8]) -> Option<String> {
    // key: j_key(8) + name_len u16 @8 + name bytes @10 (NUL-terminated).
    if key.len() < 10 {
        return None;
    }
    let name_len = rd_u16(key, 8) as usize;
    let end = 10usize.checked_add(name_len)?.min(key.len());
    let name = key.get(10..end)?;
    let name = name.split(|&b| b == 0).next().unwrap_or(name);
    if name != SYMLINK_XATTR_NAME {
        return None;
    }
    // val: j_xattr_val: flags u16 @0, xdata_len u16 @2, xdata @4.
    if val.len() < 4 {
        return None;
    }
    let flags = rd_u16(val, 0);
    let xdata_len = rd_u16(val, 2) as usize;
    if flags & XATTR_DATA_EMBEDDED == 0 {
        return None; // stream-backed symlink target is not expected in practice
    }
    let xdata = val.get(4..4 + xdata_len.min(val.len().saturating_sub(4)))?;
    let xdata = xdata.split(|&b| b == 0).next().unwrap_or(xdata);
    Some(String::from_utf8_lossy(xdata).into_owned())
}

/// Decode a `com.apple.ResourceFork` extended-attribute record into a
/// [`ResourceFork`]. Returns `None` for any other xattr.
///
/// Two storage forms:
/// - **embedded** (`XATTR_DATA_EMBEDDED`): the fork bytes are inline in
///   `xdata` — captured verbatim. Small forks only.
/// - **stream** (`XATTR_DATA_STREAM`): `xdata` is a `j_xattr_dstream` —
///   `xattr_obj_id` (u64) followed by a `j_dstream_t` whose first u64 is the
///   logical size. The fork bytes live in `FILE_EXTENT` records keyed by that
///   `xattr_obj_id`. This is the common case for real resource forks.
fn decode_resource_fork_xattr(key: &[u8], val: &[u8]) -> Option<ResourceFork> {
    // key: j_key(8) + name_len u16 @8 + name bytes @10 (NUL-terminated).
    if key.len() < 10 {
        return None;
    }
    let name_len = rd_u16(key, 8) as usize;
    let end = 10usize.checked_add(name_len)?.min(key.len());
    let name = key.get(10..end)?;
    let name = name.split(|&b| b == 0).next().unwrap_or(name);
    if name != RESOURCE_FORK_XATTR_NAME {
        return None;
    }
    // val: j_xattr_val: flags u16 @0, xdata_len u16 @2, xdata @4.
    if val.len() < 4 {
        return None;
    }
    let flags = rd_u16(val, 0);
    let xdata_len = rd_u16(val, 2) as usize;
    let xdata = val.get(4..)?;
    if flags & XATTR_DATA_EMBEDDED != 0 {
        let take = xdata_len.min(xdata.len());
        return Some(ResourceFork::Embedded(xdata[..take].to_vec()));
    }
    if flags & XATTR_DATA_STREAM != 0 {
        // j_xattr_dstream: xattr_obj_id u64 @0, then j_dstream_t; dstream.size
        // is the first u64 of the j_dstream_t, i.e. at offset 8 of xdata.
        if xdata.len() >= 16 {
            let stream_id = rd_u64(xdata, 0);
            let size = rd_u64(xdata, 8);
            return Some(ResourceFork::Stream { stream_id, size });
        }
    }
    None
}

/// Decode a `com.apple.FinderInfo` extended-attribute record into type/creator
/// `OSType`s and Finder flags. The 32-byte value is the classic FInfo+FXInfo:
/// `fdType` @0, `fdCreator` @4, `fdFlags` (big-endian u16) @8. Always embedded.
/// Returns `None` for any other xattr or a too-short value.
fn decode_finder_info_xattr(key: &[u8], val: &[u8]) -> Option<FinderInfo> {
    if key.len() < 10 {
        return None;
    }
    let name_len = rd_u16(key, 8) as usize;
    let end = 10usize.checked_add(name_len)?.min(key.len());
    let name = key.get(10..end)?;
    let name = name.split(|&b| b == 0).next().unwrap_or(name);
    if name != FINDER_INFO_XATTR_NAME {
        return None;
    }
    // val: j_xattr_val header (flags u16 @0, xdata_len u16 @2), then xdata.
    let xdata = val.get(4..)?;
    if xdata.len() < 10 {
        return None;
    }
    let mut type_code = [0u8; 4];
    let mut creator = [0u8; 4];
    type_code.copy_from_slice(&xdata[0..4]);
    creator.copy_from_slice(&xdata[4..8]);
    // Finder flags are stored big-endian in the on-disk FInfo.
    let flags = u16::from_be_bytes([xdata[8], xdata[9]]);
    Some(FinderInfo {
        type_code,
        creator,
        flags,
    })
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
                let fe = if child.dtype == DT_DIR {
                    FileEntry::new_directory(child.name.clone(), path, child.oid)
                } else if child.dtype == DT_LNK {
                    let target = cat.symlinks.get(&child.oid).cloned().unwrap_or_default();
                    let size = target.len() as u64;
                    FileEntry::new_symlink(child.name.clone(), path, size, child.oid, target)
                } else {
                    let size = cat.inodes.get(&child.oid).map(|i| i.size).unwrap_or(0);
                    let mut fe = FileEntry::new_file(child.name.clone(), path, size, child.oid);
                    // Classic-Mac resource fork, stored as the
                    // `com.apple.ResourceFork` xattr (read via
                    // `write_resource_fork_to`).
                    fe.resource_fork_size = cat.resource_forks.get(&child.oid).map(|rf| rf.size());
                    // Mac type/creator/flags from the `com.apple.FinderInfo` xattr.
                    if let Some(fi) = cat.finder_info.get(&child.oid) {
                        fe.type_code = Some(fi.type_code);
                        fe.creator_code = Some(fi.creator);
                        fe.finder_flags = Some(fi.flags);
                    }
                    fe
                };
                out.push(fe);
            }
        }
        out.sort_by_key(|e| e.name.to_lowercase());
        Ok(out)
    }

    fn read_file(
        &mut self,
        entry: &FileEntry,
        max_bytes: usize,
    ) -> Result<Vec<u8>, FilesystemError> {
        if entry.is_symlink() {
            // A symlink's "contents" is its target path.
            let cat = self.catalog()?;
            let target = cat
                .symlinks
                .get(&entry.location)
                .cloned()
                .unwrap_or_default();
            let mut bytes = target.into_bytes();
            bytes.truncate(max_bytes);
            return Ok(bytes);
        }
        let (size, extents) = self.gather_file(entry.location)?;
        let limit = size.min(max_bytes as u64);
        let mut out = Vec::with_capacity(limit.min(1 << 20) as usize);
        self.stream_extents(&extents, size, limit, &mut out)?;
        Ok(out)
    }

    fn write_file_to(
        &mut self,
        entry: &FileEntry,
        writer: &mut dyn Write,
    ) -> Result<u64, FilesystemError> {
        if entry.is_symlink() {
            let cat = self.catalog()?;
            let target = cat
                .symlinks
                .get(&entry.location)
                .cloned()
                .unwrap_or_default();
            writer.write_all(target.as_bytes())?;
            return Ok(target.len() as u64);
        }
        let (size, extents) = self.gather_file(entry.location)?;
        self.stream_extents(&extents, size, size, writer)
    }

    fn resource_fork_size(&mut self, entry: &FileEntry) -> u64 {
        match self.catalog() {
            Ok(cat) => cat
                .resource_forks
                .get(&entry.location)
                .map(|rf| rf.size())
                .unwrap_or(0),
            Err(_) => 0,
        }
    }

    /// Stream the file's `com.apple.ResourceFork` bytes. Embedded forks are
    /// written from the inline xattr bytes; stream-backed forks are read from
    /// their own `FILE_EXTENT` records (keyed by the xattr's stream id) exactly
    /// like a data fork — sparse-hole and on-encrypted-volume decryption reuse
    /// `stream_extents`. Returns `Ok(0)` when the file has no resource fork.
    fn write_resource_fork_to(
        &mut self,
        entry: &FileEntry,
        writer: &mut dyn Write,
    ) -> Result<u64, FilesystemError> {
        // Pull what we need out of the (borrowed) catalog into owned locals so
        // the later `&mut self` stream call doesn't overlap the borrow.
        let plan = {
            let cat = self.catalog()?;
            match cat.resource_forks.get(&entry.location) {
                None => return Ok(0),
                Some(ResourceFork::Embedded(bytes)) => Ok(bytes.clone()),
                Some(ResourceFork::Stream { stream_id, size }) => {
                    let mut extents = cat.extents.get(stream_id).cloned().unwrap_or_default();
                    extents.sort_by_key(|e| e.logical_addr);
                    Err((extents, *size))
                }
            }
        };
        match plan {
            Ok(bytes) => {
                writer.write_all(&bytes)?;
                Ok(bytes.len() as u64)
            }
            Err((extents, size)) => self.stream_extents(&extents, size, size, writer),
        }
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
