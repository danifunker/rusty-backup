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

/// Object-type mask over the low 16 bits of `o_type`.
const OBJECT_TYPE_MASK: u32 = 0x0000_ffff;

// Object types we recognize (low 16 bits of `obj_phys.o_type`).
const OBJECT_TYPE_NX_SUPERBLOCK: u32 = 0x0001;

/// Largest container we will map: NXSB `nx_max_file_systems` bounds the volume
/// array at 100 on current APFS.
const NX_MAX_FILE_SYSTEMS: usize = 100;

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

fn rd_u32(b: &[u8], off: usize) -> u32 {
    u32::from_le_bytes([b[off], b[off + 1], b[off + 2], b[off + 3]])
}
fn rd_u64(b: &[u8], off: usize) -> u64 {
    u64::from_le_bytes(b[off..off + 8].try_into().unwrap())
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
        };

        // Prefer the newest valid superblock from the checkpoint descriptor
        // ring; fall back to the block-0 copy if the scan finds nothing newer.
        if let Some(newest) = fs.find_latest_superblock()? {
            fs.nxsb = newest;
        }
        Ok(fs)
    }

    /// One-line summary of the container's headline fields. Used by the
    /// `apfs_dump` example during driver bring-up; also the seam the later
    /// phases hang volume enumeration off of.
    pub fn debug_summary(&self) -> String {
        format!(
            "block_size={} block_count={} xp_desc_base={} container_omap_oid={} volumes={:?}",
            self.block_size,
            self.block_count,
            self.nxsb.xp_desc_base,
            self.nxsb.omap_oid,
            self.nxsb.fs_oids,
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

impl<R: Read + Seek + Send> Filesystem for ApfsFilesystem<R> {
    fn root(&mut self) -> Result<FileEntry, FilesystemError> {
        Ok(FileEntry::new_directory("/".into(), "/".into(), 0))
    }

    fn list_directory(&mut self, _entry: &FileEntry) -> Result<Vec<FileEntry>, FilesystemError> {
        // Catalog walk lands in Phase 3; until then surface a clear error
        // instead of silently reporting an empty volume.
        Err(FilesystemError::Unsupported(
            "APFS directory browsing not implemented yet — see src/fs/apfs.rs".into(),
        ))
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
        None
    }

    fn total_size(&self) -> u64 {
        self.block_count * self.block_size as u64
    }

    fn used_size(&self) -> u64 {
        // Real usage comes from the space manager / volume alloc counts in a
        // later phase; 0 until then.
        0
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
