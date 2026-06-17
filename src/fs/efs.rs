//! SGI EFS (Extent File System) read-only support.
//!
//! Implements Steps 3 and 4 of the SGI Filesystem plan: superblock parsing,
//! inode lookup, directory listing (recursive), file reads via inline
//! extents, streaming writes, and symlink-target resolution.
//!
//! EFS stores up to 12 extents inline in the inode. When a file needs more
//! than 12 extents, EFS switches to **indirect** mode: the inode's extent
//! slots no longer describe data, they describe runs of disk blocks that
//! themselves contain arrays of `efs_extent` records (64 per 512-byte
//! block). `numextents` is the total data-extent count; `extents[0].offset`
//! is hijacked to hold `direxts`, the number of inode slots actually used
//! to point at indirect blocks. See Linux `fs/efs/inode.c::efs_map_block`.
//!
//! References:
//! - `~/xfs-efs/refs/efs-linux-5.15/efs/` — Linux kernel v5.15 EFS sources.
//! - `docs/SGI_Filesystems.md` — implementation plan and on-disk notes.

use std::io::{Read, Seek, SeekFrom, Write};

use byteorder::{BigEndian, ByteOrder};

use super::entry::{EntryType, FileEntry};
use super::filesystem::{Filesystem, FilesystemError};

pub(crate) const EFS_BLOCKSIZE: u64 = 512;
const EFS_INODESIZE: u64 = 128;
const EFS_INODES_PER_BLOCK: u64 = EFS_BLOCKSIZE / EFS_INODESIZE; // 4
const EFS_DIRECTEXTENTS: usize = 12;
/// Public alias of the 12-extent hard cap. Used by the fsck verifier
/// and any other cross-module callers.
pub const EFS_DIRECTEXTENTS_MAX: usize = EFS_DIRECTEXTENTS;
/// On-disk size of the EFS superblock, per IRIX `struct efs_super` (with
/// the implicit 2-byte natural-alignment pad between `fs_dirty` and
/// `fs_time`). 92 bytes; the surrounding 512-byte sector is zero-padded.
pub const EFS_SUPERBLOCK_SIZE: usize = 92;

const EFS_MAGIC_OLD: u32 = 0x00072959;
const EFS_MAGIC_NEW: u32 = 0x0007295A;

const EFS_DIRBLK_MAGIC: u16 = 0xBEEF;
const EFS_DIRBLK_HEADERSIZE: usize = 4;

const EFS_ROOT_INODE: u32 = 2;

/// EFS superblock. Lives at byte 512 of the partition.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EfsSuperblock {
    pub fs_size: u32,    // total size in 512-byte blocks
    pub firstcg: u32,    // block of first cylinder group
    pub cgfsize: u32,    // blocks per cylinder group
    pub cgisize: u16,    // inode blocks per cylinder group
    pub sectors: u16,    // sectors per track
    pub heads: u16,      // heads per cylinder
    pub ncg: u16,        // number of cylinder groups
    pub dirty: u16,      // dirty flag
    pub fs_time: u32,    // last mount time
    pub magic: u32,      // EFS_MAGIC_OLD or EFS_MAGIC_NEW
    pub fname: [u8; 6],  // volume name
    pub fpack: [u8; 6],  // pack name
    pub bmsize: u32,     // size of bitmap in bytes
    pub tfree: u32,      // total free data blocks
    pub tinode: u32,     // total free inodes
    pub bmblock: u32,    // bitmap location (block number)
    pub replsb: u32,     // location of replicated superblock (block)
    pub lastialloc: u32, // last allocated inode (hint, not authoritative)
    pub checksum: u32,   // checksum of volume portion of fs
}

impl EfsSuperblock {
    /// Parse a 92-byte superblock buffer.
    pub fn parse(buf: &[u8]) -> Result<Self, FilesystemError> {
        if buf.len() < EFS_SUPERBLOCK_SIZE {
            return Err(FilesystemError::Parse(format!(
                "EFS superblock buffer too small: {} bytes",
                buf.len()
            )));
        }
        // The C `struct efs_super` is NOT __attribute__((packed)), so the
        // compiler inserts 2 bytes of natural-alignment padding between
        // `fs_dirty` (be16 at +20) and `fs_time` (be32 at +24). Read by
        // explicit byte offset rather than as a packed struct.
        let magic = BigEndian::read_u32(&buf[28..32]);
        if magic != EFS_MAGIC_OLD && magic != EFS_MAGIC_NEW {
            return Err(FilesystemError::Parse(format!(
                "bad EFS magic: 0x{magic:08X} (expected 0x{EFS_MAGIC_OLD:08X} or 0x{EFS_MAGIC_NEW:08X})"
            )));
        }
        let mut fname = [0u8; 6];
        fname.copy_from_slice(&buf[32..38]);
        let mut fpack = [0u8; 6];
        fpack.copy_from_slice(&buf[38..44]);
        Ok(EfsSuperblock {
            fs_size: BigEndian::read_u32(&buf[0..4]),
            firstcg: BigEndian::read_u32(&buf[4..8]),
            cgfsize: BigEndian::read_u32(&buf[8..12]),
            cgisize: BigEndian::read_u16(&buf[12..14]),
            sectors: BigEndian::read_u16(&buf[14..16]),
            heads: BigEndian::read_u16(&buf[16..18]),
            ncg: BigEndian::read_u16(&buf[18..20]),
            dirty: BigEndian::read_u16(&buf[20..22]),
            fs_time: BigEndian::read_u32(&buf[24..28]),
            magic,
            fname,
            fpack,
            bmsize: BigEndian::read_u32(&buf[44..48]),
            tfree: BigEndian::read_u32(&buf[48..52]),
            tinode: BigEndian::read_u32(&buf[52..56]),
            bmblock: BigEndian::read_u32(&buf[56..60]),
            replsb: BigEndian::read_u32(&buf[60..64]),
            lastialloc: BigEndian::read_u32(&buf[64..68]),
            checksum: BigEndian::read_u32(&buf[88..92]),
        })
    }

    /// Serialize this superblock into `buf` at offsets 0..92. `buf` must
    /// be at least `EFS_SUPERBLOCK_SIZE` bytes; the implicit alignment
    /// pad at offsets 22..24 and the reserved `fs_spare` region at
    /// 68..88 are zeroed. The caller is responsible for the surrounding
    /// 512-byte sector (typically: read sector, mutate in place via
    /// `write_into`, write sector back).
    pub fn write_into(&self, buf: &mut [u8]) {
        assert!(
            buf.len() >= EFS_SUPERBLOCK_SIZE,
            "EFS superblock buffer must be >= {EFS_SUPERBLOCK_SIZE} bytes"
        );
        BigEndian::write_u32(&mut buf[0..4], self.fs_size);
        BigEndian::write_u32(&mut buf[4..8], self.firstcg);
        BigEndian::write_u32(&mut buf[8..12], self.cgfsize);
        BigEndian::write_u16(&mut buf[12..14], self.cgisize);
        BigEndian::write_u16(&mut buf[14..16], self.sectors);
        BigEndian::write_u16(&mut buf[16..18], self.heads);
        BigEndian::write_u16(&mut buf[18..20], self.ncg);
        BigEndian::write_u16(&mut buf[20..22], self.dirty);
        // Natural-alignment pad before fs_time.
        buf[22..24].fill(0);
        BigEndian::write_u32(&mut buf[24..28], self.fs_time);
        BigEndian::write_u32(&mut buf[28..32], self.magic);
        buf[32..38].copy_from_slice(&self.fname);
        buf[38..44].copy_from_slice(&self.fpack);
        BigEndian::write_u32(&mut buf[44..48], self.bmsize);
        BigEndian::write_u32(&mut buf[48..52], self.tfree);
        BigEndian::write_u32(&mut buf[52..56], self.tinode);
        BigEndian::write_u32(&mut buf[56..60], self.bmblock);
        BigEndian::write_u32(&mut buf[60..64], self.replsb);
        BigEndian::write_u32(&mut buf[64..68], self.lastialloc);
        buf[68..88].fill(0); // fs_spare — kernel says MUST BE ZERO
        BigEndian::write_u32(&mut buf[88..92], self.checksum);
    }

    fn label(&self) -> String {
        let n = trim_ascii(&self.fname);
        let p = trim_ascii(&self.fpack);
        if n.is_empty() && p.is_empty() {
            String::new()
        } else if p.is_empty() {
            n
        } else if n.is_empty() {
            p
        } else {
            format!("{n}:{p}")
        }
    }
}

/// A single on-disk extent (8 bytes, big-endian).
#[derive(Debug, Clone, Copy)]
pub struct EfsExtent {
    pub magic: u8,
    pub bn: u32,     // start block on disk (24 bits)
    pub length: u8,  // number of 512-byte blocks
    pub offset: u32, // logical file offset in blocks (24 bits)
}

impl EfsExtent {
    fn parse(buf: &[u8; 8]) -> Self {
        let w0 = BigEndian::read_u32(&buf[0..4]);
        let w1 = BigEndian::read_u32(&buf[4..8]);
        EfsExtent {
            magic: ((w0 >> 24) & 0xFF) as u8,
            bn: w0 & 0x00FF_FFFF,
            length: ((w1 >> 24) & 0xFF) as u8,
            offset: w1 & 0x00FF_FFFF,
        }
    }
}

/// Parsed EFS on-disk inode (128 bytes).
#[derive(Debug, Clone)]
pub struct EfsInode {
    pub inum: u32,
    pub mode: u16,
    pub nlink: u16,
    pub uid: u16,
    pub gid: u16,
    pub size: u32,
    pub atime: u32,
    pub mtime: u32,
    pub ctime: u32,
    pub gen: u32,
    pub numextents: u16,
    pub version: u8,
    pub extents: [EfsExtent; EFS_DIRECTEXTENTS],
}

impl EfsInode {
    fn parse(inum: u32, buf: &[u8; 128]) -> Self {
        let mut extents = [EfsExtent {
            magic: 0,
            bn: 0,
            length: 0,
            offset: 0,
        }; EFS_DIRECTEXTENTS];
        for (i, ext) in extents.iter_mut().enumerate() {
            let off = 32 + i * 8;
            let chunk: &[u8; 8] = buf[off..off + 8].try_into().unwrap();
            *ext = EfsExtent::parse(chunk);
        }
        EfsInode {
            inum,
            mode: BigEndian::read_u16(&buf[0..2]),
            nlink: BigEndian::read_u16(&buf[2..4]),
            uid: BigEndian::read_u16(&buf[4..6]),
            gid: BigEndian::read_u16(&buf[6..8]),
            size: BigEndian::read_u32(&buf[8..12]),
            atime: BigEndian::read_u32(&buf[12..16]),
            mtime: BigEndian::read_u32(&buf[16..20]),
            ctime: BigEndian::read_u32(&buf[20..24]),
            gen: BigEndian::read_u32(&buf[24..28]),
            numextents: BigEndian::read_u16(&buf[28..30]),
            version: buf[30],
            extents,
        }
    }

    pub(crate) fn is_dir(&self) -> bool {
        (self.mode & 0o170000) == 0o040000
    }

    fn is_symlink(&self) -> bool {
        (self.mode & 0o170000) == 0o120000
    }

    fn is_regular(&self) -> bool {
        (self.mode & 0o170000) == 0o100000
    }

    fn entry_type(&self) -> EntryType {
        if self.is_dir() {
            EntryType::Directory
        } else if self.is_symlink() {
            EntryType::Symlink
        } else if self.is_regular() {
            EntryType::File
        } else {
            EntryType::Special
        }
    }

    fn special_type(&self) -> Option<String> {
        match self.mode & 0o170000 {
            0o020000 => Some("char device".into()),
            0o060000 => Some("block device".into()),
            0o010000 => Some("fifo".into()),
            0o140000 => Some("socket".into()),
            _ => None,
        }
    }

    /// Serialize this inode into a 128-byte buffer. `extents[i]` slots
    /// past `numextents` are emitted as on-disk extents too — the read
    /// path treats `magic != 0` as corrupt so unused slots must stay
    /// zeroed by the caller (the EFS_DIRECTEXTENTS array is fixed
    /// size on disk).
    #[allow(dead_code)] // wired up in EFS Slice 3 (write_inode)
    pub(crate) fn write_into(&self, buf: &mut [u8; 128]) {
        BigEndian::write_u16(&mut buf[0..2], self.mode);
        BigEndian::write_u16(&mut buf[2..4], self.nlink);
        BigEndian::write_u16(&mut buf[4..6], self.uid);
        BigEndian::write_u16(&mut buf[6..8], self.gid);
        BigEndian::write_u32(&mut buf[8..12], self.size);
        BigEndian::write_u32(&mut buf[12..16], self.atime);
        BigEndian::write_u32(&mut buf[16..20], self.mtime);
        BigEndian::write_u32(&mut buf[20..24], self.ctime);
        BigEndian::write_u32(&mut buf[24..28], self.gen);
        BigEndian::write_u16(&mut buf[28..30], self.numextents);
        buf[30] = self.version;
        buf[31] = 0; // pad — IRIX leaves it zero
        for (i, ext) in self.extents.iter().enumerate() {
            let off = 32 + i * 8;
            let w0 = ((ext.magic as u32) << 24) | (ext.bn & 0x00FF_FFFF);
            let w1 = ((ext.length as u32) << 24) | (ext.offset & 0x00FF_FFFF);
            BigEndian::write_u32(&mut buf[off..off + 4], w0);
            BigEndian::write_u32(&mut buf[off + 4..off + 8], w1);
        }
    }

    /// Construct an empty/free inode (mode=0). Used when initializing
    /// freshly-allocated CG inode-table regions or zeroing an inode
    /// on delete.
    #[allow(dead_code)] // wired up in EFS Slice 3+
    pub(crate) fn empty(inum: u32) -> Self {
        EfsInode {
            inum,
            mode: 0,
            nlink: 0,
            uid: 0,
            gid: 0,
            size: 0,
            atime: 0,
            mtime: 0,
            ctime: 0,
            gen: 0,
            numextents: 0,
            version: 0,
            extents: [EfsExtent {
                magic: 0,
                bn: 0,
                length: 0,
                offset: 0,
            }; EFS_DIRECTEXTENTS],
        }
    }
}

/// EFS filesystem reader + (with `R: Write`) editor.
///
/// The free-space bitmap is large (~915 KiB on a 1 GB volume) and
/// re-reading it on every mutation would dominate edit-session cost.
/// Once loaded for mutation it stays in `staged_bitmap`; `sync_metadata`
/// flushes it. Inode and dirblock writes go through to disk
/// immediately (single 512-byte RMW each — cheap, no batching upside).
pub struct EfsFilesystem<R: Read + Seek> {
    reader: R,
    partition_offset: u64,
    sb: EfsSuperblock,
    label: String,
    staged_bitmap: Option<Vec<u8>>,
    /// True when `sb` has been mutated in memory since the last sync.
    sb_dirty: bool,
}

impl<R: Read + Seek> EfsFilesystem<R> {
    /// Open an EFS filesystem at the given byte offset within `reader`.
    pub fn open(mut reader: R, partition_offset: u64) -> Result<Self, FilesystemError> {
        // Superblock lives at sector 1 of the partition. Read a full
        // sector via the aligned helper.
        let mut sector = [0u8; EFS_BLOCKSIZE as usize];
        read_at_aligned(
            &mut reader,
            partition_offset + EFS_BLOCKSIZE,
            EFS_BLOCKSIZE,
            &mut sector,
        )?;
        let sb = EfsSuperblock::parse(&sector)?;
        let label = sb.label();
        Ok(EfsFilesystem {
            reader,
            partition_offset,
            sb,
            label,
            staged_bitmap: None,
            sb_dirty: false,
        })
    }

    /// Compute the byte offset (from start of partition) of inode `inum`.
    pub(crate) fn inode_byte_offset(&self, inum: u32) -> u64 {
        // Mirrors `efs_iget` in Linux v5.15:
        //   inodes_per_cg = cgisize * (BLOCKSIZE / INODESIZE)
        //   cg            = inum / inodes_per_cg
        //   inblock       = (inum % inodes_per_cg) / (BLOCKSIZE / INODESIZE)
        //   block         = firstcg + cg * cgfsize + inblock
        //   off           = (inum % (BLOCKSIZE / INODESIZE)) << INODESIZE_BITS
        let inodes_per_cg = self.sb.cgisize as u64 * EFS_INODES_PER_BLOCK;
        let cg = inum as u64 / inodes_per_cg;
        let off_in_cg = inum as u64 % inodes_per_cg;
        let inblock = off_in_cg / EFS_INODES_PER_BLOCK;
        let block = self.sb.firstcg as u64 + cg * self.sb.cgfsize as u64 + inblock;
        let byte_in_block = (inum as u64 % EFS_INODES_PER_BLOCK) * EFS_INODESIZE;
        block * EFS_BLOCKSIZE + byte_in_block
    }

    /// Read a single inode by number.
    pub(crate) fn read_inode(&mut self, inum: u32) -> Result<EfsInode, FilesystemError> {
        if inum == 0 {
            return Err(FilesystemError::InvalidData(
                "EFS inode 0 is reserved".into(),
            ));
        }
        let off = self.inode_byte_offset(inum);
        // Read the 512-byte block containing this inode and slice out 128 bytes.
        let mut block = [0u8; EFS_BLOCKSIZE as usize];
        let block_byte = (off / EFS_BLOCKSIZE) * EFS_BLOCKSIZE;
        read_at_aligned(
            &mut self.reader,
            self.partition_offset + block_byte,
            EFS_BLOCKSIZE,
            &mut block,
        )?;
        let in_block = (off - block_byte) as usize;
        let mut ino_buf = [0u8; 128];
        ino_buf.copy_from_slice(&block[in_block..in_block + 128]);
        Ok(EfsInode::parse(inum, &ino_buf))
    }

    /// Clone the in-memory superblock. Used by fsck as a stable
    /// snapshot of the geometry before the iteration begins.
    pub(crate) fn sb_clone(&self) -> EfsSuperblock {
        self.sb.clone()
    }

    /// Replace the in-memory superblock and mark it dirty so the next
    /// `sync_metadata` writes the pair. Used by the grow / shrink
    /// paths in `efs_resize`.
    pub(crate) fn set_sb(&mut self, new_sb: EfsSuperblock) {
        self.sb = new_sb;
        self.sb_dirty = true;
    }

    /// Unwrap to the underlying reader. Used by tests to round-trip
    /// the in-memory image through a re-open without giving up the
    /// privacy of `reader`.
    pub fn reader_into_inner(self) -> R {
        self.reader
    }

    pub(crate) fn raw_reader_mut(&mut self) -> &mut R {
        &mut self.reader
    }

    pub(crate) fn partition_offset_value(&self) -> u64 {
        self.partition_offset
    }

    /// Read the bitmap via the read-only path. Used by fsck.
    /// Prefers `staged_bitmap` when present — this is what lets
    /// "fsck before sync" produce the correct verdict on the
    /// prospective state during grow/shrink, because the bytes on
    /// disk at `sb.bmblock` may not yet match the pending edits.
    pub(crate) fn read_bitmap_readonly(&mut self) -> Result<Vec<u8>, FilesystemError> {
        if let Some(bm) = &self.staged_bitmap {
            return Ok(bm.clone());
        }
        let bmsize = self.sb.bmsize as usize;
        if bmsize == 0 {
            return Err(FilesystemError::InvalidData("EFS bitmap size is 0".into()));
        }
        let sectors = bmsize.div_ceil(EFS_BLOCKSIZE as usize);
        let mut buf = vec![0u8; sectors * EFS_BLOCKSIZE as usize];
        let off = self.partition_offset + self.effective_bmblock() as u64 * EFS_BLOCKSIZE;
        read_at_aligned(
            &mut self.reader,
            off,
            (sectors * EFS_BLOCKSIZE as usize) as u64,
            &mut buf,
        )?;
        buf.truncate(bmsize);
        Ok(buf)
    }

    /// Total inodes the volume can hold. Available on the read-only
    /// surface for the fsck verifier (the editable copy lives on the
    /// `R: Read + Write + Seek` impl).
    pub(crate) fn total_inodes_readonly(&self) -> u32 {
        self.sb.ncg as u32 * self.sb.cgisize as u32 * EFS_INODES_PER_BLOCK as u32
    }

    /// Read an inode without the editable bound. Reused by fsck.
    pub(crate) fn read_inode_readonly(&mut self, inum: u32) -> Result<EfsInode, FilesystemError> {
        self.read_inode(inum)
    }

    /// Raw 512-byte block read on the read-only surface. Used by
    /// fsck's connectivity walk to crawl directory blocks without
    /// requiring the editable bound.
    pub(crate) fn read_block_readonly(
        &mut self,
        bn: u32,
        out: &mut [u8; EFS_BLOCKSIZE as usize],
    ) -> Result<(), FilesystemError> {
        read_at_aligned(
            &mut self.reader,
            self.partition_offset + bn as u64 * EFS_BLOCKSIZE,
            EFS_BLOCKSIZE,
            out,
        )
    }

    /// Read the replica superblock from sb.replsb. Used by fsck.
    pub(crate) fn read_replica_superblock(&mut self) -> Result<EfsSuperblock, FilesystemError> {
        let replica_off = self.partition_offset + self.sb.replsb as u64 * EFS_BLOCKSIZE;
        let mut sector = [0u8; EFS_BLOCKSIZE as usize];
        read_at_aligned(&mut self.reader, replica_off, EFS_BLOCKSIZE, &mut sector)?;
        EfsSuperblock::parse(&sector)
    }

    /// Resolve the effective disk-block number of the free-space
    /// bitmap. When `sb.bmblock` is non-zero, trust it. Some
    /// IRIX-formatted images (notably the IRIX 5.3 fixture) write
    /// `fs_bmblock = 0` because the kernel read driver never consults
    /// the field — by convention in those images the bitmap lives
    /// immediately after the primary superblock, i.e. at disk block 2.
    #[allow(dead_code)] // wired up in EFS Slice 5 (EditableFilesystem impl)
    pub(crate) fn effective_bmblock(&self) -> u32 {
        if self.sb.bmblock != 0 {
            self.sb.bmblock
        } else {
            2
        }
    }

    /// Read one 512-byte directory block at disk block `bn`. Returns a flag
    /// indicating whether the block was successfully fetched and had a valid
    /// magic; truncated/missing blocks produce `Ok(None)` so callers can
    /// continue listing the rest of the directory.
    fn read_dir_block(
        &mut self,
        bn: u32,
        out: &mut [u8; EFS_BLOCKSIZE as usize],
    ) -> Result<bool, FilesystemError> {
        let byte = bn as u64 * EFS_BLOCKSIZE;
        match read_at_aligned(
            &mut self.reader,
            self.partition_offset + byte,
            EFS_BLOCKSIZE,
            out,
        ) {
            Ok(()) => {
                let magic = BigEndian::read_u16(&out[0..2]);
                Ok(magic == EFS_DIRBLK_MAGIC)
            }
            Err(FilesystemError::Io(e)) if e.kind() == std::io::ErrorKind::UnexpectedEof => {
                Ok(false)
            }
            Err(e) => Err(e),
        }
    }
}

/// Editable / mutation primitives. Bounded by `R: Write` so the same
/// `EfsFilesystem` type stays usable for read-only callers without
/// pulling them into the writable surface.
///
/// Bitmap convention used here: 1 bit per disk block over the range
/// `[0 .. bmsize * 8)`. **Set bit = block FREE**, cleared bit = in use.
/// This matches the convention used by IRIX `mkfs_efs` on real disks
/// (verified against a 2 GB SCSI volume — 97%+ of inode-claimed data
/// blocks have their bitmap bit cleared, ~tfree blocks have it set).
/// It is opposite to the convention used by FAT/NTFS/exFAT but matches
/// our Amiga filesystems (AFFS/PFS3/SFS). The bitmap covers the entire
/// volume, with inode-table and reserved regions force-marked in-use
/// (bit=0) by mkfs_efs; allocation flips bits from 1→0. Block 0
/// (volume header) and block 1 (primary SB) are marked in-use (bit=0).
#[allow(dead_code)] // wired up across EFS Slices 3–8
impl<R: Read + Write + Seek> EfsFilesystem<R> {
    /// Lazy-load the free-space bitmap into `staged_bitmap` and return
    /// a mutable reference. Subsequent calls return the staged copy
    /// (so mutations accumulate). `sync_metadata` flushes it.
    pub(crate) fn staged_bitmap_mut(&mut self) -> Result<&mut Vec<u8>, FilesystemError> {
        if self.staged_bitmap.is_none() {
            self.staged_bitmap = Some(self.read_bitmap()?);
        }
        Ok(self.staged_bitmap.as_mut().unwrap())
    }

    /// Discard any staged bitmap edits without flushing. Used by the
    /// snapshot/rollback machinery (Slice 6+).
    pub(crate) fn discard_staged_bitmap(&mut self) {
        self.staged_bitmap = None;
    }

    /// Read the full free-space bitmap into memory. Size is exactly
    /// `sb.bmsize` bytes, rounded up to a 512-byte sector boundary for
    /// the underlying I/O.
    pub(crate) fn read_bitmap(&mut self) -> Result<Vec<u8>, FilesystemError> {
        let bmsize = self.sb.bmsize as usize;
        if bmsize == 0 {
            return Err(FilesystemError::InvalidData(
                "EFS bitmap size is 0 — superblock not initialized for mutation".into(),
            ));
        }
        let sectors = bmsize.div_ceil(EFS_BLOCKSIZE as usize);
        let mut buf = vec![0u8; sectors * EFS_BLOCKSIZE as usize];
        let off = self.partition_offset + self.effective_bmblock() as u64 * EFS_BLOCKSIZE;
        read_at_aligned(
            &mut self.reader,
            off,
            (sectors * EFS_BLOCKSIZE as usize) as u64,
            &mut buf,
        )?;
        buf.truncate(bmsize);
        Ok(buf)
    }

    /// Write the free-space bitmap back to its on-disk location. The
    /// caller-supplied buffer must be `sb.bmsize` bytes long; the I/O
    /// rounds up to whole sectors and zero-pads the trailing bytes of
    /// the final sector if the bitmap isn't sector-aligned.
    ///
    /// Also updates `staged_bitmap` so the in-memory copy stays in
    /// sync with disk — important because `read_bitmap_readonly`
    /// (used by fsck) prefers the staged copy when present, and we
    /// don't want a `write_bitmap` to disk to bypass a stale stage.
    pub(crate) fn write_bitmap(&mut self, bm: &[u8]) -> Result<(), FilesystemError> {
        let bmsize = self.sb.bmsize as usize;
        if bm.len() != bmsize {
            return Err(FilesystemError::InvalidData(format!(
                "write_bitmap: buffer is {} bytes, expected {bmsize}",
                bm.len()
            )));
        }
        let sectors = bmsize.div_ceil(EFS_BLOCKSIZE as usize);
        let mut padded = vec![0u8; sectors * EFS_BLOCKSIZE as usize];
        padded[..bmsize].copy_from_slice(bm);
        let off = self.partition_offset + self.effective_bmblock() as u64 * EFS_BLOCKSIZE;
        self.reader.seek(SeekFrom::Start(off))?;
        self.reader.write_all(&padded)?;
        // Mirror into the staged buffer if one is loaded.
        if self.staged_bitmap.is_some() {
            self.staged_bitmap = Some(bm.to_vec());
        }
        Ok(())
    }

    /// Allocate a contiguous run of `want_blocks` free disk blocks via
    /// a first-fit scan over the bitmap. Returns one extent (always
    /// contiguous on disk by construction) and marks the bits as
    /// in-use in `bm`. The caller is responsible for writing `bm` back
    /// via `write_bitmap` once all allocation decisions are made.
    ///
    /// EFS supports up to 12 extents per inode. Callers that need
    /// non-contiguous space across multiple extents should call this
    /// repeatedly with smaller `want_blocks` and watch the 12-extent
    /// ceiling themselves.
    pub(crate) fn alloc_contiguous_in_bitmap(
        bm: &mut [u8],
        want_blocks: u32,
        avoid_below: u32,
    ) -> Result<EfsExtent, FilesystemError> {
        if want_blocks == 0 {
            return Err(FilesystemError::InvalidData(
                "alloc_contiguous_in_bitmap: want_blocks must be > 0".into(),
            ));
        }
        let total_bits = (bm.len() as u64) * 8;
        let mut run_start: Option<u32> = None;
        let mut run_len: u32 = 0;
        for bit in 0..(total_bits as u32) {
            if bit < avoid_below {
                run_start = None;
                run_len = 0;
                continue;
            }
            let byte = (bit / 8) as usize;
            let bit_in_byte = 7 - (bit % 8); // big-endian bit order, MSB first
                                             // set bit = FREE on real IRIX EFS, so a free block is bit==1.
            let free = (bm[byte] >> bit_in_byte) & 1 == 1;
            if !free {
                run_start = None;
                run_len = 0;
            } else {
                if run_start.is_none() {
                    run_start = Some(bit);
                    run_len = 1;
                } else {
                    run_len += 1;
                }
                if run_len >= want_blocks {
                    let start = run_start.unwrap();
                    // Mark bits [start..start+want_blocks) as in-use (clear).
                    for b in start..start + want_blocks {
                        let by = (b / 8) as usize;
                        let bb = 7 - (b % 8);
                        bm[by] &= !(1u8 << bb);
                    }
                    return Ok(EfsExtent {
                        magic: 0,
                        bn: start,
                        length: want_blocks as u8,
                        offset: 0,
                    });
                }
            }
        }
        Err(FilesystemError::DiskFull(format!(
            "EFS: no contiguous run of {want_blocks} free blocks in bitmap"
        )))
    }

    /// Mark a previously-allocated extent as free in the in-memory
    /// bitmap. Caller writes the bitmap back via `write_bitmap`.
    /// Convention: set bit = free.
    pub(crate) fn free_extent_in_bitmap(bm: &mut [u8], ext: &EfsExtent) {
        let start = ext.bn;
        let len = ext.length as u32;
        for b in start..start + len {
            let by = (b / 8) as usize;
            if by >= bm.len() {
                break;
            }
            let bb = 7 - (b % 8);
            bm[by] |= 1u8 << bb;
        }
    }

    /// Total inodes the volume can hold, derived from CG geometry.
    /// `ncg * cgisize * 4` — every inode-table block holds 4 inodes.
    pub(crate) fn total_inodes(&self) -> u32 {
        self.sb.ncg as u32 * self.sb.cgisize as u32 * EFS_INODES_PER_BLOCK as u32
    }

    /// Write `inode` back to its on-disk slot. Performs a 512-byte
    /// read-modify-write on the inode-table block — three other
    /// inodes share each block, so we can't blind-write the slot
    /// without preserving the surrounding bytes.
    pub(crate) fn write_inode(&mut self, inode: &EfsInode) -> Result<(), FilesystemError> {
        if inode.inum == 0 {
            return Err(FilesystemError::InvalidData(
                "EFS write_inode: inum 0 is reserved".into(),
            ));
        }
        if inode.inum >= self.total_inodes() {
            return Err(FilesystemError::InvalidData(format!(
                "EFS write_inode: inum {} >= total inodes {}",
                inode.inum,
                self.total_inodes()
            )));
        }
        let byte_off = self.inode_byte_offset(inode.inum);
        let block_byte = (byte_off / EFS_BLOCKSIZE) * EFS_BLOCKSIZE;
        let in_block = (byte_off - block_byte) as usize;

        let mut block = [0u8; EFS_BLOCKSIZE as usize];
        read_at_aligned(
            &mut self.reader,
            self.partition_offset + block_byte,
            EFS_BLOCKSIZE,
            &mut block,
        )?;
        let mut slot = [0u8; 128];
        inode.write_into(&mut slot);
        block[in_block..in_block + 128].copy_from_slice(&slot);

        self.reader
            .seek(SeekFrom::Start(self.partition_offset + block_byte))?;
        self.reader.write_all(&block)?;
        Ok(())
    }

    /// Allocate the lowest-numbered free inode (mode == 0). Skips
    /// reserved inums 0 and 1; root (inum 2) is always returned as
    /// in-use by `read_inode` if the volume is mounted. Uses
    /// `sb.lastialloc` as a starting hint to avoid rescanning the
    /// already-scanned prefix; falls back to a from-2 rescan if the
    /// hint search misses, matching the bitmap allocator pattern.
    pub(crate) fn allocate_inode(&mut self) -> Result<u32, FilesystemError> {
        let total = self.total_inodes();
        let start = self.sb.lastialloc.max(2);
        for inum in (start..total).chain(2..start) {
            let ino = self.read_inode(inum)?;
            if ino.mode == 0 {
                self.sb.lastialloc = inum;
                return Ok(inum);
            }
        }
        Err(FilesystemError::DiskFull(format!(
            "EFS: no free inodes (total {total})"
        )))
    }

    /// Read one 512-byte block at disk block `bn` (raw I/O — no magic
    /// check). Used by directory mutation to RMW a dirblock.
    pub(crate) fn read_block(
        &mut self,
        bn: u32,
    ) -> Result<[u8; EFS_BLOCKSIZE as usize], FilesystemError> {
        let mut buf = [0u8; EFS_BLOCKSIZE as usize];
        read_at_aligned(
            &mut self.reader,
            self.partition_offset + bn as u64 * EFS_BLOCKSIZE,
            EFS_BLOCKSIZE,
            &mut buf,
        )?;
        Ok(buf)
    }

    pub(crate) fn write_block(
        &mut self,
        bn: u32,
        buf: &[u8; EFS_BLOCKSIZE as usize],
    ) -> Result<(), FilesystemError> {
        self.reader.seek(SeekFrom::Start(
            self.partition_offset + bn as u64 * EFS_BLOCKSIZE,
        ))?;
        self.reader.write_all(buf)?;
        Ok(())
    }

    /// Insert `(child_inum, name)` into `dir_inum`'s entry list.
    /// Walks the directory's dirblocks in extent order; appends to the
    /// first block with room. When no existing block fits, allocates
    /// a fresh disk block via `bm` and adds it as a new extent on the
    /// directory's inode (errors with TooLarge when the inode already
    /// has 12 extents). Returns the updated bitmap so the caller can
    /// persist it. The inode write is deferred to the caller too —
    /// they orchestrate the sync order.
    ///
    /// The directory's size field is updated in `dir_inode` (in
    /// memory). Caller must `write_inode(dir_inode)` after.
    ///
    /// Naming rules: rejects empty names, names containing '/' or NUL,
    /// and names longer than 255 bytes. Does NOT enforce uniqueness;
    /// callers check `find_in_dir` first.
    pub(crate) fn dir_insert(
        &mut self,
        dir_inode: &mut EfsInode,
        bm: &mut [u8],
        name: &[u8],
        child_inum: u32,
    ) -> Result<(), FilesystemError> {
        validate_name(name)?;
        if !dir_inode.is_dir() {
            return Err(FilesystemError::InvalidData(format!(
                "EFS dir_insert: inum {} is not a directory (mode=0o{:o})",
                dir_inode.inum, dir_inode.mode
            )));
        }
        let new_entry = DirEntry {
            inum: child_inum,
            name: name.to_vec(),
        };

        // Try to append to an existing dirblock that has room.
        let mut extents: Vec<EfsExtent> = dir_inode
            .extents
            .iter()
            .take(dir_inode.numextents as usize)
            .copied()
            .collect();
        extents.sort_by_key(|e| e.offset);

        for ext in &extents {
            for i in 0..ext.length as u32 {
                let bn = ext.bn + i;
                let block = self.read_block(bn)?;
                let mut entries = parse_dir_block(&block);
                entries.push(new_entry.clone());
                if let Some(new_block) = serialize_dir_block(&entries) {
                    self.write_block(bn, &new_block)?;
                    return Ok(());
                }
                // No room in this block — try the next.
            }
        }

        // Every existing dirblock is full. Allocate one more.
        if dir_inode.numextents as usize >= EFS_DIRECTEXTENTS {
            return Err(FilesystemError::DiskFull(format!(
                "EFS dir_insert: directory inum {} already has {} extents (max {}); \
                 directory cannot grow further on this volume",
                dir_inode.inum, dir_inode.numextents, EFS_DIRECTEXTENTS
            )));
        }
        let new_ext = Self::alloc_contiguous_in_bitmap(bm, 1, 0)?;
        // Initialize the new block with just our single entry.
        let init =
            serialize_dir_block(&[new_entry]).expect("single entry always fits in 512 bytes");
        self.write_block(new_ext.bn, &init)?;
        // Append to the inode's extent list. `offset` is the next
        // logical block in the directory's address space.
        let next_logical_offset = extents
            .iter()
            .map(|e| e.offset + e.length as u32)
            .max()
            .unwrap_or(0);
        let extent = EfsExtent {
            magic: 0,
            bn: new_ext.bn,
            length: 1,
            offset: next_logical_offset,
        };
        let slot = dir_inode.numextents as usize;
        dir_inode.extents[slot] = extent;
        dir_inode.numextents += 1;
        dir_inode.size = dir_inode.size.saturating_add(EFS_BLOCKSIZE as u32);
        Ok(())
    }

    /// Remove the dirent named `name` from `dir_inum`. Returns the
    /// child inum on success, or NotFound if absent. The block is
    /// re-serialized after the removal, compacting any gaps left by
    /// previous deletions. The dir inode's `size` is not changed
    /// (blocks stay allocated until a future compaction pass).
    pub(crate) fn dir_remove(
        &mut self,
        dir_inode: &EfsInode,
        name: &[u8],
    ) -> Result<u32, FilesystemError> {
        if !dir_inode.is_dir() {
            return Err(FilesystemError::InvalidData(format!(
                "EFS dir_remove: inum {} is not a directory",
                dir_inode.inum
            )));
        }
        let mut extents: Vec<EfsExtent> = dir_inode
            .extents
            .iter()
            .take(dir_inode.numextents as usize)
            .copied()
            .collect();
        extents.sort_by_key(|e| e.offset);

        for ext in &extents {
            for i in 0..ext.length as u32 {
                let bn = ext.bn + i;
                let block = self.read_block(bn)?;
                let mut entries = parse_dir_block(&block);
                if let Some(pos) = entries.iter().position(|e| e.name == name) {
                    let removed = entries.remove(pos);
                    let new_block =
                        serialize_dir_block(&entries).expect("removal only shrinks the block");
                    self.write_block(bn, &new_block)?;
                    return Ok(removed.inum);
                }
            }
        }
        Err(FilesystemError::NotFound(format!(
            "EFS dir_remove: name not found in dir inum {}",
            dir_inode.inum
        )))
    }

    /// Rename fallback used when the new name no longer fits in the
    /// dirblock that holds the old entry. Removes the old dirent (which
    /// preserves the child inum and compacts its block), then inserts the
    /// new name pointing at the same inum — possibly allocating a fresh
    /// dirblock. The child inode is never touched, so identity and data
    /// are preserved. Mirrors create_file's staged-bitmap orchestration:
    /// dir_insert may allocate and mutates the parent inode (extent
    /// growth), so the parent inode is written back after.
    pub(crate) fn rename_via_remove_insert(
        &mut self,
        parent_inum: u32,
        old_name: &[u8],
        new_name: &[u8],
        child_inum: u32,
    ) -> Result<(), FilesystemError> {
        // dir_remove keeps the child inum and writes the (compacted)
        // dirblock back immediately.
        let parent_inode = self.read_inode(parent_inum)?;
        let removed = self.dir_remove(&parent_inode, old_name)?;
        if removed != child_inum {
            return Err(FilesystemError::InvalidData(format!(
                "EFS rename: removed dirent inum {removed} differs from entry inum {child_inum}"
            )));
        }

        let mut bm = match self.staged_bitmap.take() {
            Some(b) => b,
            None => self.read_bitmap()?,
        };
        let res = (|| -> Result<(), FilesystemError> {
            let mut parent_inode = self.read_inode(parent_inum)?;
            self.dir_insert(&mut parent_inode, &mut bm, new_name, child_inum)?;
            self.write_inode(&parent_inode)?;
            self.sb_dirty = true;
            Ok(())
        })();
        self.staged_bitmap = Some(bm);
        res
    }

    /// Allocate disk space for `data_len` bytes via the in-memory
    /// bitmap, populate `inode.extents` / `numextents`, then stream
    /// `data` into the allocated blocks. The strategy starts with one
    /// large contiguous extent and falls back to progressively smaller
    /// chunks, accumulating up to 12 extents before failing with
    /// `DiskFull`. On failure every partially-allocated extent is
    /// freed in the bitmap.
    pub(crate) fn write_file_data(
        &mut self,
        bm: &mut [u8],
        inode: &mut EfsInode,
        data: &mut dyn Read,
        data_len: u64,
    ) -> Result<(), FilesystemError> {
        // Zero out any previously-recorded extents — caller is
        // expected to free those before calling, but the on-inode
        // bytes need to be cleared so a partial-allocation inode
        // doesn't leak garbage into the catalog if we fail mid-way.
        inode.extents = [EfsExtent {
            magic: 0,
            bn: 0,
            length: 0,
            offset: 0,
        }; EFS_DIRECTEXTENTS];
        inode.numextents = 0;
        inode.size = data_len as u32;

        if data_len == 0 {
            return Ok(());
        }
        let needed_blocks = data_len.div_ceil(EFS_BLOCKSIZE) as u32;

        let mut allocated: Vec<EfsExtent> = Vec::new();
        let mut logical_offset = 0u32;
        let mut remaining = needed_blocks;
        while remaining > 0 {
            if allocated.len() >= EFS_DIRECTEXTENTS {
                // Roll back partial allocations.
                for ext in &allocated {
                    Self::free_extent_in_bitmap(bm, ext);
                }
                return Err(FilesystemError::DiskFull(format!(
                    "EFS write_file_data: required more than {} extents \
                     (volume too fragmented for {data_len}-byte file)",
                    EFS_DIRECTEXTENTS
                )));
            }
            // Try the largest possible run first; halve on failure
            // until we find something that fits or chunk drops to 1.
            let mut chunk = remaining.min(u8::MAX as u32);
            let ext = loop {
                match Self::alloc_contiguous_in_bitmap(bm, chunk, 0) {
                    Ok(mut e) => {
                        e.offset = logical_offset;
                        break e;
                    }
                    Err(FilesystemError::DiskFull(_)) if chunk > 1 => {
                        chunk /= 2;
                    }
                    Err(e) => {
                        for ext in &allocated {
                            Self::free_extent_in_bitmap(bm, ext);
                        }
                        return Err(e);
                    }
                }
            };
            logical_offset += ext.length as u32;
            remaining -= ext.length as u32;
            allocated.push(ext);
        }

        // Record extents on the inode.
        for (i, ext) in allocated.iter().enumerate() {
            inode.extents[i] = *ext;
        }
        inode.numextents = allocated.len() as u16;

        // Stream payload into the allocated blocks.
        let mut written: u64 = 0;
        let mut block = [0u8; EFS_BLOCKSIZE as usize];
        for ext in &allocated {
            for i in 0..ext.length as u32 {
                block.fill(0); // zero-fill trailing partial block
                let want = (data_len - written).min(EFS_BLOCKSIZE) as usize;
                if want > 0 {
                    data.read_exact(&mut block[..want])?;
                }
                self.write_block(ext.bn + i, &block)?;
                written += want as u64;
                if written >= data_len {
                    return Ok(());
                }
            }
        }
        Ok(())
    }

    /// Free every block referenced by `inode`'s extents and zero the
    /// extent records. Bitmap is updated in `bm`; caller flushes.
    pub(crate) fn free_inode_data(&self, bm: &mut [u8], inode: &mut EfsInode) {
        for i in 0..inode.numextents as usize {
            let ext = inode.extents[i];
            if ext.length == 0 {
                continue;
            }
            Self::free_extent_in_bitmap(bm, &ext);
        }
        inode.extents = [EfsExtent {
            magic: 0,
            bn: 0,
            length: 0,
            offset: 0,
        }; EFS_DIRECTEXTENTS];
        inode.numextents = 0;
        inode.size = 0;
    }

    /// Look up `name` in `dir_inum`. Returns the child inum if found.
    pub(crate) fn dir_find(
        &mut self,
        dir_inode: &EfsInode,
        name: &[u8],
    ) -> Result<Option<u32>, FilesystemError> {
        let mut extents: Vec<EfsExtent> = dir_inode
            .extents
            .iter()
            .take(dir_inode.numextents as usize)
            .copied()
            .collect();
        extents.sort_by_key(|e| e.offset);
        for ext in &extents {
            for i in 0..ext.length as u32 {
                let bn = ext.bn + i;
                let block = self.read_block(bn)?;
                let entries = parse_dir_block(&block);
                if let Some(e) = entries.iter().find(|e| e.name == name) {
                    return Ok(Some(e.inum));
                }
            }
        }
        Ok(None)
    }

    /// Inherent flush — same body as the trait `sync_metadata`, but
    /// available on the `R: Read + Write + Seek` bound (no Send).
    /// The resize entry points need to call this without forcing
    /// Send up the dispatch chain.
    pub(crate) fn do_sync_metadata(&mut self) -> Result<(), FilesystemError> {
        if let Some(bm) = self.staged_bitmap.take() {
            self.write_bitmap(&bm)?;
            self.staged_bitmap = Some(bm);
        }
        if self.sb_dirty {
            self.write_superblock_pair()?;
            self.sb_dirty = false;
        }
        Ok(())
    }

    /// Write the primary superblock (sector 1) AND the replica
    /// (`sb.replsb`). Both are written from the in-memory `self.sb`.
    /// The primary write is the commit point — callers should
    /// sequence: data → inodes → bitmap → replica → primary.
    pub(crate) fn write_superblock_pair(&mut self) -> Result<(), FilesystemError> {
        let mut primary_sector = [0u8; EFS_BLOCKSIZE as usize];
        // Re-read the primary sector first so any pad/spare bytes the
        // sector held outside our serializer's coverage are preserved.
        read_at_aligned(
            &mut self.reader,
            self.partition_offset + EFS_BLOCKSIZE,
            EFS_BLOCKSIZE,
            &mut primary_sector,
        )?;
        self.sb.write_into(&mut primary_sector);

        // Replica first — primary is the commit point.
        let replica_off = self.partition_offset + self.sb.replsb as u64 * EFS_BLOCKSIZE;
        self.reader.seek(SeekFrom::Start(replica_off))?;
        self.reader.write_all(&primary_sector)?;

        self.reader
            .seek(SeekFrom::Start(self.partition_offset + EFS_BLOCKSIZE))?;
        self.reader.write_all(&primary_sector)?;
        Ok(())
    }
}

/// Helper to build a `FileEntry` from an `EfsInode` + name/path.
/// Symlink targets are intentionally not resolved here — the editable
/// flow returns entries the GUI then re-reads via the read-only path
/// that resolves targets.
fn entry_from_inode(name: &str, parent_path: &str, ino: &EfsInode) -> FileEntry {
    let path = if parent_path == "/" {
        format!("/{name}")
    } else {
        format!("{parent_path}/{name}")
    };
    let size = if ino.is_dir() { 0 } else { ino.size as u64 };
    FileEntry {
        name: name.to_string(),
        path,
        entry_type: ino.entry_type(),
        size,
        location: ino.inum as u64,
        modified: None,
        type_code: None,
        creator_code: None,
        symlink_target: None,
        special_type: ino.special_type(),
        mode: Some(ino.mode as u32),
        uid: Some(ino.uid as u32),
        gid: Some(ino.gid as u32),
        resource_fork_size: None,
        aux_type: None,
        link_target_cnid: None,
        amiga_protection: None,
        amiga_comment: None,
        amiga_date: None,
        dos_attributes: None,
        mac_dates: None,
    }
}

impl<R: Read + Write + Seek + Send> super::filesystem::EditableFilesystem for EfsFilesystem<R> {
    fn create_file(
        &mut self,
        parent: &FileEntry,
        name: &str,
        data: &mut dyn Read,
        data_len: u64,
        options: &super::filesystem::CreateFileOptions,
    ) -> Result<FileEntry, FilesystemError> {
        if !parent.is_directory() {
            return Err(FilesystemError::NotADirectory(parent.path.clone()));
        }
        let name_bytes = name.as_bytes();
        validate_name(name_bytes)?;
        let parent_inum = parent.location as u32;

        // Pre-flight: name must not already exist in parent.
        let parent_inode = self.read_inode(parent_inum)?;
        if self.dir_find(&parent_inode, name_bytes)?.is_some() {
            return Err(FilesystemError::AlreadyExists(name.into()));
        }

        // Take the bitmap out of staging so we can hold it as a local
        // mut and still call other &mut self methods (write_block etc).
        // Restored to staging on every exit path (success or error).
        let mut bm = match self.staged_bitmap.take() {
            Some(b) => b,
            None => self.read_bitmap()?,
        };

        let res = (|| -> Result<FileEntry, FilesystemError> {
            let inum = self.allocate_inode()?;
            let mut new_ino = EfsInode::empty(inum);
            new_ino.mode = options.mode.unwrap_or(0o100644) as u16;
            new_ino.nlink = 1;
            new_ino.uid = options.uid.unwrap_or(0) as u16;
            new_ino.gid = options.gid.unwrap_or(0) as u16;

            self.write_file_data(&mut bm, &mut new_ino, data, data_len)?;
            self.write_inode(&new_ino)?;

            // Link into parent. dir_insert may mutate parent_inode
            // (extent growth on overflow), so re-read fresh.
            let mut parent_ino = self.read_inode(parent_inum)?;
            self.dir_insert(&mut parent_ino, &mut bm, name_bytes, inum)?;
            self.write_inode(&parent_ino)?;

            self.sb.tinode = self.sb.tinode.saturating_sub(1);
            self.sb.lastialloc = inum;
            self.sb_dirty = true;
            Ok(entry_from_inode(name, &parent.path, &new_ino))
        })();

        self.staged_bitmap = Some(bm);
        res
    }

    fn create_directory(
        &mut self,
        parent: &FileEntry,
        name: &str,
        options: &super::filesystem::CreateDirectoryOptions,
    ) -> Result<FileEntry, FilesystemError> {
        if !parent.is_directory() {
            return Err(FilesystemError::NotADirectory(parent.path.clone()));
        }
        let name_bytes = name.as_bytes();
        validate_name(name_bytes)?;
        let parent_inum = parent.location as u32;

        let parent_inode_initial = self.read_inode(parent_inum)?;
        if self.dir_find(&parent_inode_initial, name_bytes)?.is_some() {
            return Err(FilesystemError::AlreadyExists(name.into()));
        }

        let mut bm = match self.staged_bitmap.take() {
            Some(b) => b,
            None => self.read_bitmap()?,
        };

        let res = (|| -> Result<FileEntry, FilesystemError> {
            let inum = self.allocate_inode()?;
            // Allocate one disk block for the new dir's initial
            // dirblock (holding `.` and `..`).
            let mut ext = Self::alloc_contiguous_in_bitmap(&mut bm, 1, 0)?;
            ext.offset = 0;

            // Write the initial dirblock with "." and "..".
            let initial = serialize_dir_block(&[
                DirEntry {
                    inum,
                    name: b".".to_vec(),
                },
                DirEntry {
                    inum: parent_inum,
                    name: b"..".to_vec(),
                },
            ])
            .expect("./.. always fits in one block");
            self.write_block(ext.bn, &initial)?;

            let mut new_dir = EfsInode::empty(inum);
            new_dir.mode = options.mode.unwrap_or(0o040755) as u16;
            new_dir.nlink = 2; // `.` is the second link
            new_dir.uid = options.uid.unwrap_or(0) as u16;
            new_dir.gid = options.gid.unwrap_or(0) as u16;
            new_dir.size = EFS_BLOCKSIZE as u32;
            new_dir.numextents = 1;
            new_dir.extents[0] = ext;
            self.write_inode(&new_dir)?;

            // Link into parent.
            let mut parent_inode = self.read_inode(parent_inum)?;
            self.dir_insert(&mut parent_inode, &mut bm, name_bytes, inum)?;
            // Parent's nlink increases by 1 (the new dir's ".." back-link).
            parent_inode.nlink = parent_inode.nlink.saturating_add(1);
            self.write_inode(&parent_inode)?;

            self.sb.tinode = self.sb.tinode.saturating_sub(1);
            self.sb.lastialloc = inum;
            self.sb_dirty = true;
            Ok(entry_from_inode(name, &parent.path, &new_dir))
        })();

        self.staged_bitmap = Some(bm);
        res
    }

    fn delete_entry(
        &mut self,
        parent: &FileEntry,
        entry: &FileEntry,
    ) -> Result<(), FilesystemError> {
        if !parent.is_directory() {
            return Err(FilesystemError::NotADirectory(parent.path.clone()));
        }
        let parent_inum = parent.location as u32;
        let entry_inum = entry.location as u32;
        if entry_inum < 2 {
            return Err(FilesystemError::InvalidData(format!(
                "EFS delete_entry: refusing to delete reserved inum {entry_inum}"
            )));
        }
        let mut target = self.read_inode(entry_inum)?;
        // For directories, ensure empty (`.`/`..` only).
        if target.is_dir() {
            let mut found_other = false;
            let mut extents: Vec<EfsExtent> = target
                .extents
                .iter()
                .take(target.numextents as usize)
                .copied()
                .collect();
            extents.sort_by_key(|e| e.offset);
            'scan: for ext in &extents {
                for i in 0..ext.length as u32 {
                    let block = self.read_block(ext.bn + i)?;
                    let entries = parse_dir_block(&block);
                    for de in &entries {
                        if de.name != b"." && de.name != b".." {
                            found_other = true;
                            break 'scan;
                        }
                    }
                }
            }
            if found_other {
                return Err(FilesystemError::InvalidData(format!(
                    "EFS delete_entry: directory '{}' not empty",
                    entry.path
                )));
            }
        }

        let mut bm = match self.staged_bitmap.take() {
            Some(b) => b,
            None => self.read_bitmap()?,
        };

        let res = (|| -> Result<(), FilesystemError> {
            // Unlink from parent first so a crash before the inode is
            // freed leaves an orphan inode (recoverable via fsck)
            // rather than a dangling dirent.
            let mut parent_inode = self.read_inode(parent_inum)?;
            let removed_inum = self.dir_remove(&parent_inode, entry.name.as_bytes())?;
            if removed_inum != entry_inum {
                return Err(FilesystemError::InvalidData(format!(
                    "EFS delete_entry: dirent inum {} does not match entry inum {}",
                    removed_inum, entry_inum
                )));
            }

            // Free the inode's data blocks.
            self.free_inode_data(&mut bm, &mut target);

            // If the target was a directory, free its own dirblocks
            // too (free_inode_data already covered them via the
            // extent list).

            // Zero the inode slot so allocate_inode sees it as free.
            let zero = EfsInode::empty(entry_inum);
            self.write_inode(&zero)?;

            // Directory parent's nlink decreases when we delete a
            // child directory (its ".." back-link goes away).
            if entry.is_directory() {
                parent_inode.nlink = parent_inode.nlink.saturating_sub(1);
                self.write_inode(&parent_inode)?;
            }

            self.sb.tinode = self.sb.tinode.saturating_add(1);
            self.sb_dirty = true;
            Ok(())
        })();

        self.staged_bitmap = Some(bm);
        res
    }

    fn rename(
        &mut self,
        parent: &FileEntry,
        entry: &FileEntry,
        new_name: &str,
    ) -> Result<(), FilesystemError> {
        if !parent.is_directory() {
            return Err(FilesystemError::NotADirectory(parent.path.clone()));
        }
        if new_name == entry.name {
            return Ok(());
        }
        let new_name_bytes = new_name.as_bytes();
        validate_name(new_name_bytes)?;

        let parent_inum = parent.location as u32;
        let entry_inum = entry.location as u32;
        let old_name_bytes = entry.name.as_bytes();

        // EFS names are case-sensitive; any name differing from the old
        // one is a distinct dirent. Reject only if it already exists.
        let parent_inode = self.read_inode(parent_inum)?;
        if self.dir_find(&parent_inode, new_name_bytes)?.is_some() {
            return Err(FilesystemError::AlreadyExists(new_name.into()));
        }

        // Preferred path: if the renamed entry stays within its current
        // 512-byte dirblock (the common case — names are short and the
        // inum is unchanged), re-serialize that one block in place. This
        // allocates nothing, so no bitmap / superblock bookkeeping is
        // needed. Walk the directory's extents to find the block holding
        // the old name.
        let mut extents: Vec<EfsExtent> = parent_inode
            .extents
            .iter()
            .take(parent_inode.numextents as usize)
            .copied()
            .collect();
        extents.sort_by_key(|e| e.offset);

        for ext in &extents {
            for i in 0..ext.length as u32 {
                let bn = ext.bn + i;
                let block = self.read_block(bn)?;
                let mut entries = parse_dir_block(&block);
                if let Some(slot) = entries.iter().position(|e| e.name == old_name_bytes) {
                    // Confirm the dirent points at the entry we expect —
                    // guards against a stale FileEntry.
                    if entries[slot].inum != entry_inum {
                        return Err(FilesystemError::InvalidData(format!(
                            "EFS rename: dirent inum {} does not match entry inum {}",
                            entries[slot].inum, entry_inum
                        )));
                    }
                    entries[slot].name = new_name_bytes.to_vec();
                    if let Some(new_block) = serialize_dir_block(&entries) {
                        // Fits — commit the single-block rewrite.
                        self.write_block(bn, &new_block)?;
                        return Ok(());
                    }
                    // The longer name overflows this block. Fall back to
                    // remove-then-insert below (the new entry may need a
                    // fresh block). The child inum is preserved across
                    // both operations, so identity / data are untouched.
                    return self.rename_via_remove_insert(
                        parent_inum,
                        old_name_bytes,
                        new_name_bytes,
                        entry_inum,
                    );
                }
            }
        }

        Err(FilesystemError::NotFound(entry.name.clone()))
    }

    fn sync_metadata(&mut self) -> Result<(), FilesystemError> {
        self.do_sync_metadata()
    }

    fn free_space(&mut self) -> Result<u64, FilesystemError> {
        Ok(self.sb.tfree as u64 * EFS_BLOCKSIZE)
    }

    fn repair(&mut self) -> Result<super::fsck::RepairReport, FilesystemError> {
        repair_efs(self)
    }
}

/// EFS repair driver. Re-runs the verifier, fixes the repairable
/// findings (replica copy, missing bitmap bits, orphan inodes adopted
/// into `lost+found/`), then syncs. Returns a report compatible with
/// the shared `RepairReport` type. Unrepairable errors (geometry
/// damage, extents past volume, double allocation) are counted in
/// `unrepairable_count` and left for a future repair pass.
fn repair_efs<R: Read + Write + Seek + Send>(
    fs: &mut EfsFilesystem<R>,
) -> Result<super::fsck::RepairReport, FilesystemError> {
    let mut report = super::fsck::RepairReport {
        fixes_applied: Vec::new(),
        fixes_failed: Vec::new(),
        unrepairable_count: 0,
    };

    let result = super::efs_fsck::fsck_efs(fs)?;
    if result.errors.is_empty() {
        return Ok(report);
    }

    // --- Replica fixup ---
    // Any Replica*Mismatch finding means the replica's bytes don't
    // match the primary's. write_superblock_pair() rewrites the
    // replica from the in-memory sb (which was loaded from primary),
    // so a single sync_metadata after marking the sb dirty handles
    // all replica mismatches at once.
    let replica_errs = result
        .errors
        .iter()
        .filter(|e| e.code.starts_with("Replica") && e.code.ends_with("Mismatch"))
        .count();
    if replica_errs > 0 {
        fs.sb_dirty = true;
        report.fixes_applied.push(format!(
            "Replica superblock: copied primary fields ({replica_errs} field(s) corrected)"
        ));
    }

    // --- Bitmap missing-allocation fixup ---
    // The verifier reports one error per missing block; parse the
    // block number out of the message. We do this rather than carry
    // structured data through FsckIssue because the shared types
    // don't have an EFS-specific extension slot.
    let mut bitmap_fixes = 0u32;
    for issue in result
        .errors
        .iter()
        .filter(|e| e.code == "BitmapMissingAllocation")
    {
        let blk = match parse_block_number_from_msg(&issue.message) {
            Some(b) => b,
            None => {
                report.fixes_failed.push(format!(
                    "BitmapMissingAllocation: could not parse block number from '{}'",
                    issue.message
                ));
                continue;
            }
        };
        let bm = fs.staged_bitmap_mut()?;
        let by = (blk / 8) as usize;
        if by >= bm.len() {
            report.fixes_failed.push(format!(
                "BitmapMissingAllocation: block {blk} past bitmap end"
            ));
            continue;
        }
        let bb = 7 - (blk % 8);
        // set bit = free; mark as in-use by CLEARING the bit.
        bm[by] &= !(1 << bb);
        bitmap_fixes += 1;
    }
    if bitmap_fixes > 0 {
        report.fixes_applied.push(format!(
            "Bitmap: cleared {bitmap_fixes} stray free bit(s) for inode-claimed blocks"
        ));
    }

    // --- Orphan adoption into lost+found ---
    if !result.orphaned_entries.is_empty() {
        let adopt_count = adopt_orphans_into_lost_found(fs, &result.orphaned_entries, &mut report)?;
        if adopt_count > 0 {
            report.fixes_applied.push(format!(
                "Orphans: adopted {adopt_count} entry/entries into lost+found/"
            ));
        }
    }

    // --- Count unrepairable findings ---
    report.unrepairable_count = result.errors.iter().filter(|e| !e.repairable).count();

    fs.do_sync_metadata()?;
    Ok(report)
}

/// Parse "...block <N>..." out of a fsck issue message. The verifier
/// emits a stable format so this is a controlled-input parse.
fn parse_block_number_from_msg(msg: &str) -> Option<u32> {
    // Format: "inode-allocated block {blk} is not marked in-use in bitmap"
    let tail = msg.strip_prefix("inode-allocated block ")?;
    let num_str = tail.split_whitespace().next()?;
    num_str.parse().ok()
}

/// Ensure a `lost+found/` directory exists under root and link each
/// orphan into it as `ino_<inum>`. Returns the number of orphans
/// actually adopted (may be less than `orphans.len()` if some were
/// rejected — name collisions get a numeric suffix retry; other
/// failures are logged in `report.fixes_failed`).
fn adopt_orphans_into_lost_found<R: Read + Write + Seek + Send>(
    fs: &mut EfsFilesystem<R>,
    orphans: &[super::fsck::OrphanedEntry],
    report: &mut super::fsck::RepairReport,
) -> Result<u32, FilesystemError> {
    use super::entry::EntryType;
    use super::filesystem::EditableFilesystem;

    // Find or create lost+found under root.
    let root = fs.read_inode(2)?;
    let lf_inum = match fs.dir_find(&root, b"lost+found")? {
        Some(inum) => inum,
        None => {
            // Build a synthetic root FileEntry the trait method
            // wants. The fields beyond `location` aren't consulted
            // by create_directory.
            let root_entry = super::entry::FileEntry {
                name: "/".into(),
                path: "/".into(),
                entry_type: EntryType::Directory,
                size: 0,
                location: 2,
                modified: None,
                type_code: None,
                creator_code: None,
                symlink_target: None,
                special_type: None,
                mode: Some(root.mode as u32),
                uid: Some(root.uid as u32),
                gid: Some(root.gid as u32),
                resource_fork_size: None,
                aux_type: None,
                link_target_cnid: None,
                amiga_protection: None,
                amiga_comment: None,
                amiga_date: None,
                dos_attributes: None,
                mac_dates: None,
            };
            let lf = fs.create_directory(
                &root_entry,
                "lost+found",
                &super::filesystem::CreateDirectoryOptions::default(),
            )?;
            lf.location as u32
        }
    };

    // Take the bitmap out so dir_insert can mutate it.
    let mut bm = match fs.staged_bitmap.take() {
        Some(b) => b,
        None => fs.read_bitmap()?,
    };
    let res = (|| -> Result<u32, FilesystemError> {
        let mut adopted: u32 = 0;
        for orphan in orphans {
            let inum = orphan.id as u32;
            let mut lf_inode = fs.read_inode(lf_inum)?;
            // Avoid name collisions: append a numeric suffix on retry.
            let mut name = format!("ino_{inum}");
            let mut suffix = 1u32;
            while fs.dir_find(&lf_inode, name.as_bytes())?.is_some() {
                name = format!("ino_{inum}_{suffix}");
                suffix += 1;
                if suffix > 1000 {
                    report.fixes_failed.push(format!(
                        "Orphan inum {inum}: could not generate unique lost+found name"
                    ));
                    continue;
                }
            }
            match fs.dir_insert(&mut lf_inode, &mut bm, name.as_bytes(), inum) {
                Ok(()) => {
                    fs.write_inode(&lf_inode)?;
                    adopted += 1;
                }
                Err(e) => {
                    report.fixes_failed.push(format!(
                        "Orphan inum {inum}: could not adopt into lost+found: {e}"
                    ));
                }
            }
        }
        Ok(adopted)
    })();
    fs.staged_bitmap = Some(bm);
    res
}

/// One directory entry decoded from a dirblock. `name` is the raw
/// 8-bit bytes — EFS made no Unicode promises and we round-trip them
/// verbatim.
#[derive(Debug, Clone)]
pub(crate) struct DirEntry {
    pub inum: u32,
    pub name: Vec<u8>,
}

/// Parse one 512-byte directory block into a Vec of entries in slot
/// order. Skips zero slot bytes. Returns an empty Vec on bad magic so
/// callers can decide whether to treat that as corruption or
/// uninitialized.
pub(crate) fn parse_dir_block(buf: &[u8; EFS_BLOCKSIZE as usize]) -> Vec<DirEntry> {
    let magic = BigEndian::read_u16(&buf[0..2]);
    if magic != EFS_DIRBLK_MAGIC {
        return Vec::new();
    }
    let slots = buf[3] as usize;
    let max_slots = (EFS_BLOCKSIZE as usize - EFS_DIRBLK_HEADERSIZE).min(slots);
    let mut out: Vec<DirEntry> = Vec::new();
    for slot in 0..max_slots {
        let raw = buf[EFS_DIRBLK_HEADERSIZE + slot];
        if raw == 0 {
            continue;
        }
        let off = (raw as usize) << 1;
        if off + 5 > EFS_BLOCKSIZE as usize {
            continue;
        }
        let inum = BigEndian::read_u32(&buf[off..off + 4]);
        let namelen = buf[off + 4] as usize;
        if namelen == 0 || off + 5 + namelen > EFS_BLOCKSIZE as usize {
            continue;
        }
        out.push(DirEntry {
            inum,
            name: buf[off + 5..off + 5 + namelen].to_vec(),
        });
    }
    out
}

/// Serialize `entries` into a fresh 512-byte directory block. Returns
/// `None` when the entries don't fit — caller must split / spill into
/// the next dirblock. Dirents are laid out from the top of the block
/// downward; the slot table starts at byte 4 and grows up. Each
/// dirent occupies `align_up_even(5 + namelen)` bytes.
pub(crate) fn serialize_dir_block(entries: &[DirEntry]) -> Option<[u8; EFS_BLOCKSIZE as usize]> {
    let mut buf = [0u8; EFS_BLOCKSIZE as usize];
    BigEndian::write_u16(&mut buf[0..2], EFS_DIRBLK_MAGIC);
    let n = entries.len();
    if n > 255 {
        return None;
    }
    // Compute total dirent bytes, plus 4 header + n slot bytes.
    let mut dirent_bytes: usize = 0;
    for e in entries {
        if e.name.len() > 255 {
            return None;
        }
        let raw = 5 + e.name.len();
        dirent_bytes += (raw + 1) & !1; // round up to even
    }
    let slot_table_end = EFS_DIRBLK_HEADERSIZE + n;
    if slot_table_end + dirent_bytes > EFS_BLOCKSIZE as usize {
        return None;
    }
    // Lay out dirents from byte 512 downward.
    let mut cursor = EFS_BLOCKSIZE as usize;
    let mut firstused = 0u8;
    for (i, e) in entries.iter().enumerate() {
        let dirent_len = (5 + e.name.len() + 1) & !1;
        let start = cursor - dirent_len;
        BigEndian::write_u32(&mut buf[start..start + 4], e.inum);
        buf[start + 4] = e.name.len() as u8;
        buf[start + 5..start + 5 + e.name.len()].copy_from_slice(&e.name);
        // Even-padding byte (if any) stays 0 from buf init.
        buf[EFS_DIRBLK_HEADERSIZE + i] = (start >> 1) as u8;
        cursor = start;
        if firstused == 0 || ((start >> 1) as u8) < firstused {
            firstused = (start >> 1) as u8;
        }
    }
    buf[2] = firstused;
    buf[3] = n as u8;
    Some(buf)
}

/// Decode one EFS directory block. Each block is 512 bytes with a 4-byte
/// header (magic:be16, firstused:u8, slots:u8) followed by a slot table where
/// each non-zero slot byte points (<<1) to a `(inode:be32, namelen:u8,
/// name[namelen])` dirent.
fn iter_dir_block<F>(buf: &[u8; EFS_BLOCKSIZE as usize], parent_path: &str, mut emit: F)
where
    F: FnMut(u32, &str, String),
{
    let slots = buf[3] as usize;
    let max_slots = (EFS_BLOCKSIZE as usize - EFS_DIRBLK_HEADERSIZE).min(slots);
    for slot in 0..max_slots {
        let raw = buf[EFS_DIRBLK_HEADERSIZE + slot];
        if raw == 0 {
            continue;
        }
        let off = (raw as usize) << 1;
        if off + 5 > EFS_BLOCKSIZE as usize {
            continue;
        }
        let inum = BigEndian::read_u32(&buf[off..off + 4]);
        let namelen = buf[off + 4] as usize;
        if namelen == 0 || off + 5 + namelen > EFS_BLOCKSIZE as usize {
            continue;
        }
        let name_bytes = &buf[off + 5..off + 5 + namelen];
        let name = String::from_utf8_lossy(name_bytes).into_owned();
        let path = if parent_path == "/" {
            format!("/{name}")
        } else {
            format!("{parent_path}/{name}")
        };
        emit(inum, &name, path);
    }
}

impl<R: Read + Seek + Send> Filesystem for EfsFilesystem<R> {
    fn root(&mut self) -> Result<FileEntry, FilesystemError> {
        let ino = self.read_inode(EFS_ROOT_INODE)?;
        if !ino.is_dir() {
            return Err(FilesystemError::InvalidData(format!(
                "EFS root inode 2 is not a directory (mode=0o{:o})",
                ino.mode
            )));
        }
        Ok(FileEntry {
            name: "/".into(),
            path: "/".into(),
            entry_type: EntryType::Directory,
            size: ino.size as u64,
            location: EFS_ROOT_INODE as u64,
            modified: None,
            type_code: None,
            creator_code: None,
            symlink_target: None,
            special_type: None,
            mode: Some(ino.mode as u32),
            uid: Some(ino.uid as u32),
            gid: Some(ino.gid as u32),
            resource_fork_size: None,
            aux_type: None,
            link_target_cnid: None,
            amiga_protection: None,
            amiga_comment: None,
            amiga_date: None,
            dos_attributes: None,
            mac_dates: None,
        })
    }

    fn list_directory(&mut self, entry: &FileEntry) -> Result<Vec<FileEntry>, FilesystemError> {
        if !entry.is_directory() {
            return Err(FilesystemError::NotADirectory(entry.path.clone()));
        }
        let inum = entry.location as u32;
        let dir_ino = self.read_inode(inum)?;
        if !dir_ino.is_dir() {
            return Err(FilesystemError::NotADirectory(entry.path.clone()));
        }

        // Walk the inline extents in logical-offset order. EFS has no indirect
        // blocks; a directory's data lives entirely in the 12 inline extents.
        let mut extents: Vec<EfsExtent> = dir_ino
            .extents
            .iter()
            .take(dir_ino.numextents as usize)
            .copied()
            .collect();
        extents.sort_by_key(|e| e.offset);

        let mut entries: Vec<FileEntry> = Vec::new();
        let mut block_buf = [0u8; EFS_BLOCKSIZE as usize];
        for ext in &extents {
            for i in 0..ext.length as u32 {
                let bn = ext.bn + i;
                let ok = self.read_dir_block(bn, &mut block_buf)?;
                if !ok {
                    // Truncated fixture or damaged block — stop here. We've
                    // already returned everything we could read.
                    continue;
                }
                let parent_path = entry.path.as_str();
                let mut pending: Vec<(u32, String, String)> = Vec::new();
                iter_dir_block(&block_buf, parent_path, |inum, name, path| {
                    pending.push((inum, name.to_string(), path));
                });
                for (child_inum, name, path) in pending {
                    if name == "." || name == ".." {
                        continue;
                    }
                    if child_inum == 0 {
                        continue;
                    }
                    // Read child inode for type/size info. If reading fails
                    // (e.g., the inode block is past the truncated fixture),
                    // emit a placeholder file entry so the caller still sees
                    // the name. Pure browse should never panic on damaged
                    // metadata.
                    match self.read_inode(child_inum) {
                        Ok(child) => {
                            // Resolve symlink target eagerly so the GUI can
                            // display it without a second round trip. If the
                            // target data lives past the fixture window we
                            // leave it as None — the entry still surfaces as
                            // a symlink, just without a populated target.
                            let symlink_target = if child.is_symlink() {
                                read_inode_data(
                                    &mut self.reader,
                                    self.partition_offset,
                                    &child,
                                    usize::MAX,
                                )
                                .ok()
                                .map(|bytes| {
                                    String::from_utf8_lossy(&bytes)
                                        .trim_end_matches('\0')
                                        .to_string()
                                })
                            } else {
                                None
                            };
                            let mut e = FileEntry {
                                name,
                                path,
                                entry_type: child.entry_type(),
                                size: child.size as u64,
                                location: child_inum as u64,
                                modified: None,
                                type_code: None,
                                creator_code: None,
                                symlink_target,
                                special_type: child.special_type(),
                                mode: Some(child.mode as u32),
                                uid: Some(child.uid as u32),
                                gid: Some(child.gid as u32),
                                resource_fork_size: None,
                                aux_type: None,
                                link_target_cnid: None,
                                amiga_protection: None,
                                amiga_comment: None,
                                amiga_date: None,
                                dos_attributes: None,
                                mac_dates: None,
                            };
                            if matches!(e.entry_type, EntryType::Directory) {
                                e.size = 0;
                            }
                            entries.push(e);
                        }
                        Err(_) => {
                            entries.push(FileEntry::new_file(name, path, 0, child_inum as u64));
                        }
                    }
                }
            }
        }
        Ok(entries)
    }

    fn read_file(
        &mut self,
        entry: &FileEntry,
        max_bytes: usize,
    ) -> Result<Vec<u8>, FilesystemError> {
        if entry.is_directory() {
            return Err(FilesystemError::InvalidData(format!(
                "EFS read_file on directory: {}",
                entry.path
            )));
        }
        let inum = entry.location as u32;
        let inode = self.read_inode(inum)?;
        read_inode_data(&mut self.reader, self.partition_offset, &inode, max_bytes)
    }

    fn write_file_to(
        &mut self,
        entry: &FileEntry,
        writer: &mut dyn std::io::Write,
    ) -> Result<u64, FilesystemError> {
        if entry.is_directory() {
            return Err(FilesystemError::InvalidData(format!(
                "EFS write_file_to on directory: {}",
                entry.path
            )));
        }
        let inum = entry.location as u32;
        let inode = self.read_inode(inum)?;
        stream_inode_data(&mut self.reader, self.partition_offset, &inode, writer)
    }

    fn volume_label(&self) -> Option<&str> {
        if self.label.is_empty() {
            None
        } else {
            Some(&self.label)
        }
    }

    fn fs_type(&self) -> &str {
        "EFS"
    }

    fn total_size(&self) -> u64 {
        self.sb.fs_size as u64 * EFS_BLOCKSIZE
    }

    fn used_size(&self) -> u64 {
        // No allocation summary parsed yet; report total for now.
        self.total_size()
    }

    fn last_data_byte(&mut self) -> Result<u64, FilesystemError> {
        // Conservative floor: 1 + highest block claimed by any in-use
        // inode. Multiplied to bytes for the caller. This drives the
        // shrink-to-minimum picker in the GUI and the restore-flow
        // resize plan.
        let floor_blocks = super::efs_resize::compute_conservative_floor(self)?;
        Ok(floor_blocks as u64 * EFS_BLOCKSIZE)
    }

    fn validate_name(&self, name: &str) -> Result<(), FilesystemError> {
        validate_name(name.as_bytes())
    }

    fn fsck(&mut self) -> Option<Result<super::fsck::FsckResult, FilesystemError>> {
        Some(super::efs_fsck::fsck_efs(self))
    }
}

/// Resolve an inode's data extents, handling both direct and indirect mode.
///
/// Direct mode (`numextents <= 12`): the inode's extent array holds data
/// extents directly. Indirect mode (`numextents > 12`): `extents[0].offset`
/// stores `direxts`, the number of inode slots that point at runs of
/// indirect blocks; each indirect block packs up to 64 data-extent records.
/// Returns the data extents in file (logical) order.
pub(crate) fn resolve_data_extents<R: Read + Seek>(
    reader: &mut R,
    partition_offset: u64,
    inode: &EfsInode,
) -> Result<Vec<EfsExtent>, FilesystemError> {
    let total = inode.numextents as usize;
    if total <= EFS_DIRECTEXTENTS {
        let mut out: Vec<EfsExtent> = inode.extents.iter().take(total).copied().collect();
        out.sort_by_key(|e| e.offset);
        return Ok(out);
    }
    let direxts = inode.extents[0].offset as usize;
    if direxts == 0 || direxts > EFS_DIRECTEXTENTS {
        return Err(FilesystemError::InvalidData(format!(
            "EFS inode {} indirect mode: bad direxts={direxts} (numextents={total})",
            inode.inum
        )));
    }
    const EXTS_PER_BLOCK: usize = (EFS_BLOCKSIZE as usize) / 8;
    let mut out: Vec<EfsExtent> = Vec::with_capacity(total);
    let mut block_buf = [0u8; EFS_BLOCKSIZE as usize];
    'collect: for dirslot in 0..direxts {
        let ind = inode.extents[dirslot];
        for blk in 0..ind.length as u32 {
            let bn = ind.bn + blk;
            read_at_aligned(
                reader,
                partition_offset + bn as u64 * EFS_BLOCKSIZE,
                EFS_BLOCKSIZE,
                &mut block_buf,
            )?;
            for slot in 0..EXTS_PER_BLOCK {
                let off = slot * 8;
                let raw: &[u8; 8] = (&block_buf[off..off + 8]).try_into().unwrap();
                let ext = EfsExtent::parse(raw);
                if ext.magic != 0 {
                    return Err(FilesystemError::InvalidData(format!(
                        "EFS inode {} indirect extent slot {slot} in block {bn}: bad magic 0x{:02X}",
                        inode.inum, ext.magic
                    )));
                }
                out.push(ext);
                if out.len() == total {
                    break 'collect;
                }
            }
        }
    }
    if out.len() != total {
        return Err(FilesystemError::InvalidData(format!(
            "EFS inode {} indirect: collected {} of {total} extents",
            inode.inum,
            out.len()
        )));
    }
    out.sort_by_key(|e| e.offset);
    Ok(out)
}

/// Like `resolve_data_extents`, but also returns the indirect-block extents
/// (the disk regions used to hold extent records in indirect mode). The
/// `(data, indirect)` split lets callers like fsck and the resize bitmap
/// walker account for every disk block claimed by an inode — both file
/// content and the indirect-extent index blocks themselves. In direct
/// mode the indirect vector is empty.
pub(crate) fn resolve_owned_extents<R: Read + Seek>(
    reader: &mut R,
    partition_offset: u64,
    inode: &EfsInode,
) -> Result<(Vec<EfsExtent>, Vec<EfsExtent>), FilesystemError> {
    if (inode.numextents as usize) <= EFS_DIRECTEXTENTS {
        return Ok((
            resolve_data_extents(reader, partition_offset, inode)?,
            Vec::new(),
        ));
    }
    let direxts = inode.extents[0].offset as usize;
    if direxts == 0 || direxts > EFS_DIRECTEXTENTS {
        return Err(FilesystemError::InvalidData(format!(
            "EFS inode {} indirect mode: bad direxts={direxts}",
            inode.inum
        )));
    }
    let indirect: Vec<EfsExtent> = inode.extents.iter().take(direxts).copied().collect();
    let data = resolve_data_extents(reader, partition_offset, inode)?;
    Ok((data, indirect))
}

/// Walk the data extents of `inode` (direct or indirect) and return up to
/// `max_bytes` of file data. The inode's `di_size` is the authoritative
/// file length.
fn read_inode_data<R: Read + Seek>(
    reader: &mut R,
    partition_offset: u64,
    inode: &EfsInode,
    max_bytes: usize,
) -> Result<Vec<u8>, FilesystemError> {
    let size = inode.size as usize;
    let want = size.min(max_bytes);
    let mut out = Vec::with_capacity(want);
    if want == 0 {
        return Ok(out);
    }
    let extents = resolve_data_extents(reader, partition_offset, inode)?;
    let mut block_buf = [0u8; EFS_BLOCKSIZE as usize];
    'outer: for ext in &extents {
        for i in 0..ext.length as u32 {
            let bn = ext.bn + i;
            read_at_aligned(
                reader,
                partition_offset + bn as u64 * EFS_BLOCKSIZE,
                EFS_BLOCKSIZE,
                &mut block_buf,
            )?;
            let remaining = want.saturating_sub(out.len());
            if remaining == 0 {
                break 'outer;
            }
            let take = remaining.min(EFS_BLOCKSIZE as usize);
            out.extend_from_slice(&block_buf[..take]);
            if out.len() >= want {
                break 'outer;
            }
        }
    }
    Ok(out)
}

/// Streaming version of `read_inode_data`. Writes the file's bytes to
/// `writer` 1 MiB at a time and returns the byte count written. The 1 MiB
/// staging buffer is the chunk size used by the rest of the codebase for
/// streaming filesystem reads.
fn stream_inode_data<R: Read + Seek>(
    reader: &mut R,
    partition_offset: u64,
    inode: &EfsInode,
    writer: &mut dyn std::io::Write,
) -> Result<u64, FilesystemError> {
    let size = inode.size as u64;
    if size == 0 {
        return Ok(0);
    }
    let extents = resolve_data_extents(reader, partition_offset, inode)?;

    const CHUNK_BLOCKS: u32 = 1024 * 1024 / EFS_BLOCKSIZE as u32; // 1 MiB
    let mut buf = vec![0u8; (CHUNK_BLOCKS as usize) * EFS_BLOCKSIZE as usize];
    let mut written: u64 = 0;
    for ext in &extents {
        let mut remaining_blocks = ext.length as u32;
        let mut cur_bn = ext.bn;
        while remaining_blocks > 0 {
            let take_blocks = remaining_blocks.min(CHUNK_BLOCKS);
            let bytes = take_blocks as u64 * EFS_BLOCKSIZE;
            read_at_aligned(
                reader,
                partition_offset + cur_bn as u64 * EFS_BLOCKSIZE,
                bytes,
                &mut buf[..bytes as usize],
            )?;
            let remaining_file = size - written;
            let emit = remaining_file.min(bytes) as usize;
            writer.write_all(&buf[..emit])?;
            written += emit as u64;
            if written >= size {
                return Ok(written);
            }
            cur_bn += take_blocks;
            remaining_blocks -= take_blocks;
        }
    }
    Ok(written)
}

/// Sector-aligned read helper. Reads `len` bytes starting at byte offset
/// `byte_off` in the underlying file/device into `out` (which must be at
/// least `len` bytes long). `byte_off` and `len` must already be 512-byte
/// aligned; this helper enforces that, since EFS only ever reads whole
/// sectors. A short read past the end of the underlying stream is reported
/// as `Io(UnexpectedEof)`.
fn read_at_aligned<R: Read + Seek>(
    reader: &mut R,
    byte_off: u64,
    len: u64,
    out: &mut [u8],
) -> Result<(), FilesystemError> {
    debug_assert!(byte_off.is_multiple_of(EFS_BLOCKSIZE));
    debug_assert!(len.is_multiple_of(EFS_BLOCKSIZE));
    let len = len as usize;
    if out.len() < len {
        return Err(FilesystemError::InvalidData(format!(
            "read_at_aligned: output buffer {} < {len}",
            out.len()
        )));
    }
    reader.seek(SeekFrom::Start(byte_off))?;
    let mut filled = 0;
    while filled < len {
        let n = reader.read(&mut out[filled..len])?;
        if n == 0 {
            return Err(FilesystemError::Io(std::io::Error::new(
                std::io::ErrorKind::UnexpectedEof,
                format!("EFS short read at byte {byte_off}: got {filled} of {len}",),
            )));
        }
        filled += n;
    }
    Ok(())
}

/// Per the EFS conventions used by IRIX `mkfs_efs`, file names are
/// 8-bit clean Unix bytes up to 255 bytes long with no embedded NUL
/// or '/'. Empty names are rejected. This matches IRIX `link(2)`
/// semantics.
#[allow(dead_code)] // wired up in EFS Slice 4 (dir_insert) callers
pub(crate) fn validate_name(name: &[u8]) -> Result<(), FilesystemError> {
    if name.is_empty() {
        return Err(FilesystemError::InvalidData(
            "EFS: empty name is not allowed".into(),
        ));
    }
    if name.len() > 255 {
        return Err(FilesystemError::InvalidData(format!(
            "EFS: name length {} exceeds 255",
            name.len()
        )));
    }
    if name.iter().any(|&b| b == 0 || b == b'/') {
        return Err(FilesystemError::InvalidData(
            "EFS: name contains NUL or '/'".into(),
        ));
    }
    Ok(())
}

fn trim_ascii(b: &[u8]) -> String {
    let end = b.iter().position(|&c| c == 0).unwrap_or(b.len());
    String::from_utf8_lossy(&b[..end])
        .trim_matches(|c: char| c == ' ' || c == '\0')
        .to_string()
}

/// Build a freshly-formatted EFS volume in memory.
///
/// Produces a single-cylinder-group EFS volume that `EfsFilesystem::open`
/// can read and write. The root inode (inum 2) is a directory with one
/// allocated 512-byte dirblock — empty, so the volume mounts as a clean
/// `/` with no entries.
///
/// `size_bytes` must be at least 32 KiB. `name` is the 6-byte
/// `fname`/`fpack` short label (padded with spaces; characters beyond 6
/// bytes are truncated).
pub fn create_blank_efs(size_bytes: u64, name: &str) -> anyhow::Result<Vec<u8>> {
    use byteorder::{BigEndian, ByteOrder};

    if size_bytes < 32 * 1024 {
        return Err(anyhow::anyhow!(
            "EFS volume must be at least 32 KiB, got {size_bytes}"
        ));
    }
    let total_blocks_u64 = size_bytes / EFS_BLOCKSIZE;
    if total_blocks_u64 > u32::MAX as u64 {
        return Err(anyhow::anyhow!(
            "EFS volume size {size_bytes} exceeds u32 block range"
        ));
    }
    let total_blocks = total_blocks_u64 as u32;

    // Inode region size in blocks. 4 inodes per block (128-byte inodes).
    //   ≤ 4 MiB:    8 blocks (32 inodes)
    //   ≤ 64 MiB:   32 blocks (128 inodes)
    //   ≤ 512 MiB:  128 blocks (512 inodes)
    //   larger:     512 blocks (2048 inodes)
    let cgisize: u16 = match size_bytes {
        0..=4_194_304 => 8,
        4_194_305..=67_108_864 => 32,
        67_108_865..=536_870_912 => 128,
        _ => 512,
    };
    // Bitmap byte size: 1 bit per block, packed. Sized first because the
    // bitmap region (blocks 2..2+bm_sectors) must fit *before* the inode
    // region at `firstcg`.
    let bmsize: u32 = (total_blocks as usize + 7) as u32 / 8;
    let bm_sectors = bmsize.div_ceil(EFS_BLOCKSIZE as u32);
    // Single cylinder group layout. `firstcg` reserves boot(1) + sb(1) +
    // the bitmap before the inode region. A floor of 18 keeps the classic
    // IRIX layout for small volumes (bitmap ≤ 16 blocks → firstcg stays 18,
    // byte-identical to before); larger volumes grow firstcg so the bitmap
    // fits. The reader/fsck read `firstcg` from the superblock, so any value
    // is honored.
    let firstcg: u32 = (2 + bm_sectors).max(18);
    // Single CG covers the entire data region past the inode table.
    let cgfsize: u32 = total_blocks - firstcg;
    if cgfsize <= cgisize as u32 + 4 {
        return Err(anyhow::anyhow!(
            "EFS volume too small to fit inode region + a root dirblock"
        ));
    }
    // Root dirblock: first block in the data region past the inode table.
    let root_dirblock: u32 = firstcg + cgisize as u32;
    let replsb: u32 = total_blocks - 1;

    let mut img = vec![0u8; total_blocks as usize * EFS_BLOCKSIZE as usize];

    let mut fname = [b' '; 6];
    let nb = name.as_bytes();
    let n = nb.len().min(6);
    fname[..n].copy_from_slice(&nb[..n]);
    let fpack = fname;

    let sb = EfsSuperblock {
        fs_size: total_blocks,
        firstcg,
        cgfsize,
        cgisize,
        sectors: 63,
        heads: 1,
        ncg: 1,
        dirty: 0,
        fs_time: 0,
        magic: EFS_MAGIC_OLD,
        fname,
        fpack,
        bmsize,
        tfree: 0, // not maintained by our reader; fsck recomputes if needed
        tinode: 0,
        bmblock: 2,
        replsb,
        lastialloc: 2, // last allocated inode = root
        checksum: 0,
    };
    let sb_off = EFS_BLOCKSIZE as usize;
    sb.write_into(&mut img[sb_off..sb_off + EFS_SUPERBLOCK_SIZE]);
    // Replica superblock at last block.
    let rep_off = replsb as usize * EFS_BLOCKSIZE as usize;
    sb.write_into(&mut img[rep_off..rep_off + EFS_SUPERBLOCK_SIZE]);

    // Bitmap convention: set bit = free, clear bit = in-use.
    let bm_off = 2 * EFS_BLOCKSIZE as usize;
    for i in 0..bmsize as usize {
        img[bm_off + i] = 0xFF;
    }
    // Clear bits for blocks that are in-use:
    //   - boot (0)
    //   - primary sb (1)
    //   - bitmap blocks (2..2+bm_sectors)
    //   - inode region (firstcg .. firstcg+cgisize)
    //   - root dirblock
    //   - last block (replica sb)
    let mut in_use: Vec<u32> = Vec::new();
    in_use.push(0);
    in_use.push(1);
    for b in 2..2 + bm_sectors {
        in_use.push(b);
    }
    for b in firstcg..firstcg + cgisize as u32 {
        in_use.push(b);
    }
    in_use.push(root_dirblock);
    in_use.push(replsb);
    for b in in_use {
        let by = (b / 8) as usize;
        let bit = 7 - (b % 8) as usize;
        img[bm_off + by] &= !(1u8 << bit);
    }

    // Root inode (inum 2) at firstcg*BLOCKSIZE + 2*128.
    let root_inode_off = firstcg as usize * EFS_BLOCKSIZE as usize + 2 * EFS_INODESIZE as usize;
    let mut root = EfsInode::empty(2);
    root.mode = 0o040755; // directory, rwxr-xr-x
    root.nlink = 2; // . and ..
    root.size = EFS_BLOCKSIZE as u32;
    root.numextents = 1;
    root.extents[0] = EfsExtent {
        magic: 0,
        bn: root_dirblock,
        length: 1,
        offset: 0,
    };
    let mut slot = [0u8; EFS_INODESIZE as usize];
    root.write_into(&mut slot);
    img[root_inode_off..root_inode_off + EFS_INODESIZE as usize].copy_from_slice(&slot);

    // Root dirblock: just the EFS_DIRBLK header. No entries.
    let dirblk_off = root_dirblock as usize * EFS_BLOCKSIZE as usize;
    BigEndian::write_u16(&mut img[dirblk_off..dirblk_off + 2], EFS_DIRBLK_MAGIC);
    img[dirblk_off + 2] = 0; // slot count
    img[dirblk_off + 3] = 0; // first free byte

    Ok(img)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Cursor;

    fn load_fixture() -> Vec<u8> {
        let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("tests/fixtures/sgi/efs_small.img.zst");
        let compressed = std::fs::read(&path).expect("fixture present");
        let mut decoder =
            zstd::stream::read::Decoder::new(Cursor::new(compressed)).expect("zstd decoder");
        let mut out = Vec::new();
        decoder.read_to_end(&mut out).expect("decompress");
        out
    }

    #[test]
    fn parses_superblock_from_fixture() {
        let img = load_fixture();
        let fs = EfsFilesystem::open(Cursor::new(img), 0).expect("open EFS");
        // From docs/SGI_Filesystems.md "Known images on hand" (IRIX 5.3).
        assert_eq!(fs.sb.fs_size, 7_486_242);
        assert_eq!(fs.sb.firstcg, 1830);
        assert_eq!(fs.sb.cgfsize, 95_954);
        assert_eq!(fs.sb.cgisize, 2460);
        assert_eq!(fs.sb.sectors, 63);
        assert_eq!(fs.sb.heads, 10);
        assert_eq!(fs.sb.ncg, 78);
        assert_eq!(fs.sb.magic, EFS_MAGIC_OLD);
        assert_eq!(fs.fs_type(), "EFS");
        assert_eq!(fs.volume_label(), Some("noname:nopack"));
        assert_eq!(fs.total_size(), 7_486_242 * 512);
    }

    #[test]
    fn superblock_parses_bitmap_and_replica_fields_from_fixture() {
        let img = load_fixture();
        let fs = EfsFilesystem::open(Cursor::new(img), 0).expect("open EFS");
        // Sanity bounds, derived from the parsed superblock geometry —
        // we don't hard-code expected values for fields the fixture
        // documentation didn't pin down, but we *do* verify they're
        // internally consistent: bitmap and replica live inside the
        // volume, and the bitmap covers exactly the data-block count.
        let sb = &fs.sb;
        // IRIX mkfs_efs writes bmblock=0 in some images (the kernel
        // read driver doesn't consult the field); we accept either an
        // explicit pointer or a 0 sentinel here. The resize code in
        // later slices will derive the effective location when 0.
        if sb.bmblock != 0 {
            assert!(
                sb.bmblock < sb.fs_size,
                "bmblock {} not inside volume of {} blocks",
                sb.bmblock,
                sb.fs_size
            );
        }
        // replsb on the fixture lives ~47 blocks past fs_size — by IRIX
        // convention `fs_size` counts only the formal data region and
        // the replica sits in a small trailing reserved zone. Accept
        // any non-zero value within a generous bound.
        assert!(
            sb.replsb != 0 && (sb.replsb as u64) < sb.fs_size as u64 + 1024,
            "replsb {} not within reasonable bound of fs_size {}",
            sb.replsb,
            sb.fs_size
        );
        assert!(sb.bmsize > 0, "bmsize must cover at least some data blocks");
    }

    #[test]
    fn superblock_round_trips_through_write_into_then_parse() {
        let img = load_fixture();
        let fs = EfsFilesystem::open(Cursor::new(img), 0).expect("open EFS");
        let mut buf = [0u8; EFS_SUPERBLOCK_SIZE];
        fs.sb.write_into(&mut buf);
        let again = EfsSuperblock::parse(&buf).expect("parse round-trip");
        assert_eq!(again, fs.sb);
    }

    #[test]
    fn replica_superblock_matches_primary_on_fixture() {
        // The replica lives at sb.replsb. Reading it as a superblock
        // should parse to the same fields as the primary, modulo the
        // dirty bit + fs_time (kernel may update these out of sync).
        let img = load_fixture();
        let primary = EfsFilesystem::open(Cursor::new(img.clone()), 0).expect("open primary");
        let replica_byte = primary.sb.replsb as u64 * EFS_BLOCKSIZE;
        // The replica may live past the small fixture window — skip
        // the assertion if so. The behavior we care about is "if the
        // replica is reachable, it equals the primary in the
        // resize-relevant fields."
        if (replica_byte + EFS_BLOCKSIZE) as usize > img.len() {
            return;
        }
        let mut block = [0u8; EFS_BLOCKSIZE as usize];
        block.copy_from_slice(&img[replica_byte as usize..(replica_byte + EFS_BLOCKSIZE) as usize]);
        let replica = EfsSuperblock::parse(&block).expect("parse replica");
        assert_eq!(replica.fs_size, primary.sb.fs_size);
        assert_eq!(replica.firstcg, primary.sb.firstcg);
        assert_eq!(replica.cgfsize, primary.sb.cgfsize);
        assert_eq!(replica.cgisize, primary.sb.cgisize);
        assert_eq!(replica.ncg, primary.sb.ncg);
        assert_eq!(replica.bmsize, primary.sb.bmsize);
        assert_eq!(replica.bmblock, primary.sb.bmblock);
        assert_eq!(replica.replsb, primary.sb.replsb);
    }

    #[test]
    fn rejects_wrong_magic() {
        let mut img = vec![0u8; 4096];
        // Write a deliberately-wrong magic at the superblock magic offset.
        img[512 + 28..512 + 32].copy_from_slice(&0xDEAD_BEEFu32.to_be_bytes());
        match EfsFilesystem::open(Cursor::new(img), 0) {
            Err(FilesystemError::Parse(_)) => {}
            Err(e) => panic!("expected Parse error, got: {e}"),
            Ok(_) => panic!("expected error on bad magic"),
        }
    }

    #[test]
    fn inode_offset_matches_hand_computation() {
        let img = load_fixture();
        let fs = EfsFilesystem::open(Cursor::new(img), 0).expect("open EFS");
        let _ = &fs;
        // Root inode (2): cg=0, inblock=0, inblock_offset=2*128.
        // byte = firstcg * 512 + 256 = 1830 * 512 + 256 = 937216 = 0xE4D00.
        assert_eq!(fs.inode_byte_offset(2), 0xE4D00);
        // Inode 4: still cg=0, block_in_cg=0, byte_off_in_block=4*128 already > 512,
        // so block_in_cg=1, byte_off=0. byte = (1830+1)*512 = 0xE4E00.
        // Wait — inblock = (4 % 9840) / 4 = 1. byte_in_block = (4 % 4) * 128 = 0.
        // byte = (1830 + 1) * 512 = 937472 = 0xE4E00.
        assert_eq!(fs.inode_byte_offset(4), 0xE4E00);
    }

    #[test]
    fn root_directory_lists_expected_entries() {
        let img = load_fixture();
        let mut fs = EfsFilesystem::open(Cursor::new(img), 0).expect("open EFS");
        let root = fs.root().expect("root inode");
        assert!(root.is_directory());

        let entries = fs.list_directory(&root).expect("list root");
        let names: Vec<&str> = entries.iter().map(|e| e.name.as_str()).collect();

        // From manual decode of block 4290 of the fixture: the root has
        // these names. Asserting subset rather than full equality because
        // the second directory block of the root lives past the 4 MiB
        // fixture window and is correctly skipped.
        for expected in [
            "lost+found",
            "etc",
            "dev",
            "bin",
            "sbin",
            "tmp",
            "stand",
            "lib",
            "proc",
            "opt",
            "temp",
        ] {
            assert!(names.contains(&expected), "missing {expected} in {names:?}");
        }
        // "." and ".." must be filtered out.
        assert!(!names.contains(&"."), "should not list '.'");
        assert!(!names.contains(&".."), "should not list '..'");
    }

    #[test]
    fn reads_small_single_extent_file() {
        // .Sgiresources: inode 16, size=21, single extent at block 4312.
        // Content was confirmed via xxd of the fixture.
        let img = load_fixture();
        let mut fs = EfsFilesystem::open(Cursor::new(img), 0).expect("open EFS");
        let root = fs.root().unwrap();
        let entries = fs.list_directory(&root).unwrap();
        let entry = entries
            .iter()
            .find(|e| e.name == ".Sgiresources")
            .expect("entry");
        assert_eq!(entry.size, 21);
        let data = fs.read_file(entry, usize::MAX).expect("read");
        assert_eq!(data, b"*scheme:\tIndigoMagic\n");
    }

    #[test]
    fn read_file_respects_max_bytes() {
        let img = load_fixture();
        let mut fs = EfsFilesystem::open(Cursor::new(img), 0).expect("open EFS");
        let root = fs.root().unwrap();
        let entries = fs.list_directory(&root).unwrap();
        let entry = entries
            .iter()
            .find(|e| e.name == ".Sgiresources")
            .expect("entry");
        let head = fs.read_file(entry, 8).expect("read");
        assert_eq!(head, b"*scheme:");
    }

    #[test]
    fn reads_multi_block_file() {
        // .audiopanelrc: inode 17, size=566, single extent (4313, length=2).
        // The 566-byte file spans two 512-byte blocks; verify size+ first/last bytes.
        let img = load_fixture();
        let mut fs = EfsFilesystem::open(Cursor::new(img), 0).expect("open EFS");
        let root = fs.root().unwrap();
        let entries = fs.list_directory(&root).unwrap();
        let entry = entries
            .iter()
            .find(|e| e.name == ".audiopanelrc")
            .expect("entry");
        assert_eq!(entry.size, 566);
        let data = fs.read_file(entry, usize::MAX).expect("read");
        assert_eq!(data.len(), 566);
    }

    #[test]
    fn write_file_to_matches_read_file() {
        let img = load_fixture();
        let mut fs = EfsFilesystem::open(Cursor::new(img), 0).expect("open EFS");
        let root = fs.root().unwrap();
        let entries = fs.list_directory(&root).unwrap();
        let entry = entries.iter().find(|e| e.name == ".cshrc").expect("entry");
        let direct = fs.read_file(entry, usize::MAX).expect("read");
        let mut streamed = Vec::new();
        let n = fs.write_file_to(entry, &mut streamed).expect("stream");
        assert_eq!(n as usize, direct.len());
        assert_eq!(streamed, direct);
        assert_eq!(streamed.len(), entry.size as usize);
    }

    #[test]
    fn list_directory_descends_into_subdirectory() {
        // .desktop-IRIS (inode 22) is a subdirectory whose data block is
        // inside the fixture window. Verify list_directory traverses it.
        let img = load_fixture();
        let mut fs = EfsFilesystem::open(Cursor::new(img), 0).expect("open EFS");
        let root = fs.root().unwrap();
        let entries = fs.list_directory(&root).unwrap();
        let subdir = entries
            .iter()
            .find(|e| e.name == ".desktop-IRIS")
            .expect("subdir entry");
        assert!(subdir.is_directory());
        let children = fs.list_directory(subdir).expect("list subdir");
        let child_names: Vec<&str> = children.iter().map(|c| c.name.as_str()).collect();
        for expected in [
            "log",
            "log.bak",
            "iconbook",
            "ScreenSaver",
            "4Dwmsession",
            ".userenv",
        ] {
            assert!(
                child_names.contains(&expected),
                "missing {expected} in {child_names:?}"
            );
        }
        // Paths nested under the parent's path.
        for c in &children {
            assert!(
                c.path.starts_with("/.desktop-IRIS/"),
                "bad nested path: {}",
                c.path
            );
        }
    }

    #[test]
    fn synthetic_symlink_populates_target() {
        // Build a tiny synthetic EFS image with: superblock at sector 1,
        // root inode (2) as a one-block directory listing a single symlink
        // child (inode 4) whose data block holds the target string.
        //
        // Layout (sectors / 512B blocks):
        //   0: pad
        //   1: superblock
        //   2..18: pad to firstcg
        //   18..20: inode area (cgisize=2 -> 8 inodes total, inum 2,3,4 fit)
        //   20: root directory block
        //   21: symlink target data
        let mut img = vec![0u8; 32 * 512];

        // Superblock at sector 1.
        let sb_off = 512;
        let total_blocks = (img.len() / 512) as u32;
        img[sb_off..sb_off + 4].copy_from_slice(&total_blocks.to_be_bytes()); // fs_size
        img[sb_off + 4..sb_off + 8].copy_from_slice(&18u32.to_be_bytes()); // firstcg
        img[sb_off + 8..sb_off + 12].copy_from_slice(&14u32.to_be_bytes()); // cgfsize
        img[sb_off + 12..sb_off + 14].copy_from_slice(&2u16.to_be_bytes()); // cgisize
        img[sb_off + 14..sb_off + 16].copy_from_slice(&63u16.to_be_bytes()); // sectors
        img[sb_off + 16..sb_off + 18].copy_from_slice(&1u16.to_be_bytes()); // heads
        img[sb_off + 18..sb_off + 20].copy_from_slice(&1u16.to_be_bytes()); // ncg
        img[sb_off + 28..sb_off + 32].copy_from_slice(&EFS_MAGIC_OLD.to_be_bytes());
        // fname / fpack — leave zero.

        // Inode area starts at block 18.
        // Inode 2 (root dir): one extent pointing to block 20.
        let inode2_off = 18 * 512 + 2 * 128;
        // mode 0o040755 directory
        img[inode2_off..inode2_off + 2].copy_from_slice(&0o040755u16.to_be_bytes());
        // size 512 (one dir block)
        img[inode2_off + 8..inode2_off + 12].copy_from_slice(&512u32.to_be_bytes());
        // numextents 1
        img[inode2_off + 28..inode2_off + 30].copy_from_slice(&1u16.to_be_bytes());
        // extent: bn=20, length=1, offset=0
        let w0: u32 = 20;
        let w1: u32 = 1u32 << 24;
        img[inode2_off + 32..inode2_off + 36].copy_from_slice(&w0.to_be_bytes());
        img[inode2_off + 36..inode2_off + 40].copy_from_slice(&w1.to_be_bytes());

        // Inode 4 (symlink): one extent at block 21, target "/usr/sbin/init".
        let target = b"/usr/sbin/init";
        let inode4_off = 18 * 512 + 4 * 128;
        img[inode4_off..inode4_off + 2].copy_from_slice(&0o120777u16.to_be_bytes());
        img[inode4_off + 8..inode4_off + 12].copy_from_slice(&(target.len() as u32).to_be_bytes());
        img[inode4_off + 28..inode4_off + 30].copy_from_slice(&1u16.to_be_bytes());
        let w0: u32 = 21;
        let w1: u32 = 1u32 << 24;
        img[inode4_off + 32..inode4_off + 36].copy_from_slice(&w0.to_be_bytes());
        img[inode4_off + 36..inode4_off + 40].copy_from_slice(&w1.to_be_bytes());

        // Directory block at block 20: one entry "link" -> inode 4.
        let dir_off = 20 * 512;
        img[dir_off..dir_off + 2].copy_from_slice(&EFS_DIRBLK_MAGIC.to_be_bytes());
        // firstused (unused by our reader) and slots
        img[dir_off + 2] = 0xfd;
        img[dir_off + 3] = 1;
        // slot 0 byte at offset 4. Dirent at byte 502 -> stored value 502/2 = 251 = 0xfb.
        // (Dirent length is 4 + 1 + 4 = 9, must fit within the 512-byte block.)
        img[dir_off + 4] = 0xfb;
        let de_off = dir_off + 502;
        img[de_off..de_off + 4].copy_from_slice(&4u32.to_be_bytes());
        img[de_off + 4] = 4;
        img[de_off + 5..de_off + 9].copy_from_slice(b"link");

        // Symlink target data at block 21.
        let data_off = 21 * 512;
        img[data_off..data_off + target.len()].copy_from_slice(target);

        let mut fs = EfsFilesystem::open(Cursor::new(img), 0).expect("open EFS");
        let root = fs.root().unwrap();
        let children = fs.list_directory(&root).expect("list root");
        let link = children
            .iter()
            .find(|e| e.name == "link")
            .expect("link entry");
        assert!(link.is_symlink());
        assert_eq!(link.symlink_target.as_deref(), Some("/usr/sbin/init"));
        // read_file on the symlink returns the target bytes verbatim.
        let bytes = fs.read_file(link, usize::MAX).unwrap();
        assert_eq!(bytes, target);
    }

    #[test]
    fn synthetic_multi_extent_read() {
        // Build an image with a regular file (inode 4) that spans two
        // disjoint extents. Verify read_file concatenates them in
        // logical-offset order and matches write_file_to.
        let mut img = vec![0u8; 64 * 512];

        let sb_off = 512;
        let total_blocks = (img.len() / 512) as u32;
        img[sb_off..sb_off + 4].copy_from_slice(&total_blocks.to_be_bytes());
        img[sb_off + 4..sb_off + 8].copy_from_slice(&18u32.to_be_bytes());
        img[sb_off + 8..sb_off + 12].copy_from_slice(&44u32.to_be_bytes());
        img[sb_off + 12..sb_off + 14].copy_from_slice(&2u16.to_be_bytes());
        img[sb_off + 18..sb_off + 20].copy_from_slice(&1u16.to_be_bytes());
        img[sb_off + 28..sb_off + 32].copy_from_slice(&EFS_MAGIC_OLD.to_be_bytes());

        // Inode 4: 2 extents, block 30 (1 block) at offset 0, block 50 (1 block) at offset 1.
        let ino_off = 18 * 512 + 4 * 128;
        img[ino_off..ino_off + 2].copy_from_slice(&0o100644u16.to_be_bytes()); // regular file
        img[ino_off + 8..ino_off + 12].copy_from_slice(&900u32.to_be_bytes()); // size 900
        img[ino_off + 28..ino_off + 30].copy_from_slice(&2u16.to_be_bytes());

        // Extent 0: bn=30, length=1, offset=0
        let w0: u32 = 30;
        let w1: u32 = 1 << 24;
        img[ino_off + 32..ino_off + 36].copy_from_slice(&w0.to_be_bytes());
        img[ino_off + 36..ino_off + 40].copy_from_slice(&w1.to_be_bytes());
        // Extent 1: bn=50, length=1, offset=1
        let w0: u32 = 50;
        let w1: u32 = (1 << 24) | 1;
        img[ino_off + 40..ino_off + 44].copy_from_slice(&w0.to_be_bytes());
        img[ino_off + 44..ino_off + 48].copy_from_slice(&w1.to_be_bytes());

        // Fill block 30 with 0xAA, block 50 with 0xBB.
        img[30 * 512..31 * 512].fill(0xAA);
        img[50 * 512..51 * 512].fill(0xBB);

        let mut fs = EfsFilesystem::open(Cursor::new(img), 0).expect("open EFS");
        // Synthesize a FileEntry that points at inode 4 without going through
        // list_directory (root inode 2 isn't populated in this image).
        let entry = FileEntry::new_file("data".into(), "/data".into(), 900, 4);
        let data = fs.read_file(&entry, usize::MAX).unwrap();
        assert_eq!(data.len(), 900);
        assert!(data[..512].iter().all(|&b| b == 0xAA));
        assert!(data[512..900].iter().all(|&b| b == 0xBB));

        let mut streamed = Vec::new();
        fs.write_file_to(&entry, &mut streamed).unwrap();
        assert_eq!(streamed, data);
    }

    /// Build a synthetic EFS image with a writable bitmap of `bmsize`
    /// bytes at block `bmblock`. Returns the image and the parsed
    /// filesystem. Used by the Slice-2 allocator tests.
    fn build_synthetic_for_bitmap_tests() -> Vec<u8> {
        // Layout (sectors):
        //   0: pad
        //   1: superblock
        //   2: bitmap (one sector = 4096 bits = up to 4096 data blocks)
        //   3..18: pad to firstcg
        //   18: CG 0 inode region start
        //   etc.
        let mut img = vec![0u8; 256 * 512];
        let sb_off = 512;
        let total_blocks = (img.len() / 512) as u32;
        let sb = EfsSuperblock {
            fs_size: total_blocks,
            firstcg: 18,
            cgfsize: 64,
            cgisize: 2,
            sectors: 63,
            heads: 1,
            ncg: 1,
            dirty: 0,
            fs_time: 0,
            magic: EFS_MAGIC_OLD,
            fname: *b"alloct",
            fpack: *b"alloct",
            bmsize: 32, // 32 bytes = 256 bits — covers `total_blocks`
            tfree: 0,
            tinode: 0,
            bmblock: 2,
            replsb: total_blocks - 1,
            lastialloc: 0,
            checksum: 0,
        };
        sb.write_into(&mut img[sb_off..sb_off + EFS_SUPERBLOCK_SIZE]);
        // Bitmap convention: set bit = free, clear bit = in-use. Fill
        // the bitmap region with 0xFF (all free), then CLEAR bits for
        // boot, primary SB, bitmap, CG 0 inode region, and last block.
        let bm_off = 2 * 512;
        for b in 0..sb.bmsize as usize {
            img[bm_off + b] = 0xFF;
        }
        let in_use_blocks = [0u32, 1, 2, 18, 19, total_blocks - 1];
        for b in in_use_blocks {
            let by = (b / 8) as usize;
            let bb = 7 - (b % 8);
            img[bm_off + by] &= !(1 << bb);
        }
        img
    }

    #[test]
    fn read_bitmap_round_trips_through_write_bitmap() {
        let img = build_synthetic_for_bitmap_tests();
        let mut fs = EfsFilesystem::open(Cursor::new(img), 0).expect("open");
        let bm = fs.read_bitmap().expect("read bitmap");
        assert_eq!(bm.len(), fs.sb.bmsize as usize);
        // Convention: set bit = free. Block 0 (boot) is in-use so bit
        // is clear; block 100 (inside free region) has bit set.
        assert!(bm[0] & 0b1000_0000 == 0, "block 0 must be marked in-use");
        assert!(bm[100 / 8] & (1 << (7 - (100 % 8))) != 0, "block 100 free");
        // Round-trip: write back and re-read; expect identical bytes.
        let mut bm2 = bm.clone();
        bm2[5] = 0xFF;
        fs.write_bitmap(&bm2).expect("write bitmap");
        let bm3 = fs.read_bitmap().expect("re-read bitmap");
        assert_eq!(bm3, bm2);
    }

    #[test]
    fn alloc_contiguous_finds_first_fit_and_marks_in_use() {
        let img = build_synthetic_for_bitmap_tests();
        let mut fs = EfsFilesystem::open(Cursor::new(img), 0).expect("open");
        let mut bm = fs.read_bitmap().unwrap();
        let ext = EfsFilesystem::<Cursor<Vec<u8>>>::alloc_contiguous_in_bitmap(&mut bm, 4, 20)
            .expect("alloc 4 blocks");
        // First-fit with avoid_below=20 in the synthetic gives bn=20
        // (block 19 is in-use; 20..23 are free).
        assert_eq!(ext.bn, 20);
        assert_eq!(ext.length, 4);
        // The bits should now read as in-use (set bit = free, so
        // in-use means bit is cleared).
        for b in 20..24 {
            let by = (b / 8) as usize;
            let bb = 7 - (b % 8);
            assert!(bm[by] & (1 << bb) == 0, "block {b} not marked");
        }
        // A second alloc starts past the run we just took.
        let ext2 = EfsFilesystem::<Cursor<Vec<u8>>>::alloc_contiguous_in_bitmap(&mut bm, 2, 20)
            .expect("alloc 2 more");
        assert_eq!(ext2.bn, 24);
    }

    #[test]
    fn alloc_contiguous_errors_when_no_run_fits() {
        // 256-bit bitmap with everything in-use except a single block
        // at index 50 — request for 4 contiguous blocks must fail.
        // set bit = free; start fully in-use (all zeros) then set bit 50.
        let mut bm = vec![0u8; 32];
        let b = 50usize;
        bm[b / 8] |= 1 << (7 - (b % 8));
        let err = EfsFilesystem::<Cursor<Vec<u8>>>::alloc_contiguous_in_bitmap(&mut bm, 4, 0)
            .expect_err("expected DiskFull");
        assert!(matches!(err, FilesystemError::DiskFull(_)), "got {err:?}");
    }

    #[test]
    fn free_extent_returns_blocks_to_pool() {
        // Start fully in-use (set bit = free; all zeros = all in-use).
        let mut bm = vec![0u8; 32];
        let ext = EfsExtent {
            magic: 0,
            bn: 8,
            length: 8,
            offset: 0,
        };
        EfsFilesystem::<Cursor<Vec<u8>>>::free_extent_in_bitmap(&mut bm, &ext);
        for b in ext.bn..ext.bn + ext.length as u32 {
            let by = (b / 8) as usize;
            let bb = 7 - (b % 8);
            assert!(bm[by] & (1 << bb) != 0, "block {b} still in-use after free");
        }
    }

    #[test]
    fn inode_write_round_trips_through_read() {
        let img = build_synthetic_for_bitmap_tests();
        let mut fs = EfsFilesystem::open(Cursor::new(img), 0).expect("open");
        // Synthetic image has every inode zero (mode==0). Allocate
        // and write a fresh one, then read it back.
        let inum = fs.allocate_inode().expect("alloc inode");
        assert_eq!(inum, 2, "first free inode should be root (inum 2)");

        let mut ino = EfsInode::empty(inum);
        ino.mode = 0o100644;
        ino.nlink = 1;
        ino.size = 1234;
        ino.numextents = 1;
        ino.extents[0] = EfsExtent {
            magic: 0,
            bn: 25,
            length: 3,
            offset: 0,
        };
        fs.write_inode(&ino).expect("write inode");

        let read_back = fs.read_inode(inum).expect("read back");
        assert_eq!(read_back.mode, 0o100644);
        assert_eq!(read_back.size, 1234);
        assert_eq!(read_back.numextents, 1);
        assert_eq!(read_back.extents[0].bn, 25);
        assert_eq!(read_back.extents[0].length, 3);
    }

    #[test]
    fn write_inode_preserves_neighboring_slots_in_block() {
        // Two inodes in the same 512-byte block. Writing one must not
        // disturb the other.
        let img = build_synthetic_for_bitmap_tests();
        let mut fs = EfsFilesystem::open(Cursor::new(img), 0).expect("open");

        // Inode 2 and inode 3 share the first inode-table block of CG 0.
        let mut ino2 = EfsInode::empty(2);
        ino2.mode = 0o040755;
        ino2.size = 100;
        fs.write_inode(&ino2).expect("write inode 2");

        let mut ino3 = EfsInode::empty(3);
        ino3.mode = 0o100644;
        ino3.size = 200;
        fs.write_inode(&ino3).expect("write inode 3");

        // Reading inode 2 must still see size=100, mode=040755.
        let r2 = fs.read_inode(2).expect("re-read 2");
        assert_eq!(r2.mode, 0o040755);
        assert_eq!(r2.size, 100);
        let r3 = fs.read_inode(3).expect("re-read 3");
        assert_eq!(r3.mode, 0o100644);
        assert_eq!(r3.size, 200);
    }

    #[test]
    fn create_blank_efs_round_trips_through_open() {
        let img = create_blank_efs(1024 * 1024, "rb-efs").expect("format 1M EFS");
        let mut fs = EfsFilesystem::open(Cursor::new(img), 0).expect("open");
        let root = fs.root().expect("root");
        assert!(root.is_directory());
        let entries = fs.list_directory(&root).expect("list root");
        assert!(entries.is_empty(), "fresh EFS must have an empty root");
    }

    #[test]
    fn allocate_inode_returns_disk_full_when_all_used() {
        let img = build_synthetic_for_bitmap_tests();
        let mut fs = EfsFilesystem::open(Cursor::new(img), 0).expect("open");
        // Fill every inode from 2..total with a non-zero mode.
        let total = fs.total_inodes();
        for inum in 2..total {
            let mut ino = EfsInode::empty(inum);
            ino.mode = 0o100644;
            fs.write_inode(&ino).expect("write");
        }
        let err = fs.allocate_inode().expect_err("expected DiskFull");
        assert!(matches!(err, FilesystemError::DiskFull(_)), "got {err:?}");
    }

    /// Build a synthetic EFS image whose root inode (2) is a directory
    /// with one allocated dirblock at disk block 25. Bitmap covers
    /// `total_blocks` blocks; blocks 0,1,2 (boot/sb/bitmap), 18..20
    /// (inode region), and 25 (root dirblock) are marked in-use.
    fn build_synthetic_with_root_dir() -> Vec<u8> {
        let mut img = vec![0u8; 256 * 512];
        let sb_off = 512;
        let total_blocks = (img.len() / 512) as u32;
        let sb = EfsSuperblock {
            fs_size: total_blocks,
            firstcg: 18,
            cgfsize: 64,
            cgisize: 2,
            sectors: 63,
            heads: 1,
            ncg: 1,
            dirty: 0,
            fs_time: 0,
            magic: EFS_MAGIC_OLD,
            fname: *b"dirtst",
            fpack: *b"dirtst",
            bmsize: 32,
            tfree: 0,
            tinode: 0,
            bmblock: 2,
            replsb: total_blocks - 1,
            lastialloc: 2,
            checksum: 0,
        };
        sb.write_into(&mut img[sb_off..sb_off + EFS_SUPERBLOCK_SIZE]);

        // Bitmap: set bit = free. Fill with 0xFF, then clear in-use bits.
        let bm_off = 2 * 512;
        for b in 0..sb.bmsize as usize {
            img[bm_off + b] = 0xFF;
        }
        for b in [0u32, 1, 2, 18, 19, 25, total_blocks - 1] {
            let by = (b / 8) as usize;
            let bb = 7 - (b % 8);
            img[bm_off + by] &= !(1 << bb);
        }

        // Root inode (2) at byte (firstcg=18)*512 + 2*128.
        let ino2_off = 18 * 512 + 2 * 128;
        let root = EfsInode {
            inum: 2,
            mode: 0o040755,
            nlink: 2,
            uid: 0,
            gid: 0,
            size: 512,
            atime: 0,
            mtime: 0,
            ctime: 0,
            gen: 0,
            numextents: 1,
            version: 0,
            extents: {
                let mut e = [EfsExtent {
                    magic: 0,
                    bn: 0,
                    length: 0,
                    offset: 0,
                }; EFS_DIRECTEXTENTS];
                e[0] = EfsExtent {
                    magic: 0,
                    bn: 25,
                    length: 1,
                    offset: 0,
                };
                e
            },
        };
        let mut slot = [0u8; 128];
        root.write_into(&mut slot);
        img[ino2_off..ino2_off + 128].copy_from_slice(&slot);

        // Root dirblock at block 25: empty for now (no entries).
        let mut dirblk = [0u8; EFS_BLOCKSIZE as usize];
        BigEndian::write_u16(&mut dirblk[0..2], EFS_DIRBLK_MAGIC);
        dirblk[2] = 0;
        dirblk[3] = 0;
        img[25 * 512..26 * 512].copy_from_slice(&dirblk);

        img
    }

    #[test]
    fn dir_insert_then_find_round_trip() {
        let img = build_synthetic_with_root_dir();
        let mut fs = EfsFilesystem::open(Cursor::new(img), 0).expect("open");
        let mut root = fs.read_inode(2).expect("root");
        let mut bm = fs.read_bitmap().expect("bm");

        fs.dir_insert(&mut root, &mut bm, b"hello", 17)
            .expect("insert");
        fs.dir_insert(&mut root, &mut bm, b"world", 18)
            .expect("insert");

        // Persist the inode so subsequent reads via inode-2 see the
        // new entries; bitmap unchanged here because we appended to
        // the existing dirblock.
        fs.write_inode(&root).expect("write inode");

        let h = fs.dir_find(&root, b"hello").expect("find").expect("found");
        assert_eq!(h, 17);
        let w = fs.dir_find(&root, b"world").expect("find").expect("found");
        assert_eq!(w, 18);
        let none = fs.dir_find(&root, b"missing").expect("find");
        assert_eq!(none, None);
    }

    #[test]
    fn dir_remove_returns_inum_and_drops_entry() {
        let img = build_synthetic_with_root_dir();
        let mut fs = EfsFilesystem::open(Cursor::new(img), 0).expect("open");
        let mut root = fs.read_inode(2).expect("root");
        let mut bm = fs.read_bitmap().expect("bm");
        fs.dir_insert(&mut root, &mut bm, b"one", 10).expect("ins");
        fs.dir_insert(&mut root, &mut bm, b"two", 11).expect("ins");
        let removed = fs.dir_remove(&root, b"one").expect("remove");
        assert_eq!(removed, 10);
        assert_eq!(fs.dir_find(&root, b"one").unwrap(), None);
        assert_eq!(fs.dir_find(&root, b"two").unwrap(), Some(11));
        // Remove again returns NotFound.
        let err = fs.dir_remove(&root, b"one").expect_err("not found");
        assert!(matches!(err, FilesystemError::NotFound(_)));
    }

    #[test]
    fn dir_insert_grows_to_new_block_when_first_full() {
        let img = build_synthetic_with_root_dir();
        let mut fs = EfsFilesystem::open(Cursor::new(img), 0).expect("open");
        let mut root = fs.read_inode(2).expect("root");
        let mut bm = fs.read_bitmap().expect("bm");

        // Fill the first dirblock with as many small entries as fit.
        // Each entry: 5 + 3 = 8 bytes dirent + 1 slot byte = 9 bytes;
        // 4 header bytes + 9 * N <= 512 → N <= 56.
        let mut inserted = 0usize;
        for i in 0..200u32 {
            let name = format!("f{i:03}").into_bytes();
            match fs.dir_insert(&mut root, &mut bm, &name, 100 + i) {
                Ok(()) => inserted += 1,
                Err(_) => break,
            }
            // Stop once we've forced a second extent.
            if root.numextents > 1 {
                inserted += 1;
                continue;
            }
        }
        assert!(
            root.numextents >= 2,
            "expected dir growth: only {} entries inserted, numextents={}",
            inserted,
            root.numextents
        );
        // The bitmap must reflect the new dirblock allocation.
        fs.write_bitmap(&bm).expect("flush bitmap");
        fs.write_inode(&root).expect("write inode");

        // Re-open and verify a few entries land.
        let reopened_img = fs.reader.into_inner();
        let mut fs2 = EfsFilesystem::open(Cursor::new(reopened_img), 0).expect("reopen");
        let root2 = fs2.read_inode(2).expect("root2");
        assert!(root2.numextents >= 2);
        let found = fs2
            .dir_find(&root2, b"f005")
            .expect("find")
            .expect("present");
        assert_eq!(found, 105);
    }

    #[test]
    fn validate_name_rejects_bad_input() {
        assert!(validate_name(b"").is_err());
        assert!(validate_name(b"a/b").is_err());
        assert!(validate_name(b"a\0b").is_err());
        assert!(validate_name(&[b'x'; 256]).is_err());
        assert!(validate_name(b"good_name").is_ok());
        // 255-byte name is the boundary.
        assert!(validate_name(&[b'y'; 255]).is_ok());
    }

    #[test]
    fn editable_create_file_round_trip() {
        use super::super::filesystem::{CreateFileOptions, EditableFilesystem, Filesystem};
        let img = build_synthetic_with_root_dir();
        let mut fs = EfsFilesystem::open(Cursor::new(img), 0).expect("open");
        // Populate sb.tfree to a sensible value so post-create
        // accounting is meaningful (the synthetic image leaves it 0).
        fs.sb.tfree = 200;
        fs.sb.tinode = 8;

        let root = fs.root().expect("root");
        let payload = b"hello world".to_vec();
        let mut cur = Cursor::new(payload.clone());
        let opts = CreateFileOptions {
            mode: Some(0o100600),
            ..Default::default()
        };
        let new_entry = fs
            .create_file(&root, "greeting.txt", &mut cur, payload.len() as u64, &opts)
            .expect("create_file");
        assert!(new_entry.is_file());
        assert_eq!(new_entry.size, payload.len() as u64);

        fs.sync_metadata().expect("sync");

        // Re-open and verify.
        let bytes = fs.reader.into_inner();
        let mut fs2 = EfsFilesystem::open(Cursor::new(bytes), 0).expect("reopen");
        let root2 = fs2.root().unwrap();
        let kids = fs2.list_directory(&root2).expect("list");
        let entry = kids
            .iter()
            .find(|e| e.name == "greeting.txt")
            .expect("file present");
        let data = fs2.read_file(entry, usize::MAX).expect("read");
        assert_eq!(data, payload);
    }

    #[test]
    fn editable_rename_preserves_inode_and_content() {
        use super::super::filesystem::{CreateFileOptions, EditableFilesystem, Filesystem};
        let img = build_synthetic_with_root_dir();
        let mut fs = EfsFilesystem::open(Cursor::new(img), 0).expect("open");
        fs.sb.tfree = 200;
        fs.sb.tinode = 8;

        let root = fs.root().expect("root");
        let payload = b"rename me on efs".to_vec();
        let mut cur = Cursor::new(payload.clone());
        let created = fs
            .create_file(
                &root,
                "old.txt",
                &mut cur,
                payload.len() as u64,
                &CreateFileOptions::default(),
            )
            .expect("create_file");
        let original_inum = created.location;

        fs.rename(&root, &created, "new.txt").expect("rename");
        fs.sync_metadata().expect("sync");

        // Re-open and verify.
        let bytes = fs.reader.into_inner();
        let mut fs2 = EfsFilesystem::open(Cursor::new(bytes), 0).expect("reopen");
        let root2 = fs2.root().unwrap();
        let kids = fs2.list_directory(&root2).expect("list");
        assert!(
            !kids.iter().any(|e| e.name == "old.txt"),
            "old name should be gone"
        );
        let renamed = kids
            .iter()
            .find(|e| e.name == "new.txt")
            .expect("new name present");

        // Identity (inode number) and content preserved.
        assert_eq!(renamed.location, original_inum);
        let data = fs2.read_file(renamed, usize::MAX).expect("read");
        assert_eq!(data, payload);
    }

    #[test]
    fn editable_create_directory_round_trip() {
        use super::super::filesystem::{
            CreateDirectoryOptions, CreateFileOptions, EditableFilesystem, Filesystem,
        };
        let img = build_synthetic_with_root_dir();
        let mut fs = EfsFilesystem::open(Cursor::new(img), 0).expect("open");
        fs.sb.tfree = 200;
        fs.sb.tinode = 8;

        let root = fs.root().expect("root");
        let dir_entry = fs
            .create_directory(&root, "subdir", &CreateDirectoryOptions::default())
            .expect("mkdir");
        assert!(dir_entry.is_directory());

        // Create a file inside the new dir.
        let payload = b"nested".to_vec();
        let mut cur = Cursor::new(payload.clone());
        fs.create_file(
            &dir_entry,
            "inner.txt",
            &mut cur,
            payload.len() as u64,
            &CreateFileOptions::default(),
        )
        .expect("create inside subdir");
        fs.sync_metadata().expect("sync");

        // Re-open + verify path.
        let bytes = fs.reader.into_inner();
        let mut fs2 = EfsFilesystem::open(Cursor::new(bytes), 0).expect("reopen");
        let root2 = fs2.root().unwrap();
        let kids = fs2.list_directory(&root2).unwrap();
        let sub = kids.iter().find(|e| e.name == "subdir").expect("subdir");
        assert!(sub.is_directory());
        let sub_kids = fs2.list_directory(sub).expect("list subdir");
        assert!(sub_kids.iter().any(|e| e.name == "inner.txt"));
    }

    #[test]
    fn editable_delete_entry_frees_inode_and_blocks() {
        use super::super::filesystem::{CreateFileOptions, EditableFilesystem, Filesystem};
        let img = build_synthetic_with_root_dir();
        let mut fs = EfsFilesystem::open(Cursor::new(img), 0).expect("open");
        fs.sb.tfree = 200;
        fs.sb.tinode = 8;

        let root = fs.root().expect("root");
        let payload = vec![0xCD; 1500];
        let mut cur = Cursor::new(payload.clone());
        let entry = fs
            .create_file(
                &root,
                "tmp.bin",
                &mut cur,
                payload.len() as u64,
                &CreateFileOptions::default(),
            )
            .expect("create");

        let tinode_after_create = fs.sb.tinode;
        fs.delete_entry(&root, &entry).expect("delete");
        assert_eq!(fs.sb.tinode, tinode_after_create + 1, "tinode bumped back");

        // The dirent is gone after sync.
        fs.sync_metadata().expect("sync");
        let bytes = fs.reader.into_inner();
        let mut fs2 = EfsFilesystem::open(Cursor::new(bytes), 0).expect("reopen");
        let root2 = fs2.root().unwrap();
        let kids = fs2.list_directory(&root2).expect("list");
        assert!(
            !kids.iter().any(|e| e.name == "tmp.bin"),
            "deleted file still present: {kids:?}"
        );
    }

    #[test]
    fn repair_fixes_missing_bitmap_bit() {
        use super::super::filesystem::{CreateFileOptions, EditableFilesystem, Filesystem};
        let img = build_synthetic_with_root_dir();
        let mut fs = EfsFilesystem::open(Cursor::new(img), 0).expect("open");
        fs.sb.tfree = 200;
        fs.sb.tinode = 8;
        let root = fs.root().expect("root");
        let payload = vec![0xAA; 512];
        let mut cur = Cursor::new(payload.clone());
        let entry = fs
            .create_file(
                &root,
                "f.bin",
                &mut cur,
                payload.len() as u64,
                &CreateFileOptions::default(),
            )
            .expect("create");
        EditableFilesystem::sync_metadata(&mut fs).expect("sync");

        // Reach into the bitmap and SET the bit for the file's data
        // block (set bit = free), forging a "free but inode-claimed"
        // corruption — fsck should flag it as BitmapMissingAllocation.
        let file_ino = fs.read_inode(entry.location as u32).expect("ino");
        let data_blk = file_ino.extents[0].bn;
        let mut bm = fs.read_bitmap().expect("bm");
        let by = (data_blk / 8) as usize;
        let bb = 7 - (data_blk % 8);
        bm[by] |= 1u8 << bb;
        fs.write_bitmap(&bm).expect("write bm");

        let before = super::super::efs_fsck::fsck_efs(&mut fs).expect("fsck");
        assert!(
            before
                .errors
                .iter()
                .any(|e| e.code == "BitmapMissingAllocation"),
            "expected BitmapMissingAllocation"
        );

        let report = fs.repair().expect("repair");
        assert!(
            report.fixes_applied.iter().any(|s| s.contains("Bitmap")),
            "expected bitmap fix, got: {:?}",
            report
        );
        let after = super::super::efs_fsck::fsck_efs(&mut fs).expect("fsck after");
        assert!(
            !after
                .errors
                .iter()
                .any(|e| e.code == "BitmapMissingAllocation"),
            "BitmapMissingAllocation still present after repair: {:?}",
            after.errors.iter().map(|e| &e.code).collect::<Vec<_>>()
        );
    }

    #[test]
    fn repair_adopts_orphans_into_lost_found() {
        use super::super::filesystem::{CreateFileOptions, EditableFilesystem, Filesystem};
        let img = build_synthetic_with_root_dir();
        let mut fs = EfsFilesystem::open(Cursor::new(img), 0).expect("open");
        fs.sb.tfree = 200;
        fs.sb.tinode = 8;
        // Create a file (linked into root). Then manually unlink it
        // from the parent directory without freeing the inode —
        // simulating fsck-style corruption where the dirent was lost
        // but the inode body and data blocks survived.
        let root = fs.root().expect("root");
        let payload = vec![0xEE; 100];
        let mut cur = Cursor::new(payload.clone());
        let orphan_entry = fs
            .create_file(
                &root,
                "orphan",
                &mut cur,
                payload.len() as u64,
                &CreateFileOptions::default(),
            )
            .expect("create");
        let orphan_inum = orphan_entry.location as u32;
        let root_inode = fs.read_inode(2).expect("root ino");
        fs.dir_remove(&root_inode, b"orphan").expect("unlink");
        EditableFilesystem::sync_metadata(&mut fs).expect("sync");

        // Verify the inode is orphaned: fsck reports OrphanInode.
        let before = super::super::efs_fsck::fsck_efs(&mut fs).expect("fsck");
        assert!(
            before.errors.iter().any(|e| e.code == "OrphanInode"),
            "expected OrphanInode, got: {:?}",
            before.errors.iter().map(|e| &e.code).collect::<Vec<_>>()
        );
        assert_eq!(before.orphaned_entries.len(), 1);
        assert_eq!(before.orphaned_entries[0].id, orphan_inum as u64);

        let report = fs.repair().expect("repair");
        assert!(
            report.fixes_applied.iter().any(|s| s.contains("Orphans")),
            "expected orphan adoption, got: {:?}",
            report
        );

        // After repair: lost+found exists under root and contains the orphan.
        let root2 = fs.read_inode(2).expect("root2");
        let lf_inum = fs
            .dir_find(&root2, b"lost+found")
            .expect("find lf")
            .expect("lf present");
        let lf_inode = fs.read_inode(lf_inum).expect("lf ino");
        let adopted = fs
            .dir_find(&lf_inode, format!("ino_{orphan_inum}").as_bytes())
            .expect("find orphan")
            .expect("adopted present");
        assert_eq!(adopted, orphan_inum);
    }

    #[test]
    fn parse_then_serialize_dir_block_round_trips() {
        let entries = vec![
            DirEntry {
                inum: 2,
                name: b".".to_vec(),
            },
            DirEntry {
                inum: 2,
                name: b"..".to_vec(),
            },
            DirEntry {
                inum: 17,
                name: b"hello".to_vec(),
            },
        ];
        let block = serialize_dir_block(&entries).expect("serialize");
        let parsed = parse_dir_block(&block);
        assert_eq!(parsed.len(), 3);
        assert_eq!(parsed[0].name, b".");
        assert_eq!(parsed[1].name, b"..");
        assert_eq!(parsed[2].name, b"hello");
        assert_eq!(parsed[2].inum, 17);
    }

    #[test]
    fn write_superblock_pair_updates_primary_and_replica() {
        let img = build_synthetic_for_bitmap_tests();
        let mut fs = EfsFilesystem::open(Cursor::new(img), 0).expect("open");
        // Bump tfree to a distinctive sentinel and write.
        fs.sb.tfree = 0xDEAD_BEEF;
        fs.write_superblock_pair().expect("write sb pair");

        // Re-read primary by opening again from the now-mutated buffer.
        let bytes = fs.reader.into_inner();
        let fs2 = EfsFilesystem::open(Cursor::new(bytes.clone()), 0).expect("reopen");
        assert_eq!(fs2.sb.tfree, 0xDEAD_BEEF);

        // Replica must also carry the new tfree.
        let replica_off = fs2.sb.replsb as usize * 512;
        let replica =
            EfsSuperblock::parse(&bytes[replica_off..replica_off + 92]).expect("parse replica");
        assert_eq!(replica.tfree, 0xDEAD_BEEF);
        assert_eq!(replica.fs_size, fs2.sb.fs_size);
    }
}
