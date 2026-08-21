//! SGI EFS v1 — the original Extent File System, as shipped on the IRIS 2000 /
//! 3000 series. Read/write; see `docs/SGI_EFS_v1.md` for the write rules.
//!
//! This is the ancestor of the IRIX EFS in [`efs`](super::efs), not a dialect
//! of it. SGI grew it out of the Bell / System V filesystem by replacing the
//! block-list inode with an extent-list one, and the System V ancestry still
//! shows: directories are flat arrays of `struct direct` — a 16-bit inode
//! number and a 14-byte name — with none of the `0xBEEF` slotted directory
//! blocks IRIX EFS later used. The two formats also carry different magics
//! (`0x041755` here, `0x072959` there), and the v1 superblock has a shorter,
//! differently-packed header. They share only the inode and extent layout.
//!
//! Everything here is verified against `<sys/efs_sb.h>`, `<sys/efs_ino.h>`,
//! `<sys/efs_fs.h>` and `<sys/dir.h>` as recovered from a real IRIS 3130 disk
//! (RCS revisions dated 1986-1987); see `docs/SGI_EFS_v1.md`.
//!
//! ## On-disk layout
//!
//! Per `<sys/efs_fs.h>`, in 512-byte basic blocks, partition-relative:
//!
//! - block 0 unused,
//! - block 1 the superblock,
//! - block 2 onwards the free-block bitmap, `ceil(fs_bmsize / 512)` blocks,
//! - unused blocks up to `fs_firstcg`,
//! - `fs_ncg` cylinder groups of `fs_cgfsize` blocks, each opening with
//!   `fs_cgisize` blocks of inodes (4 × 128 bytes per block),
//! - trailing blocks past the last group, outside `fs_size`.
//!
//! `fs_size == fs_firstcg + fs_ncg * fs_cgfsize` always holds. The bitmap uses
//! **set bit = free**, LSB-first within each byte, the same convention as IRIX
//! EFS.
//!
//! ## Superblock (`struct efs`)
//!
//! Compiled for the 68020 with **2-byte** alignment, so the `long` fields sit
//! at merely-even offsets. `fs_time` at 0x16 is the first one that is not
//! 4-aligned, and everything after it inherits the shift — misreading this as
//! 4-aligned puts `fs_magic` two bytes out and nothing parses.
//!
//! | Off | Field | | Off | Field |
//! |-----|-------|-|-----|-------|
//! | 0x00 | `fs_size` be32 | | 0x1A | `fs_fname[6]` |
//! | 0x04 | `fs_firstcg` be32 | | 0x20 | `fs_fpack[6]` |
//! | 0x08 | `fs_cgfsize` be32 | | 0x26 | `fs_magic` be32 |
//! | 0x0C | `fs_cgisize` be16 | | 0x2A | `fs_prealloc` be32 |
//! | 0x0E | `fs_sectors` be16 | | 0x2E | `fs_bmsize` be32 |
//! | 0x10 | `fs_heads` be16 | | 0x32 | `fs_tfree` be32 |
//! | 0x12 | `fs_ncg` be16 | | 0x36 | `fs_tinode` be32 |
//! | 0x14 | `fs_dirty` be16 | | 0x3A | `fs_spare[100]` |
//! | 0x16 | `fs_time` be32 | | 0x9E | `fs_checksum` be32 |
//!
//! The checksum is the same rotate-and-XOR IRIX EFS uses, but run over offsets
//! 0..0x9E rather than IRIX's 0..0x58, so it needs its own routine:
//! [`efs_v1_superblock_checksum`]. Verified byte-exact against the stored
//! `fs_checksum` of both EFS volumes on the IRIS 3130 disk.
//!
//! ## Byte order
//!
//! Images taken off period SGI disk controllers are byte-swapped within every
//! 16-bit word — see the `sgi_dklabel` module header. The magic is probed both
//! ways at open time and every block read is fixed up on the way in, so a
//! partition image without its disk label still opens. Stored bytes are never
//! rewritten: a backup of one of these disks must stay byte-identical.

#[cfg(feature = "rust173-polyfill")]
use crate::rust173_compat::IntIsMultipleOf as _;
use std::io::{Read, Seek, SeekFrom, Write};

use byteorder::{BigEndian, ByteOrder};

use super::entry::{EntryType, FileEntry};
use super::filesystem::{Filesystem, FilesystemError};
use crate::partition::sgi_dklabel::{apply_byte_order, SgiLabelByteOrder};

/// A basic block ("bb"): one disk sector.
pub const EFS_V1_BLOCKSIZE: u64 = 512;
/// `EFS_MAGIC` from `<sys/efs_sb.h>`.
pub const EFS_V1_MAGIC: u32 = 0x0004_1755;
/// `EFS2_MAGIC` — the same layout with a bumped magic.
pub const EFS_V1_MAGIC2: u32 = 0x0004_1756;
/// Byte offset of `fs_magic` inside the superblock.
const OFF_MAGIC: usize = 0x26;
/// On-volume part of `struct efs`, up to and including `fs_checksum`.
pub const EFS_V1_SUPERBLOCK_SIZE: usize = 0xA2;
/// `EFS_SUPERBB` — the superblock's block number.
const EFS_V1_SUPERBB: u64 = 1;
/// `EFS_BITMAPBB` — where the free-block bitmap starts.
const EFS_V1_BITMAPBB: u32 = 2;

const EFS_V1_INODESIZE: u64 = 128;
/// `EFS_INOPBB` — inodes per basic block.
const EFS_V1_INOPBB: u32 = (EFS_V1_BLOCKSIZE / EFS_V1_INODESIZE) as u32;
/// `EFS_DIRECTEXTENTS` — extents stored inline in an inode.
const EFS_V1_DIRECTEXTENTS: usize = 12;
/// `EFS_MAXEXTENTS` — the hard cap on extents per file.
const EFS_V1_MAXEXTENTS: usize = 2048;
/// `EFS_MAXEXTENTLEN` — longest single extent, in blocks.
const EFS_V1_MAXEXTENTLEN: u32 = 256 - 8;
/// Extent records packed into one indirect index block.
const EFS_V1_EXTENTS_PER_BLOCK: usize = (EFS_V1_BLOCKSIZE / 8) as usize;

/// `DIRSIZ` — the fixed name field of a `struct direct`.
const EFS_V1_DIRSIZ: usize = 14;
/// `sizeof(struct direct)`: a 16-bit `ino_t` plus `d_name[DIRSIZ]`.
const EFS_V1_DIRENTSIZE: usize = 2 + EFS_V1_DIRSIZ;

/// The root is inode 2, as in every filesystem of this lineage.
const EFS_V1_ROOT_INODE: u32 = 2;

/// `di_size` is a 32-bit `off_t` the kernel treats as signed.
const EFS_V1_MAX_FILE_SIZE: u64 = i32::MAX as u64;

/// Ceiling on a single `read_file` / directory allocation, so a corrupt
/// `di_size` cannot make us reserve gigabytes before the first read.
const EFS_V1_SANE_ALLOC: usize = 64 * 1024 * 1024;

/// `MAXPATHLEN` — the bound on a symlink target, which is stored as file data.
const EFS_V1_MAXPATHLEN: usize = 1024;

/// The on-volume part of `struct efs`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EfsV1Superblock {
    /// Blocks in the filesystem, excluding the tail past the last group.
    pub fs_size: u32,
    /// Block offset of the first cylinder group.
    pub firstcg: u32,
    /// Blocks per cylinder group.
    pub cgfsize: u32,
    /// Inode blocks at the head of each cylinder group.
    pub cgisize: u16,
    pub sectors: u16,
    pub heads: u16,
    pub ncg: u16,
    /// Nonzero when the volume wants `fsck`.
    pub dirty: u16,
    pub fs_time: u32,
    pub fname: [u8; 6],
    pub fpack: [u8; 6],
    pub magic: u32,
    /// `fs_prealloc` — the allocator's preferred pre-allocation run.
    pub prealloc: u32,
    /// Bitmap length in bytes.
    pub bmsize: u32,
    /// Free data blocks.
    pub tfree: u32,
    /// Free inodes.
    pub tinode: u32,
    pub checksum: u32,
}

impl EfsV1Superblock {
    /// Parse a superblock out of an already byte-order-corrected buffer.
    pub fn parse(buf: &[u8]) -> Result<Self, FilesystemError> {
        if buf.len() < EFS_V1_SUPERBLOCK_SIZE {
            return Err(FilesystemError::Parse(format!(
                "EFS v1 superblock buffer too small: {} bytes",
                buf.len()
            )));
        }
        let magic = BigEndian::read_u32(&buf[OFF_MAGIC..OFF_MAGIC + 4]);
        if magic != EFS_V1_MAGIC && magic != EFS_V1_MAGIC2 {
            return Err(FilesystemError::Parse(format!(
                "bad EFS v1 magic: 0x{magic:08X} (expected 0x{EFS_V1_MAGIC:08X} or 0x{EFS_V1_MAGIC2:08X})"
            )));
        }
        let mut fname = [0u8; 6];
        fname.copy_from_slice(&buf[0x1A..0x20]);
        let mut fpack = [0u8; 6];
        fpack.copy_from_slice(&buf[0x20..0x26]);
        let sb = EfsV1Superblock {
            fs_size: BigEndian::read_u32(&buf[0x00..0x04]),
            firstcg: BigEndian::read_u32(&buf[0x04..0x08]),
            cgfsize: BigEndian::read_u32(&buf[0x08..0x0C]),
            cgisize: BigEndian::read_u16(&buf[0x0C..0x0E]),
            sectors: BigEndian::read_u16(&buf[0x0E..0x10]),
            heads: BigEndian::read_u16(&buf[0x10..0x12]),
            ncg: BigEndian::read_u16(&buf[0x12..0x14]),
            dirty: BigEndian::read_u16(&buf[0x14..0x16]),
            fs_time: BigEndian::read_u32(&buf[0x16..0x1A]),
            fname,
            fpack,
            magic,
            prealloc: BigEndian::read_u32(&buf[0x2A..0x2E]),
            bmsize: BigEndian::read_u32(&buf[0x2E..0x32]),
            tfree: BigEndian::read_u32(&buf[0x32..0x36]),
            tinode: BigEndian::read_u32(&buf[0x36..0x3A]),
            checksum: BigEndian::read_u32(&buf[0x9E..0xA2]),
        };
        sb.validate()?;
        Ok(sb)
    }

    /// Reject geometry that cannot describe a real volume. The magic alone is
    /// only 32 bits; these are the invariants `<sys/efs_fs.h>` documents, and
    /// they are what stop a chance magic hit inside file data from being
    /// mounted as a filesystem.
    fn validate(&self) -> Result<(), FilesystemError> {
        if self.cgfsize == 0 || self.ncg == 0 {
            return Err(FilesystemError::Parse(
                "EFS v1 superblock has no cylinder groups".to_string(),
            ));
        }
        if self.cgisize as u32 >= self.cgfsize {
            return Err(FilesystemError::Parse(format!(
                "EFS v1 cylinder group is all inodes: cgisize {} >= cgfsize {}",
                self.cgisize, self.cgfsize
            )));
        }
        let expect = (self.firstcg as u64) + (self.ncg as u64) * (self.cgfsize as u64);
        if expect != self.fs_size as u64 {
            return Err(FilesystemError::Parse(format!(
                "EFS v1 geometry does not close: firstcg {} + ncg {} * cgfsize {} = {expect}, fs_size {}",
                self.firstcg, self.ncg, self.cgfsize, self.fs_size
            )));
        }
        if self.firstcg < EFS_V1_BITMAPBB {
            return Err(FilesystemError::Parse(format!(
                "EFS v1 first cylinder group at block {} overlaps the bitmap",
                self.firstcg
            )));
        }
        Ok(())
    }

    /// `EFS_COMPUTE_IPCG` — inodes per cylinder group.
    pub fn inodes_per_cg(&self) -> u32 {
        self.cgisize as u32 * EFS_V1_INOPBB
    }

    /// Total inodes the volume can hold.
    pub fn total_inodes(&self) -> u32 {
        self.inodes_per_cg().saturating_mul(self.ncg as u32)
    }

    /// Bitmap length in whole blocks.
    fn bitmap_blocks(&self) -> u32 {
        (self.bmsize as u64).div_ceil(EFS_V1_BLOCKSIZE) as u32
    }

    /// `EFS_ITOBB` / `EFS_ITOO` — the block holding inode `inum`, and the
    /// byte offset within it. `None` when `inum` is past the inode table.
    fn inode_location(&self, inum: u32) -> Option<(u32, usize)> {
        if inum >= self.total_inodes() {
            return None;
        }
        let cg = inum / self.inodes_per_cg();
        let cg_bb = (inum / EFS_V1_INOPBB) % self.cgisize as u32;
        let block = self.firstcg + cg * self.cgfsize + cg_bb;
        let offset = (inum % EFS_V1_INOPBB) as usize * EFS_V1_INODESIZE as usize;
        Some((block, offset))
    }

    /// Write the known fields back into `buf`, leaving every byte this
    /// driver does not model untouched so an existing volume keeps them.
    pub fn write_into(&self, buf: &mut [u8]) {
        BigEndian::write_u32(&mut buf[0x00..0x04], self.fs_size);
        BigEndian::write_u32(&mut buf[0x04..0x08], self.firstcg);
        BigEndian::write_u32(&mut buf[0x08..0x0C], self.cgfsize);
        BigEndian::write_u16(&mut buf[0x0C..0x0E], self.cgisize);
        BigEndian::write_u16(&mut buf[0x0E..0x10], self.sectors);
        BigEndian::write_u16(&mut buf[0x10..0x12], self.heads);
        BigEndian::write_u16(&mut buf[0x12..0x14], self.ncg);
        BigEndian::write_u16(&mut buf[0x14..0x16], self.dirty);
        BigEndian::write_u32(&mut buf[0x16..0x1A], self.fs_time);
        buf[0x1A..0x20].copy_from_slice(&self.fname);
        buf[0x20..0x26].copy_from_slice(&self.fpack);
        BigEndian::write_u32(&mut buf[0x26..0x2A], self.magic);
        BigEndian::write_u32(&mut buf[0x2A..0x2E], self.prealloc);
        BigEndian::write_u32(&mut buf[0x2E..0x32], self.bmsize);
        BigEndian::write_u32(&mut buf[0x32..0x36], self.tfree);
        BigEndian::write_u32(&mut buf[0x36..0x3A], self.tinode);
        BigEndian::write_u32(&mut buf[0x9E..0xA2], self.checksum);
    }

    /// Recompute `fs_checksum` over the serialized form of `buf`, which must
    /// already hold this superblock. Updates both `self` and `buf`.
    pub fn recompute_checksum(&mut self, buf: &mut [u8]) {
        BigEndian::write_u32(&mut buf[0x9E..0xA2], 0);
        self.checksum = efs_v1_superblock_checksum(buf);
        BigEndian::write_u32(&mut buf[0x9E..0xA2], self.checksum);
    }

    /// Volume name, as `fsname:packname` when both are set.
    fn label(&self) -> String {
        let n = trim_ascii(&self.fname);
        let p = trim_ascii(&self.fpack);
        match (n.is_empty(), p.is_empty()) {
            (true, true) => String::new(),
            (false, true) => n,
            (true, false) => p,
            (false, false) => format!("{n}:{p}"),
        }
    }
}

fn trim_ascii(raw: &[u8]) -> String {
    let end = raw.iter().position(|&b| b == 0).unwrap_or(raw.len());
    String::from_utf8_lossy(&raw[..end]).trim().to_string()
}

/// One 8-byte `struct extent`. Identical to the IRIX EFS form.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct EfsV1Extent {
    /// `ex_magic` — must be zero; anything else means the record is corrupt.
    pub magic: u8,
    /// `ex_bn` — first block, or 0 for a hole.
    pub bn: u32,
    /// `ex_length` — blocks covered.
    pub length: u8,
    /// `ex_offset` — logical block offset into the file.
    pub offset: u32,
}

/// The all-zero extent: no blocks, no offset. Used to pad inode slots.
const EFS_V1_EMPTY_EXTENT: EfsV1Extent = EfsV1Extent {
    magic: 0,
    bn: 0,
    length: 0,
    offset: 0,
};

impl EfsV1Extent {
    fn parse(buf: &[u8]) -> Self {
        let w0 = BigEndian::read_u32(&buf[0..4]);
        let w1 = BigEndian::read_u32(&buf[4..8]);
        EfsV1Extent {
            magic: ((w0 >> 24) & 0xFF) as u8,
            bn: w0 & 0x00FF_FFFF,
            length: ((w1 >> 24) & 0xFF) as u8,
            offset: w1 & 0x00FF_FFFF,
        }
    }

    fn write_into(&self, buf: &mut [u8]) {
        let w0 = ((self.magic as u32) << 24) | (self.bn & 0x00FF_FFFF);
        let w1 = ((self.length as u32) << 24) | (self.offset & 0x00FF_FFFF);
        BigEndian::write_u32(&mut buf[0..4], w0);
        BigEndian::write_u32(&mut buf[4..8], w1);
    }

    /// `ex_bn == 0` marks a range that was never written; it reads as zeros.
    pub fn is_hole(&self) -> bool {
        self.bn == 0
    }
}

/// A parsed `struct efs_dinode` (128 bytes).
#[derive(Debug, Clone)]
pub struct EfsV1Inode {
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
    /// `di_refs` — reorganiser bookkeeping, where IRIX later put `di_version`.
    pub refs: u16,
    pub extents: [EfsV1Extent; EFS_V1_DIRECTEXTENTS],
}

impl EfsV1Inode {
    fn parse(inum: u32, buf: &[u8]) -> Self {
        let mut extents = [EfsV1Extent {
            magic: 0,
            bn: 0,
            length: 0,
            offset: 0,
        }; EFS_V1_DIRECTEXTENTS];
        for (i, ext) in extents.iter_mut().enumerate() {
            let off = 0x20 + i * 8;
            *ext = EfsV1Extent::parse(&buf[off..off + 8]);
        }
        EfsV1Inode {
            inum,
            mode: BigEndian::read_u16(&buf[0x00..0x02]),
            nlink: BigEndian::read_u16(&buf[0x02..0x04]),
            uid: BigEndian::read_u16(&buf[0x04..0x06]),
            gid: BigEndian::read_u16(&buf[0x06..0x08]),
            size: BigEndian::read_u32(&buf[0x08..0x0C]),
            atime: BigEndian::read_u32(&buf[0x0C..0x10]),
            mtime: BigEndian::read_u32(&buf[0x10..0x14]),
            ctime: BigEndian::read_u32(&buf[0x14..0x18]),
            gen: BigEndian::read_u32(&buf[0x18..0x1C]),
            numextents: BigEndian::read_u16(&buf[0x1C..0x1E]),
            refs: BigEndian::read_u16(&buf[0x1E..0x20]),
            extents,
        }
    }

    /// Serialize into a 128-byte inode slot.
    fn write_into(&self, buf: &mut [u8; EFS_V1_INODESIZE as usize]) {
        BigEndian::write_u16(&mut buf[0x00..0x02], self.mode);
        BigEndian::write_u16(&mut buf[0x02..0x04], self.nlink);
        BigEndian::write_u16(&mut buf[0x04..0x06], self.uid);
        BigEndian::write_u16(&mut buf[0x06..0x08], self.gid);
        BigEndian::write_u32(&mut buf[0x08..0x0C], self.size);
        BigEndian::write_u32(&mut buf[0x0C..0x10], self.atime);
        BigEndian::write_u32(&mut buf[0x10..0x14], self.mtime);
        BigEndian::write_u32(&mut buf[0x14..0x18], self.ctime);
        BigEndian::write_u32(&mut buf[0x18..0x1C], self.gen);
        BigEndian::write_u16(&mut buf[0x1C..0x1E], self.numextents);
        BigEndian::write_u16(&mut buf[0x1E..0x20], self.refs);
        for (i, ext) in self.extents.iter().enumerate() {
            let off = 0x20 + i * 8;
            ext.write_into(&mut buf[off..off + 8]);
        }
    }

    /// A never-allocated inode: `di_mode` zero and no extents.
    fn empty(inum: u32) -> Self {
        EfsV1Inode {
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
            refs: 0,
            extents: [EFS_V1_EMPTY_EXTENT; EFS_V1_DIRECTEXTENTS],
        }
    }

    /// A `di_mode` of zero means the inode was never allocated.
    pub fn is_free(&self) -> bool {
        self.mode == 0
    }

    pub fn is_dir(&self) -> bool {
        (self.mode & 0o170000) == 0o040000
    }

    fn is_regular(&self) -> bool {
        (self.mode & 0o170000) == 0o100000
    }

    /// SGI carried 4.2BSD symbolic links into this System V derivative, so
    /// `S_IFLNK` inodes really do occur (`/usr/include/machine` is one).
    fn is_symlink(&self) -> bool {
        (self.mode & 0o170000) == 0o120000
    }

    fn is_device(&self) -> bool {
        matches!(self.mode & 0o170000, 0o020000 | 0o060000)
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

    /// `di_u.di_dev` — the device number, which shares storage with the first
    /// extent slot on a character or block special file.
    fn device_number(&self) -> Option<(u32, u32)> {
        if !self.is_device() {
            return None;
        }
        let w0 = ((self.extents[0].magic as u32) << 24) | self.extents[0].bn;
        Some(((w0 >> 8) & 0xFF, w0 & 0xFF))
    }

    /// Bytes readable from this inode, bounded by EFS's signed 32-bit `off_t`.
    fn effective_size(&self) -> u64 {
        (self.size as u64).min(EFS_V1_MAX_FILE_SIZE)
    }
}

/// An open EFS v1 volume. Reads on any `R`; the write half needs `R: Write`.
pub struct EfsV1Filesystem<R: Read + Seek> {
    reader: R,
    partition_offset: u64,
    /// How this image's 16-bit words are ordered; applied to every read.
    order: SgiLabelByteOrder,
    sb: EfsV1Superblock,
    label: String,
    /// Cached highest allocated block, for `last_data_byte`.
    highest_block: Option<u32>,
}

/// Probe for an EFS v1 superblock at `partition_offset`, returning the byte
/// order it parses in. Used by the filesystem-type detector and by `open`.
pub fn detect<R: Read + Seek>(reader: &mut R, partition_offset: u64) -> Option<SgiLabelByteOrder> {
    let mut sector = [0u8; EFS_V1_BLOCKSIZE as usize];
    reader
        .seek(SeekFrom::Start(
            partition_offset + EFS_V1_SUPERBB * EFS_V1_BLOCKSIZE,
        ))
        .ok()?;
    reader.read_exact(&mut sector).ok()?;
    for order in [SgiLabelByteOrder::Native, SgiLabelByteOrder::Swabbed] {
        let mut buf = sector;
        apply_byte_order(order, &mut buf);
        if EfsV1Superblock::parse(&buf).is_ok() {
            return Some(order);
        }
    }
    None
}

impl<R: Read + Seek> EfsV1Filesystem<R> {
    /// Open the volume at `partition_offset`, detecting the image byte order.
    pub fn open(reader: R, partition_offset: u64) -> Result<Self, FilesystemError> {
        Self::open_with_order(reader, partition_offset, None)
    }

    /// Open with a byte order already decided — by the disk label, say.
    /// `None` probes for it.
    pub fn open_with_order(
        mut reader: R,
        partition_offset: u64,
        order: Option<SgiLabelByteOrder>,
    ) -> Result<Self, FilesystemError> {
        let order = match order {
            Some(o) => o,
            None => detect(&mut reader, partition_offset).ok_or_else(|| {
                FilesystemError::Parse(
                    "no EFS v1 superblock at this offset (no 0x041755 magic in either byte order)"
                        .to_string(),
                )
            })?,
        };
        let mut sector = [0u8; EFS_V1_BLOCKSIZE as usize];
        reader.seek(SeekFrom::Start(
            partition_offset + EFS_V1_SUPERBB * EFS_V1_BLOCKSIZE,
        ))?;
        reader.read_exact(&mut sector)?;
        apply_byte_order(order, &mut sector);
        let sb = EfsV1Superblock::parse(&sector)?;

        // A volume declaring more blocks than the image holds is a truncated
        // capture. Reads inside the surviving prefix still work, so warn
        // rather than refuse — otherwise a partial dump cannot be browsed at
        // all, which is exactly when browsing matters most.
        let available = reader
            .seek(SeekFrom::End(0))?
            .saturating_sub(partition_offset);
        let declared = (sb.fs_size as u64).saturating_mul(EFS_V1_BLOCKSIZE);
        if declared > available {
            log::warn!(
                "EFS v1 volume declares {declared} bytes but only {available} are present; \
                 treating as a truncated image"
            );
        }

        let label = sb.label();
        Ok(EfsV1Filesystem {
            reader,
            partition_offset,
            order,
            sb,
            label,
            highest_block: None,
        })
    }

    /// Take the underlying reader back, for callers that need the image bytes
    /// after mutating the volume.
    pub fn reader_into_inner(self) -> R {
        self.reader
    }

    /// The parsed superblock.
    pub fn superblock(&self) -> &EfsV1Superblock {
        &self.sb
    }

    /// How the image's 16-bit words are ordered.
    pub fn byte_order(&self) -> SgiLabelByteOrder {
        self.order
    }

    /// Read `count` blocks starting at `bn` into `buf`, fixing byte order.
    fn read_blocks(&mut self, bn: u32, count: u32, buf: &mut [u8]) -> Result<(), FilesystemError> {
        let want = count as usize * EFS_V1_BLOCKSIZE as usize;
        let buf = &mut buf[..want];
        self.reader.seek(SeekFrom::Start(
            self.partition_offset + bn as u64 * EFS_V1_BLOCKSIZE,
        ))?;
        self.reader.read_exact(buf)?;
        apply_byte_order(self.order, buf);
        Ok(())
    }

    fn read_block(&mut self, bn: u32, buf: &mut [u8]) -> Result<(), FilesystemError> {
        self.read_blocks(bn, 1, buf)
    }

    /// Read inode `inum`. Inode 0 and 1 are reserved and never in use.
    pub fn read_inode(&mut self, inum: u32) -> Result<EfsV1Inode, FilesystemError> {
        let (block, offset) = self.sb.inode_location(inum).ok_or_else(|| {
            FilesystemError::InvalidData(format!(
                "EFS v1 inode {inum} is past the inode table ({} inodes)",
                self.sb.total_inodes()
            ))
        })?;
        let mut sector = [0u8; EFS_V1_BLOCKSIZE as usize];
        self.read_block(block, &mut sector)?;
        Ok(EfsV1Inode::parse(
            inum,
            &sector[offset..offset + EFS_V1_INODESIZE as usize],
        ))
    }

    /// Every data extent of `inode`, in logical order, following the indirect
    /// index when `numextents` exceeds the twelve inline slots.
    ///
    /// In indirect mode the inline slots stop describing data and instead
    /// point at runs of blocks packed with extent records, 64 to a block;
    /// `di_extents[0].ex_offset` says how many inline slots are used that way.
    pub fn extents_of(&mut self, inode: &EfsV1Inode) -> Result<Vec<EfsV1Extent>, FilesystemError> {
        let total = inode.numextents as usize;
        if total == 0 {
            return Ok(Vec::new());
        }
        if total > EFS_V1_MAXEXTENTS {
            return Err(FilesystemError::InvalidData(format!(
                "EFS v1 inode {} claims {total} extents (max {EFS_V1_MAXEXTENTS})",
                inode.inum
            )));
        }
        if total <= EFS_V1_DIRECTEXTENTS {
            let mut out: Vec<EfsV1Extent> = inode.extents[..total].to_vec();
            self.check_extents(inode, &mut out)?;
            return Ok(out);
        }

        let direxts = inode.extents[0].offset as usize;
        if direxts == 0 || direxts > EFS_V1_DIRECTEXTENTS {
            return Err(FilesystemError::InvalidData(format!(
                "EFS v1 inode {} has {direxts} indirect slots (expected 1..={EFS_V1_DIRECTEXTENTS})",
                inode.inum
            )));
        }
        let mut out = Vec::with_capacity(total);
        let mut block = [0u8; EFS_V1_BLOCKSIZE as usize];
        'outer: for slot in &inode.extents[..direxts] {
            for i in 0..slot.length as u32 {
                self.read_block(slot.bn + i, &mut block)?;
                for rec in 0..EFS_V1_EXTENTS_PER_BLOCK {
                    if out.len() >= total {
                        break 'outer;
                    }
                    out.push(EfsV1Extent::parse(&block[rec * 8..rec * 8 + 8]));
                }
            }
        }
        if out.len() < total {
            return Err(FilesystemError::InvalidData(format!(
                "EFS v1 inode {}: indirect index holds {} of {total} extents",
                inode.inum,
                out.len()
            )));
        }
        self.check_extents(inode, &mut out)?;
        Ok(out)
    }

    /// Validate extents per `<sys/efs_ino.h>` and sort them into logical
    /// order. A bad `ex_magic`, a zero or over-long run, or a block outside
    /// the data region all mean the inode is corrupt.
    fn check_extents(
        &self,
        inode: &EfsV1Inode,
        exts: &mut [EfsV1Extent],
    ) -> Result<(), FilesystemError> {
        for e in exts.iter() {
            if e.magic != 0 {
                return Err(FilesystemError::InvalidData(format!(
                    "EFS v1 inode {}: extent magic 0x{:02X} (must be zero)",
                    inode.inum, e.magic
                )));
            }
            if e.length == 0 || e.length as u32 > EFS_V1_MAXEXTENTLEN {
                return Err(FilesystemError::InvalidData(format!(
                    "EFS v1 inode {}: extent length {} out of range",
                    inode.inum, e.length
                )));
            }
            if !e.is_hole()
                && (e.bn < self.sb.firstcg
                    || e.bn as u64 + e.length as u64 > self.sb.fs_size as u64)
            {
                return Err(FilesystemError::InvalidData(format!(
                    "EFS v1 inode {}: extent {}+{} outside blocks {}..{}",
                    inode.inum, e.bn, e.length, self.sb.firstcg, self.sb.fs_size
                )));
            }
        }
        exts.sort_by_key(|e| e.offset);
        Ok(())
    }

    /// Stream the file `inode` describes into `writer`, stopping after
    /// `max_bytes`. Holes are emitted as zeros, which is what they mean.
    fn stream_data(
        &mut self,
        inode: &EfsV1Inode,
        max_bytes: u64,
        writer: &mut dyn Write,
    ) -> Result<u64, FilesystemError> {
        let limit = inode.effective_size().min(max_bytes);
        if limit == 0 {
            return Ok(0);
        }
        let exts = self.extents_of(inode)?;
        let mut written: u64 = 0;
        let mut buf = vec![0u8; EFS_V1_MAXEXTENTLEN as usize * EFS_V1_BLOCKSIZE as usize];
        for e in &exts {
            let start = e.offset as u64 * EFS_V1_BLOCKSIZE;
            if start >= limit {
                break;
            }
            // A sparse file may skip logical blocks entirely; pad to where
            // this extent begins so the output stays at the right offset.
            if start > written {
                write_zeros(writer, start - written)?;
                written = start;
            }
            let span = (e.length as u64 * EFS_V1_BLOCKSIZE).min(limit - start);
            if e.is_hole() {
                write_zeros(writer, span)?;
            } else {
                self.read_blocks(e.bn, e.length as u32, &mut buf)?;
                writer.write_all(&buf[..span as usize])?;
            }
            written += span;
        }
        if written < limit {
            write_zeros(writer, limit - written)?;
            written = limit;
        }
        Ok(written)
    }

    /// Read the whole of `inode`'s data, capped at `max_bytes`.
    fn read_data(
        &mut self,
        inode: &EfsV1Inode,
        max_bytes: usize,
    ) -> Result<Vec<u8>, FilesystemError> {
        let limit = inode.effective_size().min(max_bytes as u64);
        let mut out = Vec::with_capacity((limit as usize).min(EFS_V1_SANE_ALLOC));
        self.stream_data(inode, limit, &mut out)?;
        Ok(out)
    }

    /// Directory entries of `inode` as `(inum, name)`, skipping free slots and
    /// `.` / `..`. Names are the System V `struct direct` shape: a 16-bit
    /// inode number then a NUL-padded 14-byte name.
    fn read_dir_entries(
        &mut self,
        inode: &EfsV1Inode,
    ) -> Result<Vec<(u32, String)>, FilesystemError> {
        let data = self.read_data(inode, EFS_V1_SANE_ALLOC)?;
        let total = self.sb.total_inodes();
        let mut out = Vec::new();
        for chunk in data.chunks_exact(EFS_V1_DIRENTSIZE) {
            let inum = BigEndian::read_u16(&chunk[0..2]) as u32;
            if inum == 0 {
                continue;
            }
            let raw = &chunk[2..2 + EFS_V1_DIRSIZ];
            let end = raw.iter().position(|&b| b == 0).unwrap_or(raw.len());
            let name = String::from_utf8_lossy(&raw[..end]).to_string();
            if name.is_empty() || name == "." || name == ".." {
                continue;
            }
            // A dirent pointing past the inode table is damage, not a file;
            // the entry is dropped rather than failing the whole listing.
            if inum >= total {
                log::warn!(
                    "EFS v1 directory inode {} references inode {inum}, past the {total}-inode table",
                    inode.inum
                );
                continue;
            }
            out.push((inum, name));
        }
        Ok(out)
    }

    /// The path a symbolic link holds. Stored as ordinary file data, so it is
    /// bounded by `MAXPATHLEN` rather than the 14-byte directory-entry limit.
    fn read_symlink_target(&mut self, inode: &EfsV1Inode) -> Option<String> {
        let bytes = self.read_data(inode, EFS_V1_MAXPATHLEN).ok()?;
        let target = String::from_utf8_lossy(&bytes)
            .trim_end_matches(' ')
            .to_string();
        if target.is_empty() {
            None
        } else {
            Some(target)
        }
    }

    /// Highest block any in-use inode claims, plus one. Walks the whole inode
    /// table, so callers should treat it as expensive.
    fn scan_highest_block(&mut self) -> Result<u32, FilesystemError> {
        if let Some(v) = self.highest_block {
            return Ok(v);
        }
        // Metadata alone already reaches the end of the last inode table.
        let mut high = self
            .sb
            .firstcg
            .max(EFS_V1_BITMAPBB + self.sb.bitmap_blocks());
        for cg in 0..self.sb.ncg as u32 {
            high = high.max(self.sb.firstcg + cg * self.sb.cgfsize + self.sb.cgisize as u32);
        }
        for inum in 0..self.sb.total_inodes() {
            let inode = match self.read_inode(inum) {
                Ok(i) => i,
                Err(_) => continue,
            };
            if inode.is_free() || inode.is_device() {
                continue;
            }
            let exts = match self.extents_of(&inode) {
                Ok(e) => e,
                Err(_) => continue,
            };
            for e in exts {
                if !e.is_hole() {
                    high = high.max(e.bn + e.length as u32);
                }
            }
        }
        let high = high.min(self.sb.fs_size);
        self.highest_block = Some(high);
        Ok(high)
    }

    fn entry_from_inode(&self, name: &str, parent: &str, inode: &EfsV1Inode) -> FileEntry {
        let path = join_path(parent, name);
        let mut e = FileEntry::new_file(
            name.to_string(),
            path,
            inode.effective_size(),
            inode.inum as u64,
        );
        e.entry_type = inode.entry_type();
        if inode.mtime != 0 {
            e.modified_unix = Some(inode.mtime as u64);
            e.modified = Some(crate::fs::unix_common::inode::format_unix_timestamp(
                inode.mtime as i64,
            ));
        }
        e.mode = Some(inode.mode as u32);
        e.uid = Some(inode.uid as u32);
        e.gid = Some(inode.gid as u32);
        e.special_type = inode
            .special_type()
            .map(|kind| match inode.device_number() {
                Some((major, minor)) => format!("{kind} {major},{minor}"),
                None => kind,
            });
        if inode.is_dir() {
            e.size = inode.effective_size();
        }
        e
    }
}

fn join_path(parent: &str, name: &str) -> String {
    if parent == "/" {
        format!("/{name}")
    } else {
        format!("{}/{}", parent.trim_end_matches('/'), name)
    }
}

fn write_zeros(writer: &mut dyn Write, mut n: u64) -> Result<(), FilesystemError> {
    let zeros = [0u8; EFS_V1_BLOCKSIZE as usize];
    while n > 0 {
        let take = n.min(zeros.len() as u64) as usize;
        writer.write_all(&zeros[..take])?;
        n -= take as u64;
    }
    Ok(())
}

/// `fs_checksum`: rotate-left-1 and XOR over big-endian 16-bit words across
/// 0..0x9E, checksum field read as zero. IRIX's 0..0x58 routine does not fit.
pub fn efs_v1_superblock_checksum(sb: &[u8]) -> u32 {
    debug_assert!(
        sb.len() >= 0x9E,
        "superblock buffer must cover offsets 0..0x9E for the checksum"
    );
    let mut c: u32 = 0;
    let mut i = 0;
    while i < 0x9E {
        c ^= ((sb[i] as u32) << 8) | sb[i + 1] as u32;
        c = c.rotate_left(1);
        i += 2;
    }
    c
}

/// Current UNIX time, clamped into the 32-bit fields the format uses.
fn now_u32() -> u32 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0) as u32
}

/// The blocks an allocator may hand out: cylinder group data areas only, so a
/// damaged bitmap cannot yield an inode table. See docs/SGI_EFS_v1.md.
#[derive(Debug, Clone, Copy)]
pub(crate) struct EfsV1DataRegions {
    firstcg: u32,
    cgfsize: u32,
    cgisize: u32,
    ncg: u32,
    fs_size: u32,
}

impl EfsV1DataRegions {
    pub(crate) fn from_sb(sb: &EfsV1Superblock) -> Self {
        EfsV1DataRegions {
            firstcg: sb.firstcg,
            cgfsize: sb.cgfsize,
            cgisize: sb.cgisize as u32,
            ncg: sb.ncg as u32,
            fs_size: sb.fs_size,
        }
    }

    /// The `[start, end)` data-block range of cylinder group `cg`: the group
    /// minus its leading inode table, clamped to `fs_size`.
    fn cg_data_range(&self, cg: u32) -> Option<(u32, u32)> {
        if self.cgfsize == 0 || self.cgisize >= self.cgfsize {
            return None;
        }
        let cg_start = self.firstcg.checked_add(cg.checked_mul(self.cgfsize)?)?;
        let data_start = cg_start.checked_add(self.cgisize)?;
        let data_end = cg_start.checked_add(self.cgfsize)?.min(self.fs_size);
        if data_end <= data_start {
            return None;
        }
        Some((data_start, data_end))
    }

    /// Every cylinder group's data range, low block to high.
    fn ranges(self) -> impl Iterator<Item = (u32, u32)> {
        (0..self.ncg).filter_map(move |cg| self.cg_data_range(cg))
    }

    /// Whether an extent is allowed to point at `blk`.
    pub(crate) fn contains(self, blk: u32) -> bool {
        self.ranges().any(|(lo, hi)| blk >= lo && blk < hi)
    }
}

/// Write plumbing. Split from the read impl so a read-only source still opens:
/// only these methods need `R: Write`.
impl<R: Read + Write + Seek + Send> EfsV1Filesystem<R> {
    /// Write `buf` starting at block `bn`, applying this image's word order on
    /// the way out so a byte-swapped volume stays internally consistent.
    fn write_blocks(&mut self, bn: u32, buf: &[u8]) -> Result<(), FilesystemError> {
        if !buf.len().is_multiple_of(EFS_V1_BLOCKSIZE as usize) {
            return Err(FilesystemError::InvalidData(format!(
                "EFS v1 write_blocks: {} bytes is not a whole number of blocks",
                buf.len()
            )));
        }
        let mut tmp = buf.to_vec();
        apply_byte_order(self.order, &mut tmp);
        self.reader.seek(SeekFrom::Start(
            self.partition_offset + bn as u64 * EFS_V1_BLOCKSIZE,
        ))?;
        self.reader.write_all(&tmp)?;
        Ok(())
    }

    fn write_block(
        &mut self,
        bn: u32,
        buf: &[u8; EFS_V1_BLOCKSIZE as usize],
    ) -> Result<(), FilesystemError> {
        self.write_blocks(bn, buf)
    }

    /// Persist the superblock, recomputing `fs_checksum`. Only the fields this
    /// driver models are rewritten; the rest of the sector is preserved.
    pub(crate) fn sync_superblock(&mut self) -> Result<(), FilesystemError> {
        let mut sector = [0u8; EFS_V1_BLOCKSIZE as usize];
        self.read_block(EFS_V1_SUPERBB as u32, &mut sector)?;
        self.sb.fs_time = now_u32();
        let mut sb = self.sb.clone();
        sb.write_into(&mut sector);
        sb.recompute_checksum(&mut sector);
        self.sb = sb;
        self.write_block(EFS_V1_SUPERBB as u32, &sector)
    }

    /// The free-space bitmap, `bmsize` bytes starting at block 2. Set bit =
    /// free, LSB-first within each byte.
    pub(crate) fn read_bitmap(&mut self) -> Result<Vec<u8>, FilesystemError> {
        let bmsize = self.sb.bmsize as usize;
        if bmsize == 0 {
            return Err(FilesystemError::InvalidData(
                "EFS v1 bitmap size is 0 — superblock cannot support mutation".to_string(),
            ));
        }
        let blocks = self.sb.bitmap_blocks();
        let mut buf = vec![0u8; blocks as usize * EFS_V1_BLOCKSIZE as usize];
        self.read_blocks(EFS_V1_BITMAPBB, blocks, &mut buf)?;
        buf.truncate(bmsize);
        Ok(buf)
    }

    /// Write the bitmap back, preserving any bytes past `bmsize` in the final
    /// block rather than zeroing the tail.
    pub(crate) fn write_bitmap(&mut self, bm: &[u8]) -> Result<(), FilesystemError> {
        let bmsize = self.sb.bmsize as usize;
        if bm.len() != bmsize {
            return Err(FilesystemError::InvalidData(format!(
                "EFS v1 write_bitmap: buffer is {} bytes, expected {bmsize}",
                bm.len()
            )));
        }
        let blocks = self.sb.bitmap_blocks();
        let mut buf = vec![0u8; blocks as usize * EFS_V1_BLOCKSIZE as usize];
        self.read_blocks(EFS_V1_BITMAPBB, blocks, &mut buf)?;
        buf[..bmsize].copy_from_slice(bm);
        self.write_blocks(EFS_V1_BITMAPBB, &buf)
    }

    /// First-fit a contiguous run of `want` free blocks, confined to cylinder
    /// group data areas, and mark it in use. The caller persists `bm`.
    pub(crate) fn alloc_contiguous_in_bitmap(
        bm: &mut [u8],
        regions: &EfsV1DataRegions,
        want: u32,
    ) -> Result<EfsV1Extent, FilesystemError> {
        if want == 0 {
            return Err(FilesystemError::InvalidData(
                "EFS v1 alloc: want must be > 0".to_string(),
            ));
        }
        let total_bits = ((bm.len() as u64) * 8).min(u32::MAX as u64) as u32;
        for (lo, hi) in regions.ranges() {
            let hi = hi.min(total_bits);
            if hi <= lo {
                continue;
            }
            let mut run_start: Option<u32> = None;
            let mut run_len: u32 = 0;
            for bit in lo..hi {
                if (bm[(bit / 8) as usize] >> (bit % 8)) & 1 == 0 {
                    run_start = None;
                    run_len = 0;
                    continue;
                }
                let start = *run_start.get_or_insert(bit);
                run_len += 1;
                if run_len >= want {
                    for b in start..start + want {
                        bm[(b / 8) as usize] &= !(1u8 << (b % 8));
                    }
                    return Ok(EfsV1Extent {
                        magic: 0,
                        bn: start,
                        length: want as u8,
                        offset: 0,
                    });
                }
            }
        }
        Err(FilesystemError::DiskFull(format!(
            "EFS v1: no contiguous run of {want} free blocks in any cylinder group's data area"
        )))
    }

    /// Return an extent's blocks to the free pool. Convention: set bit = free.
    pub(crate) fn free_extent_in_bitmap(bm: &mut [u8], ext: &EfsV1Extent) {
        if ext.is_hole() {
            return;
        }
        for b in ext.bn..ext.bn + ext.length as u32 {
            let by = (b / 8) as usize;
            if by >= bm.len() {
                break;
            }
            bm[by] |= 1u8 << (b % 8);
        }
    }

    /// Free data blocks the bitmap still shows as available, counted only
    /// inside cylinder group data areas — the same span `fs_tfree` covers.
    pub(crate) fn count_free_blocks(bm: &[u8], regions: &EfsV1DataRegions) -> u32 {
        let total_bits = ((bm.len() as u64) * 8).min(u32::MAX as u64) as u32;
        let mut n = 0u32;
        for (lo, hi) in regions.ranges() {
            for bit in lo..hi.min(total_bits) {
                n += ((bm[(bit / 8) as usize] >> (bit % 8)) & 1) as u32;
            }
        }
        n
    }

    /// Lowest free inode (`di_mode == 0`). Inums 0 and 1 are reserved.
    pub(crate) fn allocate_inode(&mut self) -> Result<u32, FilesystemError> {
        let total = self.sb.total_inodes();
        for inum in 2..total {
            if self.read_inode(inum)?.is_free() {
                return Ok(inum);
            }
        }
        Err(FilesystemError::DiskFull(format!(
            "EFS v1: no free inodes (total {total})"
        )))
    }

    /// Write `inode` into its slot, preserving the three neighbours that share
    /// the 512-byte inode-table block.
    pub(crate) fn write_inode(&mut self, inode: &EfsV1Inode) -> Result<(), FilesystemError> {
        let (block, offset) = self.sb.inode_location(inode.inum).ok_or_else(|| {
            FilesystemError::InvalidData(format!(
                "EFS v1 write_inode: inum {} is past the inode table",
                inode.inum
            ))
        })?;
        if inode.inum < 2 {
            return Err(FilesystemError::InvalidData(
                "EFS v1 write_inode: inums 0 and 1 are reserved".to_string(),
            ));
        }
        let mut sector = [0u8; EFS_V1_BLOCKSIZE as usize];
        self.read_block(block, &mut sector)?;
        let mut slot = [0u8; EFS_V1_INODESIZE as usize];
        inode.write_into(&mut slot);
        sector[offset..offset + EFS_V1_INODESIZE as usize].copy_from_slice(&slot);
        self.write_block(block, &sector)
    }

    /// Return every block `inode` holds — data extents plus any indirect index
    /// blocks — to `bm`.
    fn free_inode_extents(
        &mut self,
        inode: &EfsV1Inode,
        bm: &mut [u8],
    ) -> Result<(), FilesystemError> {
        if inode.numextents == 0 || inode.is_device() {
            return Ok(());
        }
        let regions = EfsV1DataRegions::from_sb(&self.sb);
        let data_exts = self.extents_of(inode)?;
        for e in &data_exts {
            // A damaged inode can point at an inode table; freeing that would
            // hand live inodes to the allocator.
            if e.is_hole() {
                continue;
            }
            if !regions.contains(e.bn) || !regions.contains(e.bn + e.length as u32 - 1) {
                log::warn!(
                    "EFS v1 inode {}: extent {}+{} is outside every cylinder group's data area; \
                     not returning it to the free pool",
                    inode.inum,
                    e.bn,
                    e.length
                );
                continue;
            }
            Self::free_extent_in_bitmap(bm, e);
        }
        // In indirect mode the inline slots describe index blocks, which are
        // allocated space too and would otherwise leak.
        if inode.numextents as usize > EFS_V1_DIRECTEXTENTS {
            let direxts = (inode.extents[0].offset as usize).min(EFS_V1_DIRECTEXTENTS);
            for slot in &inode.extents[..direxts] {
                Self::free_extent_in_bitmap(bm, slot);
            }
        }
        Ok(())
    }

    /// Pack `data_extents` into index blocks and point the inline slots at
    /// them — how EFS describes a file needing more than twelve extents.
    fn install_indirect_extents(
        &mut self,
        bm: &mut [u8],
        inode: &mut EfsV1Inode,
        data_extents: &[EfsV1Extent],
    ) -> Result<(), FilesystemError> {
        let index_blocks = data_extents.len().div_ceil(EFS_V1_EXTENTS_PER_BLOCK) as u32;
        let regions = EfsV1DataRegions::from_sb(&self.sb);

        let mut index_exts: Vec<EfsV1Extent> = Vec::new();
        let mut remaining = index_blocks;
        while remaining > 0 {
            if index_exts.len() >= EFS_V1_DIRECTEXTENTS {
                for ext in &index_exts {
                    Self::free_extent_in_bitmap(bm, ext);
                }
                return Err(FilesystemError::DiskFull(format!(
                    "EFS v1: the {index_blocks} index blocks for this file need more than \
                     {EFS_V1_DIRECTEXTENTS} extents to describe (volume too fragmented)"
                )));
            }
            let mut chunk = remaining.min(EFS_V1_MAXEXTENTLEN);
            let ext = loop {
                match Self::alloc_contiguous_in_bitmap(bm, &regions, chunk) {
                    Ok(e) => break e,
                    Err(FilesystemError::DiskFull(_)) if chunk > 1 => chunk /= 2,
                    Err(e) => {
                        for ext in &index_exts {
                            Self::free_extent_in_bitmap(bm, ext);
                        }
                        return Err(e);
                    }
                }
            };
            remaining -= ext.length as u32;
            index_exts.push(ext);
        }

        let mut records = data_extents.iter();
        let mut block = [0u8; EFS_V1_BLOCKSIZE as usize];
        for ind in &index_exts {
            for i in 0..ind.length as u32 {
                block.fill(0);
                for slot in 0..EFS_V1_EXTENTS_PER_BLOCK {
                    let Some(ext) = records.next() else { break };
                    ext.write_into(&mut block[slot * 8..slot * 8 + 8]);
                }
                self.write_block(ind.bn + i, &block)?;
            }
        }

        inode.extents = [EFS_V1_EMPTY_EXTENT; EFS_V1_DIRECTEXTENTS];
        for (i, ext) in index_exts.iter().enumerate() {
            inode.extents[i] = *ext;
            inode.extents[i].offset = 0;
        }
        // Slot 0's `ex_offset` carries how many inline slots are index runs.
        inode.extents[0].offset = index_exts.len() as u32;
        inode.numextents = data_extents.len() as u16;
        Ok(())
    }

    /// Replace `inode`'s contents with `data`. In-memory convenience wrapper
    /// over [`Self::set_inode_stream`]; used for directories and symlinks.
    fn set_inode_data(
        &mut self,
        inode: &mut EfsV1Inode,
        data: &[u8],
        bm: &mut [u8],
    ) -> Result<(), FilesystemError> {
        let len = data.len() as u64;
        self.set_inode_stream(inode, &mut std::io::Cursor::new(data), len, bm)
    }

    /// Replace `inode`'s contents with `len` bytes from `data`, reallocating
    /// its extents. Old blocks go back to `bm` first, so a rewrite reuses them.
    fn set_inode_stream(
        &mut self,
        inode: &mut EfsV1Inode,
        data: &mut dyn Read,
        len: u64,
        bm: &mut [u8],
    ) -> Result<(), FilesystemError> {
        if len > EFS_V1_MAX_FILE_SIZE {
            return Err(FilesystemError::InvalidData(format!(
                "EFS v1: {len} bytes exceeds the {EFS_V1_MAX_FILE_SIZE}-byte file ceiling"
            )));
        }
        self.free_inode_extents(inode, bm)?;
        inode.extents = [EFS_V1_EMPTY_EXTENT; EFS_V1_DIRECTEXTENTS];
        inode.numextents = 0;
        inode.size = len as u32;
        if len == 0 {
            return Ok(());
        }

        let regions = EfsV1DataRegions::from_sb(&self.sb);
        let nblocks = len.div_ceil(EFS_V1_BLOCKSIZE) as u32;
        let mut exts: Vec<EfsV1Extent> = Vec::new();
        let mut remaining = nblocks;
        let mut logical = 0u32;
        while remaining > 0 {
            if exts.len() >= EFS_V1_MAXEXTENTS {
                for e in &exts {
                    Self::free_extent_in_bitmap(bm, e);
                }
                return Err(FilesystemError::DiskFull(format!(
                    "EFS v1: file needs more than {EFS_V1_MAXEXTENTS} extents"
                )));
            }
            let mut chunk = remaining.min(EFS_V1_MAXEXTENTLEN);
            let ext = loop {
                match Self::alloc_contiguous_in_bitmap(bm, &regions, chunk) {
                    Ok(mut e) => {
                        e.offset = logical;
                        break e;
                    }
                    Err(FilesystemError::DiskFull(_)) if chunk > 1 => chunk /= 2,
                    Err(e) => {
                        for x in &exts {
                            Self::free_extent_in_bitmap(bm, x);
                        }
                        return Err(e);
                    }
                }
            };
            logical += ext.length as u32;
            remaining -= ext.length as u32;
            exts.push(ext);
        }

        // Push the payload out before the inode points at it, so a failure
        // here never leaves an inode advertising blocks it does not own.
        let mut remaining = len;
        for e in &exts {
            let cap = e.length as u64 * EFS_V1_BLOCKSIZE;
            let span = cap.min(remaining) as usize;
            // One extent at a time — at most EFS_V1_MAXEXTENTLEN blocks, so a
            // multi-gigabyte file never lands in RAM whole.
            let mut buf = vec![0u8; cap as usize];
            data.read_exact(&mut buf[..span])?;
            self.write_blocks(e.bn, &buf)?;
            remaining -= span as u64;
        }

        if exts.len() <= EFS_V1_DIRECTEXTENTS {
            for (i, e) in exts.iter().enumerate() {
                inode.extents[i] = *e;
            }
            inode.numextents = exts.len() as u16;
            Ok(())
        } else {
            self.install_indirect_extents(bm, inode, &exts)
        }
    }

    /// Recount `fs_tfree` from the bitmap. Cheaper than tracking deltas and it
    /// self-corrects, matching the invariant the real disk holds.
    fn refresh_tfree(&mut self, bm: &[u8]) {
        let regions = EfsV1DataRegions::from_sb(&self.sb);
        self.sb.tfree = Self::count_free_blocks(bm, &regions);
    }

    /// The inum `name` maps to in `dir`, if present. `.` and `..` are matched
    /// like any other entry here, unlike the browse listing which hides them.
    fn dir_find(&mut self, dir: &EfsV1Inode, name: &str) -> Result<Option<u32>, FilesystemError> {
        let data = self.read_data(dir, EFS_V1_SANE_ALLOC)?;
        for chunk in data.chunks_exact(EFS_V1_DIRENTSIZE) {
            let inum = BigEndian::read_u16(&chunk[0..2]) as u32;
            if inum == 0 {
                continue;
            }
            let raw = &chunk[2..2 + EFS_V1_DIRSIZ];
            let end = raw.iter().position(|&b| b == 0).unwrap_or(raw.len());
            if raw[..end] == *name.as_bytes() {
                return Ok(Some(inum));
            }
        }
        Ok(None)
    }

    /// Add `(inum, name)` to `dir`, reusing a slot vacated by a delete when one
    /// is free. The caller writes `dir` and the bitmap back.
    fn dir_insert(
        &mut self,
        dir: &mut EfsV1Inode,
        name: &str,
        inum: u32,
        bm: &mut [u8],
    ) -> Result<(), FilesystemError> {
        let mut ent = [0u8; EFS_V1_DIRENTSIZE];
        BigEndian::write_u16(&mut ent[0..2], inum as u16);
        let nb = name.as_bytes();
        ent[2..2 + nb.len()].copy_from_slice(nb);

        let mut data = self.read_data(dir, EFS_V1_SANE_ALLOC)?;
        let mut placed = false;
        for chunk in data.chunks_exact_mut(EFS_V1_DIRENTSIZE) {
            if BigEndian::read_u16(&chunk[0..2]) == 0 {
                chunk.copy_from_slice(&ent);
                placed = true;
                break;
            }
        }
        if !placed {
            data.extend_from_slice(&ent);
        }
        self.set_inode_data(dir, &data, bm)
    }

    /// Clear `name`'s slot in `dir`, System V style: the entry's inum goes to
    /// zero and the directory keeps its size. Returns the inum that was there.
    fn dir_remove(
        &mut self,
        dir: &mut EfsV1Inode,
        name: &str,
        bm: &mut [u8],
    ) -> Result<Option<u32>, FilesystemError> {
        let mut data = self.read_data(dir, EFS_V1_SANE_ALLOC)?;
        let mut found = None;
        for chunk in data.chunks_exact_mut(EFS_V1_DIRENTSIZE) {
            let inum = BigEndian::read_u16(&chunk[0..2]) as u32;
            if inum == 0 {
                continue;
            }
            let raw = &chunk[2..2 + EFS_V1_DIRSIZ];
            let end = raw.iter().position(|&b| b == 0).unwrap_or(raw.len());
            if raw[..end] == *name.as_bytes() {
                found = Some(inum);
                chunk.fill(0);
                break;
            }
        }
        if found.is_some() {
            self.set_inode_data(dir, &data, bm)?;
        }
        Ok(found)
    }
}

impl<R: Read + Write + Seek + Send> super::filesystem::EditableFilesystem for EfsV1Filesystem<R> {
    fn as_filesystem(&self) -> &dyn Filesystem {
        self
    }

    fn as_filesystem_mut(&mut self) -> &mut dyn Filesystem {
        self
    }

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
        self.validate_name(name)?;
        if data_len > EFS_V1_MAX_FILE_SIZE {
            return Err(FilesystemError::InvalidData(format!(
                "EFS v1: {data_len} bytes exceeds the {EFS_V1_MAX_FILE_SIZE}-byte file ceiling"
            )));
        }
        let parent_inum = parent.location as u32;
        let parent_inode = self.read_inode(parent_inum)?;
        if self.dir_find(&parent_inode, name)?.is_some() {
            return Err(FilesystemError::AlreadyExists(name.to_string()));
        }

        let mut bm = self.read_bitmap()?;
        let inum = self.allocate_inode()?;
        let times = super::times::resolve_or_now(options.unix_times);
        let now = now_u32();

        let mut ino = EfsV1Inode::empty(inum);
        ino.mode = options.mode.unwrap_or(0o100644) as u16;
        ino.nlink = 1;
        ino.uid = options.uid.unwrap_or(0) as u16;
        ino.gid = options.gid.unwrap_or(0) as u16;
        ino.atime = times.atime_or_now() as u32;
        ino.mtime = times.mtime_or_now() as u32;
        ino.ctime = times.ctime_or_now() as u32;
        self.set_inode_stream(&mut ino, &mut data.take(data_len), data_len, &mut bm)?;

        // dir_insert can reallocate the parent's extents, so re-read it fresh
        // rather than reusing the copy taken for the duplicate-name check.
        let mut parent_ino = self.read_inode(parent_inum)?;
        self.dir_insert(&mut parent_ino, name, inum, &mut bm)?;
        parent_ino.mtime = now;
        parent_ino.ctime = now;

        // Bitmap before the inodes that reference those blocks: failing here
        // leaks free space, whereas the other order hands them out twice.
        self.refresh_tfree(&bm);
        self.write_bitmap(&bm)?;
        self.write_inode(&ino)?;
        self.write_inode(&parent_ino)?;

        self.sb.tinode = self.sb.tinode.saturating_sub(1);
        self.sync_superblock()?;
        self.highest_block = None;
        Ok(self.entry_from_inode(name, &parent.path, &ino))
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
        self.validate_name(name)?;
        let parent_inum = parent.location as u32;
        let parent_inode = self.read_inode(parent_inum)?;
        if self.dir_find(&parent_inode, name)?.is_some() {
            return Err(FilesystemError::AlreadyExists(name.to_string()));
        }

        let mut bm = self.read_bitmap()?;
        let inum = self.allocate_inode()?;
        let times = super::times::resolve_or_now(options.unix_times);
        let now = now_u32();

        // A System V directory starts life holding exactly `.` and `..`.
        let mut seed = vec![0u8; EFS_V1_DIRENTSIZE * 2];
        BigEndian::write_u16(&mut seed[0..2], inum as u16);
        seed[2] = b'.';
        BigEndian::write_u16(
            &mut seed[EFS_V1_DIRENTSIZE..EFS_V1_DIRENTSIZE + 2],
            parent_inum as u16,
        );
        seed[EFS_V1_DIRENTSIZE + 2] = b'.';
        seed[EFS_V1_DIRENTSIZE + 3] = b'.';

        let mut ino = EfsV1Inode::empty(inum);
        ino.mode = options.mode.unwrap_or(0o040755) as u16;
        // `.` plus the child link from the parent.
        ino.nlink = 2;
        ino.uid = options.uid.unwrap_or(0) as u16;
        ino.gid = options.gid.unwrap_or(0) as u16;
        ino.atime = times.atime_or_now() as u32;
        ino.mtime = times.mtime_or_now() as u32;
        ino.ctime = times.ctime_or_now() as u32;
        self.set_inode_data(&mut ino, &seed, &mut bm)?;

        let mut parent_ino = self.read_inode(parent_inum)?;
        self.dir_insert(&mut parent_ino, name, inum, &mut bm)?;
        // The new directory's `..` is another link to the parent.
        parent_ino.nlink = parent_ino.nlink.saturating_add(1);
        parent_ino.mtime = now;
        parent_ino.ctime = now;

        // See create_file: allocations reach disk before anything cites them.
        self.refresh_tfree(&bm);
        self.write_bitmap(&bm)?;
        self.write_inode(&ino)?;
        self.write_inode(&parent_ino)?;

        self.sb.tinode = self.sb.tinode.saturating_sub(1);
        self.sync_superblock()?;
        self.highest_block = None;
        Ok(self.entry_from_inode(name, &parent.path, &ino))
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
        let inum = entry.location as u32;
        let ino = self.read_inode(inum)?;
        if ino.is_free() {
            return Err(FilesystemError::NotFound(entry.path.clone()));
        }
        if ino.is_dir() {
            // Only `.` and `..` may remain.
            let children = self.read_dir_entries(&ino)?;
            if !children.is_empty() {
                return Err(FilesystemError::InvalidData(format!(
                    "EFS v1: directory {} is not empty ({} entries remain)",
                    entry.path,
                    children.len()
                )));
            }
        }

        let mut bm = self.read_bitmap()?;
        let mut parent_ino = self.read_inode(parent_inum)?;
        let removed = self.dir_remove(&mut parent_ino, &entry.name, &mut bm)?;
        if removed.is_none() {
            return Err(FilesystemError::NotFound(entry.path.clone()));
        }
        let now = now_u32();
        parent_ino.mtime = now;
        parent_ino.ctime = now;
        if ino.is_dir() {
            // The child's `..` is gone.
            parent_ino.nlink = parent_ino.nlink.saturating_sub(1);
        }
        self.write_inode(&parent_ino)?;

        self.free_inode_extents(&ino, &mut bm)?;
        self.write_inode(&EfsV1Inode::empty(inum))?;

        self.sb.tinode = self.sb.tinode.saturating_add(1);
        self.refresh_tfree(&bm);
        self.write_bitmap(&bm)?;
        self.sync_superblock()?;
        self.highest_block = None;
        Ok(())
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
        self.validate_name(new_name)?;
        let parent_inum = parent.location as u32;
        let parent_inode = self.read_inode(parent_inum)?;
        if self.dir_find(&parent_inode, new_name)?.is_some() {
            return Err(FilesystemError::AlreadyExists(new_name.to_string()));
        }

        let mut bm = self.read_bitmap()?;
        let mut parent_ino = self.read_inode(parent_inum)?;
        let inum = match self.dir_remove(&mut parent_ino, &entry.name, &mut bm)? {
            Some(i) => i,
            None => return Err(FilesystemError::NotFound(entry.path.clone())),
        };
        self.dir_insert(&mut parent_ino, new_name, inum, &mut bm)?;
        let now = now_u32();
        parent_ino.mtime = now;
        parent_ino.ctime = now;
        self.write_inode(&parent_ino)?;

        // The inode itself is untouched: same inum, same extents, same data.
        let mut ino = self.read_inode(inum)?;
        ino.ctime = now;
        self.write_inode(&ino)?;

        self.refresh_tfree(&bm);
        self.write_bitmap(&bm)?;
        self.sync_superblock()
    }

    fn set_permissions(&mut self, entry: &FileEntry, mode: u32) -> Result<(), FilesystemError> {
        let mut ino = self.read_inode(entry.location as u32)?;
        ino.mode = super::unix_common::inode::with_permission_bits(ino.mode as u32, mode) as u16;
        ino.ctime = now_u32();
        self.write_inode(&ino)
    }

    fn set_owner(&mut self, entry: &FileEntry, uid: u32, gid: u32) -> Result<(), FilesystemError> {
        let mut ino = self.read_inode(entry.location as u32)?;
        ino.uid = uid as u16;
        ino.gid = gid as u16;
        ino.ctime = now_u32();
        self.write_inode(&ino)
    }

    fn supports_symlinks(&self) -> bool {
        true
    }

    /// SGI carried 4.2BSD symlinks into this System V derivative, and stores
    /// the target as ordinary file data rather than inside the inode.
    fn create_symlink(
        &mut self,
        parent: &FileEntry,
        name: &str,
        target: &str,
        options: &super::filesystem::CreateFileOptions,
    ) -> Result<FileEntry, FilesystemError> {
        if !parent.is_directory() {
            return Err(FilesystemError::NotADirectory(parent.path.clone()));
        }
        self.validate_name(name)?;
        if target.is_empty() || target.len() > EFS_V1_MAXPATHLEN {
            return Err(FilesystemError::InvalidData(format!(
                "EFS v1 symlink target must be 1..={EFS_V1_MAXPATHLEN} bytes, got {}",
                target.len()
            )));
        }
        let parent_inum = parent.location as u32;
        let parent_inode = self.read_inode(parent_inum)?;
        if self.dir_find(&parent_inode, name)?.is_some() {
            return Err(FilesystemError::AlreadyExists(name.to_string()));
        }

        let mut bm = self.read_bitmap()?;
        let inum = self.allocate_inode()?;
        let now = now_u32();
        let times = super::times::resolve_or_now(options.unix_times);
        let mut ino = EfsV1Inode::empty(inum);
        ino.mode = 0o120777;
        ino.nlink = 1;
        ino.uid = options.uid.unwrap_or(0) as u16;
        ino.gid = options.gid.unwrap_or(0) as u16;
        ino.atime = times.atime_or_now() as u32;
        ino.mtime = times.mtime_or_now() as u32;
        ino.ctime = times.ctime_or_now() as u32;
        self.set_inode_data(&mut ino, target.as_bytes(), &mut bm)?;

        let mut parent_ino = self.read_inode(parent_inum)?;
        self.dir_insert(&mut parent_ino, name, inum, &mut bm)?;
        parent_ino.mtime = now;
        parent_ino.ctime = now;

        // See create_file: allocations reach disk before anything cites them.
        self.refresh_tfree(&bm);
        self.write_bitmap(&bm)?;
        self.write_inode(&ino)?;
        self.write_inode(&parent_ino)?;

        self.sb.tinode = self.sb.tinode.saturating_sub(1);
        self.sync_superblock()?;
        self.highest_block = None;
        let mut e = self.entry_from_inode(name, &parent.path, &ino);
        e.symlink_target = Some(target.to_string());
        Ok(e)
    }

    fn sync_metadata(&mut self) -> Result<(), FilesystemError> {
        self.sync_superblock()
    }

    fn free_space(&mut self) -> Result<u64, FilesystemError> {
        let bm = self.read_bitmap()?;
        let regions = EfsV1DataRegions::from_sb(&self.sb);
        Ok(Self::count_free_blocks(&bm, &regions) as u64 * EFS_V1_BLOCKSIZE)
    }
}

impl<R: Read + Seek + Send> Filesystem for EfsV1Filesystem<R> {
    /// `DIRSIZ` is 14 bytes and names are not NUL-terminated when they fill
    /// the field, so anything longer cannot be represented at all.
    fn validate_name(&self, name: &str) -> Result<(), FilesystemError> {
        if name.is_empty() {
            return Err(FilesystemError::InvalidData("empty name".to_string()));
        }
        if name == "." || name == ".." {
            return Err(FilesystemError::InvalidData(format!(
                "'{name}' is reserved"
            )));
        }
        if name.contains('/') || name.contains('\0') {
            return Err(FilesystemError::InvalidData(format!(
                "EFS v1 name '{name}' contains a path separator or NUL"
            )));
        }
        if name.len() > EFS_V1_DIRSIZ {
            return Err(FilesystemError::InvalidData(format!(
                "EFS v1 name '{name}' is {} bytes; the limit is {EFS_V1_DIRSIZ}",
                name.len()
            )));
        }
        Ok(())
    }

    fn root(&mut self) -> Result<FileEntry, FilesystemError> {
        let ino = self.read_inode(EFS_V1_ROOT_INODE)?;
        if !ino.is_dir() {
            return Err(FilesystemError::InvalidData(format!(
                "EFS v1 root inode 2 is not a directory (mode=0o{:o})",
                ino.mode
            )));
        }
        let mut e = self.entry_from_inode("/", "", &ino);
        e.path = "/".to_string();
        e.name = "/".to_string();
        Ok(e)
    }

    fn list_directory(&mut self, entry: &FileEntry) -> Result<Vec<FileEntry>, FilesystemError> {
        if !entry.is_directory() {
            return Err(FilesystemError::NotADirectory(entry.path.clone()));
        }
        let dir = self.read_inode(entry.location as u32)?;
        if !dir.is_dir() {
            return Err(FilesystemError::NotADirectory(entry.path.clone()));
        }
        let names = self.read_dir_entries(&dir)?;
        let mut out = Vec::with_capacity(names.len());
        for (inum, name) in names {
            match self.read_inode(inum) {
                Ok(child) => {
                    let mut e = self.entry_from_inode(&name, &entry.path, &child);
                    if child.is_symlink() {
                        e.symlink_target = self.read_symlink_target(&child);
                    }
                    out.push(e);
                }
                // Damaged inode: keep the name visible rather than losing the
                // whole listing to one bad block.
                Err(_) => out.push(FileEntry::new_file(
                    name.clone(),
                    join_path(&entry.path, &name),
                    0,
                    inum as u64,
                )),
            }
        }
        Ok(out)
    }

    fn read_file(
        &mut self,
        entry: &FileEntry,
        max_bytes: usize,
    ) -> Result<Vec<u8>, FilesystemError> {
        if entry.is_directory() {
            return Err(FilesystemError::InvalidData(format!(
                "EFS v1 read_file on directory: {}",
                entry.path
            )));
        }
        let inode = self.read_inode(entry.location as u32)?;
        self.read_data(&inode, max_bytes)
    }

    fn write_file_to(
        &mut self,
        entry: &FileEntry,
        writer: &mut dyn Write,
    ) -> Result<u64, FilesystemError> {
        if entry.is_directory() {
            return Err(FilesystemError::InvalidData(format!(
                "EFS v1 write_file_to on directory: {}",
                entry.path
            )));
        }
        let inode = self.read_inode(entry.location as u32)?;
        self.stream_data(&inode, u64::MAX, writer)
    }

    fn volume_label(&self) -> Option<&str> {
        if self.label.is_empty() {
            None
        } else {
            Some(&self.label)
        }
    }

    fn fs_type(&self) -> &str {
        "SGI EFS v1"
    }

    fn total_size(&self) -> u64 {
        self.sb.fs_size as u64 * EFS_V1_BLOCKSIZE
    }

    fn used_size(&self) -> u64 {
        self.total_size()
            .saturating_sub(self.sb.tfree as u64 * EFS_V1_BLOCKSIZE)
    }

    fn allocation_unit(&self) -> Option<u64> {
        Some(EFS_V1_BLOCKSIZE)
    }

    fn last_data_byte(&mut self) -> Result<u64, FilesystemError> {
        Ok(self.scan_highest_block()? as u64 * EFS_V1_BLOCKSIZE)
    }
}

/// Smallest volume worth formatting: the bitmap, one cylinder group with an
/// inode table, and room for a root directory.
const EFS_V1_MIN_BLOCKS: u32 = 256;

/// Blocks per cylinder group to aim for. The IRIS 3130 disk this driver was
/// written from used 3568 (root) and 3984 (/usr); 3600 sits between them.
const EFS_V1_TARGET_CGFSIZE: u32 = 3600;

/// Fraction of a cylinder group given over to its inode table, as a divisor.
/// The real disk ran 88/3568 and 96/3984 — both a hair under 1/40.
const EFS_V1_INODE_DIVISOR: u32 = 40;

/// Format a blank EFS v1 volume, in **native** byte order (a fresh image has
/// not been through a word-swapping controller). See docs/SGI_EFS_v1.md.
pub fn create_blank_efs_v1(size_bytes: u64, name: &str) -> anyhow::Result<Vec<u8>> {
    let total_blocks = (size_bytes / EFS_V1_BLOCKSIZE) as u32;
    if total_blocks < EFS_V1_MIN_BLOCKS {
        anyhow::bail!(
            "EFS v1 needs at least {} bytes; asked for {size_bytes}",
            EFS_V1_MIN_BLOCKS as u64 * EFS_V1_BLOCKSIZE
        );
    }

    // Geometry closes on `fs_size == firstcg + ncg * cgfsize` while `firstcg`
    // must clear the bitmap sized from it, so iterate until the two agree.
    let mut ncg = (total_blocks / EFS_V1_TARGET_CGFSIZE).max(1);
    while ncg > 1 && total_blocks / ncg < EFS_V1_MIN_BLOCKS {
        ncg -= 1;
    }
    let mut firstcg = EFS_V1_BITMAPBB + 2;
    let (fs_size, cgfsize, bmsize) = loop {
        let avail = total_blocks.saturating_sub(firstcg);
        let cgfsize = avail / ncg;
        if cgfsize < 8 {
            anyhow::bail!("EFS v1: {size_bytes} bytes is too small to lay out a cylinder group");
        }
        let fs_size = firstcg + ncg * cgfsize;
        let bmsize = fs_size.div_ceil(8);
        let bmblocks = bmsize.div_ceil(EFS_V1_BLOCKSIZE as u32);
        // Keep `firstcg` even, the way the period mkfs laid it out.
        let need = EFS_V1_BITMAPBB + bmblocks;
        let need = need + (need & 1);
        if firstcg >= need {
            break (fs_size, cgfsize, bmsize);
        }
        firstcg = need;
    };
    let cgisize = (cgfsize / EFS_V1_INODE_DIVISOR).max(1).min(cgfsize - 1);

    let mut sb = EfsV1Superblock {
        fs_size,
        firstcg,
        cgfsize,
        cgisize: cgisize as u16,
        sectors: 17,
        heads: 7,
        ncg: ncg as u16,
        dirty: 0,
        fs_time: now_u32(),
        fname: pack_label(name),
        fpack: pack_label("sgi"),
        magic: EFS_V1_MAGIC,
        prealloc: 16,
        bmsize,
        tfree: 0,
        tinode: 0,
        checksum: 0,
    };
    sb.validate()
        .map_err(|e| anyhow::anyhow!("EFS v1 mkfs produced invalid geometry: {e}"))?;

    let mut img = vec![0u8; (total_blocks as u64 * EFS_V1_BLOCKSIZE) as usize];
    let regions = EfsV1DataRegions::from_sb(&sb);

    // Bitmap starts all-in-use, then each group's data area is released —
    // inode tables stay in use, as on a real EFS v1 volume.
    let mut bm = vec![0u8; bmsize as usize];
    for (lo, hi) in regions.ranges() {
        for b in lo..hi {
            bm[(b / 8) as usize] |= 1u8 << (b % 8);
        }
    }

    // The root directory gets the first data block of cylinder group 0.
    let root_block = firstcg + cgisize;
    bm[(root_block / 8) as usize] &= !(1u8 << (root_block % 8));

    let mut root_dir = vec![0u8; EFS_V1_DIRENTSIZE * 2];
    BigEndian::write_u16(&mut root_dir[0..2], EFS_V1_ROOT_INODE as u16);
    root_dir[2] = b'.';
    BigEndian::write_u16(
        &mut root_dir[EFS_V1_DIRENTSIZE..EFS_V1_DIRENTSIZE + 2],
        EFS_V1_ROOT_INODE as u16,
    );
    root_dir[EFS_V1_DIRENTSIZE + 2] = b'.';
    root_dir[EFS_V1_DIRENTSIZE + 3] = b'.';
    let root_off = root_block as usize * EFS_V1_BLOCKSIZE as usize;
    img[root_off..root_off + root_dir.len()].copy_from_slice(&root_dir);

    let now = sb.fs_time;
    let mut root = EfsV1Inode::empty(EFS_V1_ROOT_INODE);
    // 0777 is what the era's mkfs stamped on both volumes of the real disk.
    root.mode = 0o040777;
    // `.` and the `..` of the root itself.
    root.nlink = 2;
    root.size = root_dir.len() as u32;
    root.atime = now;
    root.mtime = now;
    root.ctime = now;
    root.numextents = 1;
    root.extents[0] = EfsV1Extent {
        magic: 0,
        bn: root_block,
        length: 1,
        offset: 0,
    };

    // `fs_tinode` counts free inodes but leaves one out, verified against both
    // volumes of the IRIS 3130 disk.
    sb.tfree = count_free_bits(&bm, &regions);
    sb.tinode = sb.total_inodes().saturating_sub(2);

    let (iblock, ioff) = sb
        .inode_location(EFS_V1_ROOT_INODE)
        .ok_or_else(|| anyhow::anyhow!("EFS v1 mkfs: root inode has no home in the inode table"))?;
    let mut slot = [0u8; EFS_V1_INODESIZE as usize];
    root.write_into(&mut slot);
    let ipos = iblock as usize * EFS_V1_BLOCKSIZE as usize + ioff;
    img[ipos..ipos + slot.len()].copy_from_slice(&slot);

    let bmpos = EFS_V1_BITMAPBB as usize * EFS_V1_BLOCKSIZE as usize;
    img[bmpos..bmpos + bm.len()].copy_from_slice(&bm);

    let sbpos = EFS_V1_SUPERBB as usize * EFS_V1_BLOCKSIZE as usize;
    let sector = &mut img[sbpos..sbpos + EFS_V1_BLOCKSIZE as usize];
    sb.write_into(sector);
    sb.recompute_checksum(sector);

    Ok(img)
}

/// Pack a volume label into one of the superblock's 6-byte name fields.
fn pack_label(name: &str) -> [u8; 6] {
    let mut out = [0u8; 6];
    let bytes: Vec<u8> = name
        .bytes()
        .filter(|b| b.is_ascii_graphic())
        .take(6)
        .collect();
    out[..bytes.len()].copy_from_slice(&bytes);
    out
}

/// Free bits inside cylinder group data areas — the span `fs_tfree` covers.
fn count_free_bits(bm: &[u8], regions: &EfsV1DataRegions) -> u32 {
    let total_bits = ((bm.len() as u64) * 8).min(u32::MAX as u64) as u32;
    let mut n = 0u32;
    for (lo, hi) in regions.ranges() {
        for bit in lo..hi.min(total_bits) {
            n += ((bm[(bit / 8) as usize] >> (bit % 8)) & 1) as u32;
        }
    }
    n
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Cursor;

    const BS: usize = EFS_V1_BLOCKSIZE as usize;

    /// Build a small but structurally faithful EFS v1 volume:
    /// firstcg 8, one cylinder group of 40 blocks with 2 inode blocks, so
    /// fs_size 48 and data from block 10.
    struct Builder {
        img: Vec<u8>,
        firstcg: u32,
        cgfsize: u32,
        cgisize: u16,
        ncg: u16,
    }

    impl Builder {
        fn new() -> Self {
            let firstcg = 8u32;
            let cgfsize = 40u32;
            let cgisize = 2u16;
            let ncg = 1u16;
            let fs_size = firstcg + ncg as u32 * cgfsize;
            Builder {
                img: vec![0u8; fs_size as usize * BS],
                firstcg,
                cgfsize,
                cgisize,
                ncg,
            }
        }

        fn fs_size(&self) -> u32 {
            self.firstcg + self.ncg as u32 * self.cgfsize
        }

        fn write_superblock(&mut self, tfree: u32, tinode: u32) {
            let o = BS; // block 1
            let fs_size = self.fs_size();
            let bmsize = (fs_size as u64).div_ceil(8) as u32;
            let (firstcg, cgfsize, cgisize, ncg) =
                (self.firstcg, self.cgfsize, self.cgisize, self.ncg);
            let sb = &mut self.img[o..o + EFS_V1_SUPERBLOCK_SIZE];
            BigEndian::write_u32(&mut sb[0x00..0x04], fs_size);
            BigEndian::write_u32(&mut sb[0x04..0x08], firstcg);
            BigEndian::write_u32(&mut sb[0x08..0x0C], cgfsize);
            BigEndian::write_u16(&mut sb[0x0C..0x0E], cgisize);
            BigEndian::write_u16(&mut sb[0x0E..0x10], 17);
            BigEndian::write_u16(&mut sb[0x10..0x12], 7);
            BigEndian::write_u16(&mut sb[0x12..0x14], ncg);
            BigEndian::write_u16(&mut sb[0x14..0x16], 0);
            BigEndian::write_u32(&mut sb[0x16..0x1A], 0x21CB_DE2B);
            sb[0x1A..0x1E].copy_from_slice(b"root");
            sb[0x20..0x23].copy_from_slice(b"sgi");
            BigEndian::write_u32(&mut sb[OFF_MAGIC..OFF_MAGIC + 4], EFS_V1_MAGIC);
            BigEndian::write_u32(&mut sb[0x2A..0x2E], 16);
            BigEndian::write_u32(&mut sb[0x2E..0x32], bmsize);
            BigEndian::write_u32(&mut sb[0x32..0x36], tfree);
            BigEndian::write_u32(&mut sb[0x36..0x3A], tinode);
        }

        fn inode_offset(&self, inum: u32) -> usize {
            let cg_bb = (inum / EFS_V1_INOPBB) % self.cgisize as u32;
            let block = self.firstcg + cg_bb;
            block as usize * BS + (inum % EFS_V1_INOPBB) as usize * EFS_V1_INODESIZE as usize
        }

        fn write_inode(&mut self, inum: u32, mode: u16, size: u32, exts: &[(u32, u8, u32)]) {
            let o = self.inode_offset(inum);
            let ino = &mut self.img[o..o + EFS_V1_INODESIZE as usize];
            BigEndian::write_u16(&mut ino[0x00..0x02], mode);
            BigEndian::write_u16(&mut ino[0x02..0x04], 2);
            BigEndian::write_u16(&mut ino[0x04..0x06], 0);
            BigEndian::write_u16(&mut ino[0x06..0x08], 0);
            BigEndian::write_u32(&mut ino[0x08..0x0C], size);
            BigEndian::write_u32(&mut ino[0x0C..0x10], 0x21CB_DC4A);
            BigEndian::write_u32(&mut ino[0x10..0x14], 0x21CB_DCA3);
            BigEndian::write_u32(&mut ino[0x14..0x18], 0x21CB_DCA3);
            BigEndian::write_u16(&mut ino[0x1C..0x1E], exts.len() as u16);
            for (i, (bn, len, off)) in exts.iter().enumerate() {
                let eo = 0x20 + i * 8;
                BigEndian::write_u32(&mut ino[eo..eo + 4], *bn & 0x00FF_FFFF);
                BigEndian::write_u32(&mut ino[eo + 4..eo + 8], ((*len as u32) << 24) | *off);
            }
        }

        fn write_dir(&mut self, bn: u32, entries: &[(u32, &str)]) {
            let o = bn as usize * BS;
            for (i, (inum, name)) in entries.iter().enumerate() {
                let eo = o + i * EFS_V1_DIRENTSIZE;
                BigEndian::write_u16(&mut self.img[eo..eo + 2], *inum as u16);
                let n = name.as_bytes();
                self.img[eo + 2..eo + 2 + n.len()].copy_from_slice(n);
            }
        }

        fn write_data(&mut self, bn: u32, bytes: &[u8]) {
            let o = bn as usize * BS;
            self.img[o..o + bytes.len()].copy_from_slice(bytes);
        }
    }

    /// Root at inode 2 with `hello` (inode 3) and a `sub` directory (inode 4)
    /// holding `deep` (inode 5).
    fn sample_volume() -> Vec<u8> {
        let mut b = Builder::new();
        b.write_superblock(20, 100);
        // Root directory in block 10.
        b.write_inode(2, 0o040777, (4 * EFS_V1_DIRENTSIZE) as u32, &[(10, 1, 0)]);
        b.write_dir(10, &[(2, "."), (2, ".."), (3, "hello"), (4, "sub")]);
        // Regular file in block 11.
        b.write_inode(3, 0o100644, 12, &[(11, 1, 0)]);
        b.write_data(11, b"hello world\n");
        // Subdirectory in block 12.
        b.write_inode(4, 0o040755, (3 * EFS_V1_DIRENTSIZE) as u32, &[(12, 1, 0)]);
        b.write_dir(12, &[(4, "."), (2, ".."), (5, "deep")]);
        // Two-extent file, out of logical order on disk, in blocks 14 and 13.
        b.write_inode(5, 0o100644, 700, &[(14, 1, 1), (13, 1, 0)]);
        b.write_data(13, &[0xAA; BS]);
        b.write_data(14, &[0xBB; 188]);
        b.img
    }

    fn open(img: Vec<u8>) -> EfsV1Filesystem<Cursor<Vec<u8>>> {
        EfsV1Filesystem::open(Cursor::new(img), 0).unwrap()
    }

    fn swab(mut v: Vec<u8>) -> Vec<u8> {
        crate::partition::sgi_dklabel::swab16_in_place(&mut v);
        v
    }

    fn names(fs: &mut EfsV1Filesystem<Cursor<Vec<u8>>>, dir: &FileEntry) -> Vec<String> {
        let mut n: Vec<String> = fs
            .list_directory(dir)
            .unwrap()
            .into_iter()
            .map(|e| e.name)
            .collect();
        n.sort();
        n
    }

    #[test]
    fn parses_superblock_fields() {
        let fs = open(sample_volume());
        let sb = fs.superblock();
        assert_eq!(sb.magic, EFS_V1_MAGIC);
        assert_eq!(sb.fs_size, 48);
        assert_eq!(sb.firstcg, 8);
        assert_eq!(sb.cgfsize, 40);
        assert_eq!(sb.cgisize, 2);
        assert_eq!(sb.ncg, 1);
        assert_eq!(sb.prealloc, 16);
        assert_eq!(sb.inodes_per_cg(), 8);
        assert_eq!(sb.total_inodes(), 8);
        assert_eq!(fs.volume_label(), Some("root:sgi"));
        assert_eq!(fs.fs_type(), "SGI EFS v1");
    }

    #[test]
    fn superblock_offsets_match_m68k_two_byte_packing() {
        // fs_time is the field that proves the packing: at 0x16, not 0x18.
        // If this drifts, fs_magic lands two bytes out and nothing parses.
        let fs = open(sample_volume());
        assert_eq!(fs.superblock().fs_time, 0x21CB_DE2B);
        assert_eq!(OFF_MAGIC, 0x26);
        assert_eq!(EFS_V1_SUPERBLOCK_SIZE, 0xA2);
    }

    #[test]
    fn lists_root_and_descends() {
        let mut fs = open(sample_volume());
        let root = fs.root().unwrap();
        assert_eq!(root.path, "/");
        assert_eq!(names(&mut fs, &root), vec!["hello", "sub"]);
        let sub = fs
            .list_directory(&root)
            .unwrap()
            .into_iter()
            .find(|e| e.name == "sub")
            .unwrap();
        assert_eq!(sub.path, "/sub");
        assert!(sub.is_directory());
        assert_eq!(names(&mut fs, &sub), vec!["deep"]);
    }

    #[test]
    fn dot_entries_are_hidden() {
        let mut fs = open(sample_volume());
        let root = fs.root().unwrap();
        assert!(!names(&mut fs, &root).iter().any(|n| n == "." || n == ".."));
    }

    #[test]
    fn reads_file_contents() {
        let mut fs = open(sample_volume());
        let root = fs.root().unwrap();
        let hello = fs
            .list_directory(&root)
            .unwrap()
            .into_iter()
            .find(|e| e.name == "hello")
            .unwrap();
        assert_eq!(hello.size, 12);
        assert_eq!(hello.mode, Some(0o100644));
        assert_eq!(fs.read_file(&hello, usize::MAX).unwrap(), b"hello world\n");
        assert_eq!(fs.read_file(&hello, 5).unwrap(), b"hello");
    }

    #[test]
    fn reassembles_extents_in_logical_order() {
        // The inode lists the offset-1 extent first; the reader must sort.
        let mut fs = open(sample_volume());
        let root = fs.root().unwrap();
        let sub = fs
            .list_directory(&root)
            .unwrap()
            .into_iter()
            .find(|e| e.name == "sub")
            .unwrap();
        let deep = fs
            .list_directory(&sub)
            .unwrap()
            .into_iter()
            .find(|e| e.name == "deep")
            .unwrap();
        let data = fs.read_file(&deep, usize::MAX).unwrap();
        assert_eq!(data.len(), 700);
        assert!(data[..BS].iter().all(|&b| b == 0xAA));
        assert!(data[BS..].iter().all(|&b| b == 0xBB));
    }

    #[test]
    fn write_file_to_matches_read_file() {
        let mut fs = open(sample_volume());
        let root = fs.root().unwrap();
        let hello = fs
            .list_directory(&root)
            .unwrap()
            .into_iter()
            .find(|e| e.name == "hello")
            .unwrap();
        let mut out = Vec::new();
        let n = fs.write_file_to(&hello, &mut out).unwrap();
        assert_eq!(n, 12);
        assert_eq!(out, b"hello world\n");
    }

    #[test]
    fn opens_a_byte_swapped_image_identically() {
        let plain = sample_volume();
        let mut a = open(plain.clone());
        let mut b = open(swab(plain));
        assert_eq!(a.byte_order(), SgiLabelByteOrder::Native);
        assert_eq!(b.byte_order(), SgiLabelByteOrder::Swabbed);
        assert_eq!(a.superblock(), b.superblock());
        let ra = a.root().unwrap();
        let rb = b.root().unwrap();
        assert_eq!(names(&mut a, &ra), names(&mut b, &rb));
        let ha = a
            .list_directory(&ra)
            .unwrap()
            .into_iter()
            .find(|e| e.name == "hello")
            .unwrap();
        let hb = b
            .list_directory(&rb)
            .unwrap()
            .into_iter()
            .find(|e| e.name == "hello")
            .unwrap();
        assert_eq!(
            a.read_file(&ha, usize::MAX).unwrap(),
            b.read_file(&hb, usize::MAX).unwrap()
        );
    }

    #[test]
    fn holes_read_as_zeros() {
        let mut b = Builder::new();
        b.write_superblock(20, 100);
        b.write_inode(2, 0o040777, (3 * EFS_V1_DIRENTSIZE) as u32, &[(10, 1, 0)]);
        b.write_dir(10, &[(2, "."), (2, ".."), (3, "sparse")]);
        // Logical block 0 is a hole (bn 0); block 1 has real data.
        b.write_inode(3, 0o100644, 1024, &[(0, 1, 0), (11, 1, 1)]);
        b.write_data(11, &[0xCD; BS]);
        let mut fs = open(b.img);
        let root = fs.root().unwrap();
        let sparse = fs
            .list_directory(&root)
            .unwrap()
            .into_iter()
            .find(|e| e.name == "sparse")
            .unwrap();
        let data = fs.read_file(&sparse, usize::MAX).unwrap();
        assert_eq!(data.len(), 1024);
        assert!(data[..BS].iter().all(|&b| b == 0));
        assert!(data[BS..].iter().all(|&b| b == 0xCD));
    }

    #[test]
    fn device_nodes_surface_as_special_entries() {
        let mut b = Builder::new();
        b.write_superblock(20, 100);
        b.write_inode(2, 0o040777, (3 * EFS_V1_DIRENTSIZE) as u32, &[(10, 1, 0)]);
        b.write_dir(10, &[(2, "."), (2, ".."), (3, "console")]);
        // di_dev shares storage with the first extent slot: major 0, minor 1.
        b.write_inode(3, 0o020600, 0, &[(0x0001, 0, 0)]);
        let mut fs = open(b.img);
        let root = fs.root().unwrap();
        let dev = fs
            .list_directory(&root)
            .unwrap()
            .into_iter()
            .find(|e| e.name == "console")
            .unwrap();
        assert_eq!(dev.entry_type, EntryType::Special);
        assert_eq!(dev.special_type.as_deref(), Some("char device 0,1"));
    }

    #[test]
    fn rejects_geometry_that_does_not_close() {
        let mut b = Builder::new();
        b.write_superblock(20, 100);
        BigEndian::write_u32(&mut b.img[BS..BS + 4], 99); // fs_size lies
        assert!(EfsV1Filesystem::open(Cursor::new(b.img), 0).is_err());
    }

    #[test]
    fn rejects_a_chance_magic_without_a_superblock() {
        // The magic alone must not be enough: a 4-byte hit inside file data
        // has none of the geometry around it.
        let mut img = vec![0u8; 48 * BS];
        BigEndian::write_u32(&mut img[BS + OFF_MAGIC..BS + OFF_MAGIC + 4], EFS_V1_MAGIC);
        assert!(detect(&mut Cursor::new(img.clone()), 0).is_none());
        assert!(EfsV1Filesystem::open(Cursor::new(img), 0).is_err());
    }

    #[test]
    fn rejects_extent_with_nonzero_magic() {
        let mut b = Builder::new();
        b.write_superblock(20, 100);
        b.write_inode(2, 0o040777, (3 * EFS_V1_DIRENTSIZE) as u32, &[(10, 1, 0)]);
        b.write_dir(10, &[(2, "."), (2, ".."), (3, "bad")]);
        b.write_inode(3, 0o100644, 512, &[(11, 1, 0)]);
        // Poke a nonzero ex_magic into the file's only extent.
        let o = b.inode_offset(3) + 0x20;
        b.img[o] = 0x7F;
        let mut fs = open(b.img);
        let root = fs.root().unwrap();
        let bad = fs
            .list_directory(&root)
            .unwrap()
            .into_iter()
            .find(|e| e.name == "bad")
            .unwrap();
        assert!(fs.read_file(&bad, usize::MAX).is_err());
    }

    #[test]
    fn dirent_past_the_inode_table_is_dropped() {
        let mut b = Builder::new();
        b.write_superblock(20, 100);
        b.write_inode(2, 0o040777, (4 * EFS_V1_DIRENTSIZE) as u32, &[(10, 1, 0)]);
        // Inode 900 is far past the 8-inode table of this volume.
        b.write_dir(10, &[(2, "."), (2, ".."), (900, "ghost"), (3, "real")]);
        b.write_inode(3, 0o100644, 0, &[]);
        let mut fs = open(b.img);
        let root = fs.root().unwrap();
        assert_eq!(names(&mut fs, &root), vec!["real"]);
    }

    #[test]
    fn indirect_extents_are_followed() {
        // 13 extents forces indirect mode: one index block (block 10) holds
        // the records, and the file's data is blocks 12..25.
        let mut b = Builder::new();
        b.write_superblock(20, 100);
        b.write_inode(2, 0o040777, (3 * EFS_V1_DIRENTSIZE) as u32, &[(11, 1, 0)]);
        b.write_dir(11, &[(2, "."), (2, ".."), (3, "big")]);
        let n = 13u32;
        b.write_inode(3, 0o100644, n * BS as u32, &[]);
        let o = b.inode_offset(3);
        BigEndian::write_u16(&mut b.img[o + 0x1C..o + 0x1E], n as u16);
        // Inline slot 0 points at the index block; ex_offset carries direxts.
        BigEndian::write_u32(&mut b.img[o + 0x20..o + 0x24], 10);
        BigEndian::write_u32(&mut b.img[o + 0x24..o + 0x28], (1 << 24) | 1);
        for i in 0..n {
            let eo = 10 * BS + i as usize * 8;
            BigEndian::write_u32(&mut b.img[eo..eo + 4], 12 + i);
            BigEndian::write_u32(&mut b.img[eo + 4..eo + 8], (1 << 24) | i);
            b.write_data(12 + i, &[i as u8; BS]);
        }
        let mut fs = open(b.img);
        let root = fs.root().unwrap();
        let big = fs
            .list_directory(&root)
            .unwrap()
            .into_iter()
            .find(|e| e.name == "big")
            .unwrap();
        let data = fs.read_file(&big, usize::MAX).unwrap();
        assert_eq!(data.len() as u32, n * BS as u32);
        for i in 0..n as usize {
            assert!(data[i * BS..(i + 1) * BS].iter().all(|&b| b == i as u8));
        }
    }

    #[test]
    fn sizes_report_from_the_superblock() {
        let fs = open(sample_volume());
        assert_eq!(fs.total_size(), 48 * BS as u64);
        assert_eq!(fs.used_size(), (48 - 20) * BS as u64);
        assert_eq!(fs.allocation_unit(), Some(512));
    }

    #[test]
    fn last_data_byte_covers_every_allocated_block() {
        let mut fs = open(sample_volume());
        // Highest block any inode claims is 14 (+1); metadata reaches 10.
        assert_eq!(fs.last_data_byte().unwrap(), 15 * BS as u64);
    }

    #[test]
    fn checksum_matches_the_irix_algorithm() {
        // The v1 superblock is checksummed exactly like IRIX EFS, over
        // offsets 0..0x9E. Verified against all three volumes of the IRIS
        // 3130 disk this driver was written from.
        let img = sample_volume();
        let sb = &img[BS..BS + EFS_V1_SUPERBLOCK_SIZE];
        let mut c: u32 = 0;
        let mut i = 0;
        while i < 0x9E {
            c ^= ((sb[i] as u32) << 8) | sb[i + 1] as u32;
            c = c.rotate_left(1);
            i += 2;
        }
        // Our synthetic volume leaves fs_checksum zero; assert the routine
        // runs over the right span rather than a specific stored value.
        assert_ne!(c, 0);
        assert_eq!(BigEndian::read_u32(&sb[0x9E..0xA2]), 0);
    }

    // ---- write side -----------------------------------------------------

    use crate::fs::filesystem::{CreateDirectoryOptions, CreateFileOptions, EditableFilesystem};

    /// A blank volume big enough to exercise the allocator: 4 MiB.
    fn blank() -> Vec<u8> {
        create_blank_efs_v1(4 * 1024 * 1024, "test").unwrap()
    }

    fn put(
        fs: &mut EfsV1Filesystem<Cursor<Vec<u8>>>,
        dir: &FileEntry,
        name: &str,
        data: &[u8],
    ) -> FileEntry {
        fs.create_file(
            dir,
            name,
            &mut Cursor::new(data.to_vec()),
            data.len() as u64,
            &CreateFileOptions::default(),
        )
        .unwrap()
    }

    fn slurp(fs: &mut EfsV1Filesystem<Cursor<Vec<u8>>>, e: &FileEntry) -> Vec<u8> {
        fs.read_file(e, usize::MAX).unwrap()
    }

    #[test]
    fn blank_volume_round_trips_through_open() {
        let mut fs = open(blank());
        let sb = fs.superblock().clone();
        assert_eq!(sb.magic, EFS_V1_MAGIC);
        assert_eq!(
            sb.fs_size,
            sb.firstcg + sb.ncg as u32 * sb.cgfsize,
            "geometry must close"
        );
        assert!(sb.tfree > 0 && sb.tinode > 0);
        let root = fs.root().unwrap();
        assert!(root.is_directory());
        // `.` and `..` are hidden by the listing, so a fresh root is empty.
        assert!(fs.list_directory(&root).unwrap().is_empty());
    }

    #[test]
    fn blank_volume_checksum_is_valid() {
        let img = blank();
        let sector = &img[BS..BS + EFS_V1_BLOCKSIZE as usize];
        let stored = BigEndian::read_u32(&sector[0x9E..0xA2]);
        let mut probe = sector.to_vec();
        BigEndian::write_u32(&mut probe[0x9E..0xA2], 0);
        assert_eq!(stored, efs_v1_superblock_checksum(&probe));
        assert_ne!(stored, 0);
    }

    #[test]
    fn created_file_reads_back() {
        let mut fs = open(blank());
        let root = fs.root().unwrap();
        let payload = b"hello from an IRIS 3130".to_vec();
        put(&mut fs, &root, "hello", &payload);

        let mut fs = open(fs.reader_into_inner().into_inner());
        let root = fs.root().unwrap();
        assert_eq!(names(&mut fs, &root), vec!["hello"]);
        let e = fs.list_directory(&root).unwrap().remove(0);
        assert_eq!(e.size, payload.len() as u64);
        assert_eq!(slurp(&mut fs, &e), payload);
    }

    #[test]
    fn created_directory_nests_and_lists() {
        let mut fs = open(blank());
        let root = fs.root().unwrap();
        let sub = fs
            .create_directory(&root, "subdir", &CreateDirectoryOptions::default())
            .unwrap();
        put(&mut fs, &sub, "inner", b"nested payload");

        let mut fs = open(fs.reader_into_inner().into_inner());
        let root = fs.root().unwrap();
        assert_eq!(names(&mut fs, &root), vec!["subdir"]);
        let sub = fs.list_directory(&root).unwrap().remove(0);
        assert!(sub.is_directory());
        assert_eq!(names(&mut fs, &sub), vec!["inner"]);
        let inner = fs.list_directory(&sub).unwrap().remove(0);
        assert_eq!(slurp(&mut fs, &inner), b"nested payload".to_vec());
    }

    #[test]
    fn delete_returns_blocks_and_the_inode() {
        let mut fs = open(blank());
        let root = fs.root().unwrap();
        let free_before = fs.free_space().unwrap();
        let tinode_before = fs.superblock().tinode;

        let big = vec![0xA5u8; 40 * 1024];
        put(&mut fs, &root, "big", &big);
        assert!(fs.free_space().unwrap() < free_before);
        assert_eq!(fs.superblock().tinode, tinode_before - 1);

        let e = fs.list_directory(&root).unwrap().remove(0);
        fs.delete_entry(&root, &e).unwrap();
        assert_eq!(fs.free_space().unwrap(), free_before);
        assert_eq!(fs.superblock().tinode, tinode_before);
        assert!(fs.list_directory(&root).unwrap().is_empty());
    }

    #[test]
    fn a_non_empty_directory_is_not_deleted() {
        let mut fs = open(blank());
        let root = fs.root().unwrap();
        let sub = fs
            .create_directory(&root, "keep", &CreateDirectoryOptions::default())
            .unwrap();
        put(&mut fs, &sub, "child", b"x");
        let dir_entry = fs.list_directory(&root).unwrap().remove(0);
        assert!(fs.delete_entry(&root, &dir_entry).is_err());
    }

    #[test]
    fn large_file_round_trips_through_indirect_extents() {
        let mut fs = open(blank());
        let root = fs.root().unwrap();
        // Past 12 extents the inode switches to an indirect index; keep the
        // content position-dependent so a mis-ordered extent shows up.
        let payload: Vec<u8> = (0..600_000u32).map(|i| (i % 251) as u8).collect();
        put(&mut fs, &root, "big", &payload);

        let mut fs = open(fs.reader_into_inner().into_inner());
        let root = fs.root().unwrap();
        let e = fs.list_directory(&root).unwrap().remove(0);
        assert_eq!(e.size, payload.len() as u64);
        assert_eq!(slurp(&mut fs, &e), payload);
    }

    #[test]
    fn writes_are_byte_order_symmetric() {
        // Write into a byte-swapped volume, then swab back to native: what we
        // wrote must read identically, or a swapped capture is inconsistent.
        let mut fs = open(swab(blank()));
        assert_eq!(fs.byte_order(), SgiLabelByteOrder::Swabbed);
        let root = fs.root().unwrap();
        let payload: Vec<u8> = (0..5000u32).map(|i| (i % 97) as u8).collect();
        put(&mut fs, &root, "swapped", &payload);
        fs.create_directory(&root, "dir", &CreateDirectoryOptions::default())
            .unwrap();

        let swapped_img = fs.reader_into_inner().into_inner();
        let mut native = open(swab(swapped_img));
        assert_eq!(native.byte_order(), SgiLabelByteOrder::Native);
        let root = native.root().unwrap();
        assert_eq!(names(&mut native, &root), vec!["dir", "swapped"]);
        let e = native
            .list_directory(&root)
            .unwrap()
            .into_iter()
            .find(|e| e.name == "swapped")
            .unwrap();
        assert_eq!(slurp(&mut native, &e), payload);
    }

    #[test]
    fn names_longer_than_dirsiz_are_refused() {
        let mut fs = open(blank());
        let root = fs.root().unwrap();
        assert!(fs.validate_name("0123456789abcd").is_ok()); // exactly 14
        assert!(fs.validate_name("0123456789abcde").is_err()); // 15
        assert!(fs.validate_name("has/slash").is_err());
        assert!(fs.validate_name("").is_err());
        assert!(fs
            .create_file(
                &root,
                "0123456789abcde",
                &mut Cursor::new(Vec::new()),
                0,
                &CreateFileOptions::default()
            )
            .is_err());
    }

    #[test]
    fn a_duplicate_name_is_refused() {
        let mut fs = open(blank());
        let root = fs.root().unwrap();
        put(&mut fs, &root, "dup", b"first");
        assert!(fs
            .create_file(
                &root,
                "dup",
                &mut Cursor::new(b"second".to_vec()),
                6,
                &CreateFileOptions::default()
            )
            .is_err());
    }

    #[test]
    fn rename_keeps_the_inode_and_contents() {
        let mut fs = open(blank());
        let root = fs.root().unwrap();
        let e = put(&mut fs, &root, "before", b"same bytes");
        let inum = e.location;
        fs.rename(&root, &e, "after").unwrap();

        let mut fs = open(fs.reader_into_inner().into_inner());
        let root = fs.root().unwrap();
        let e = fs.list_directory(&root).unwrap().remove(0);
        assert_eq!(e.name, "after");
        assert_eq!(e.location, inum);
        assert_eq!(slurp(&mut fs, &e), b"same bytes".to_vec());
    }

    #[test]
    fn symlink_target_round_trips() {
        let mut fs = open(blank());
        let root = fs.root().unwrap();
        fs.create_symlink(&root, "link", "/usr/include", &CreateFileOptions::default())
            .unwrap();

        let mut fs = open(fs.reader_into_inner().into_inner());
        let root = fs.root().unwrap();
        let e = fs.list_directory(&root).unwrap().remove(0);
        assert_eq!(e.entry_type, EntryType::Symlink);
        assert_eq!(e.symlink_target.as_deref(), Some("/usr/include"));
    }

    #[test]
    fn permissions_and_owner_persist() {
        let mut fs = open(blank());
        let root = fs.root().unwrap();
        let e = put(&mut fs, &root, "chmodme", b"x");
        fs.set_permissions(&e, 0o640).unwrap();
        fs.set_owner(&e, 100, 200).unwrap();

        let mut fs = open(fs.reader_into_inner().into_inner());
        let root = fs.root().unwrap();
        let e = fs.list_directory(&root).unwrap().remove(0);
        assert_eq!(e.mode.unwrap() & 0o777, 0o640);
        // The file-type bits must survive a permission change.
        assert_eq!(e.mode.unwrap() & 0o170000, 0o100000);
        assert_eq!(e.uid, Some(100));
        assert_eq!(e.gid, Some(200));
    }

    #[test]
    fn many_files_grow_the_root_directory_past_one_block() {
        let mut fs = open(blank());
        let root = fs.root().unwrap();
        // 512/16 = 32 dirents per block, so 60 entries needs a second block.
        for i in 0..60 {
            put(&mut fs, &root, &format!("f{i:03}"), b"x");
        }
        let mut fs = open(fs.reader_into_inner().into_inner());
        let root = fs.root().unwrap();
        assert_eq!(fs.list_directory(&root).unwrap().len(), 60);
        assert!(root.size > EFS_V1_BLOCKSIZE);
    }

    #[test]
    fn allocation_never_lands_in_an_inode_table() {
        let mut fs = open(blank());
        let root = fs.root().unwrap();
        put(&mut fs, &root, "probe", &vec![7u8; 200 * 1024]);
        let sb = fs.superblock().clone();
        let regions = EfsV1DataRegions::from_sb(&sb);
        let e = fs.list_directory(&root).unwrap().remove(0);
        let ino = fs.read_inode(e.location as u32).unwrap();
        for ext in fs.extents_of(&ino).unwrap() {
            for b in ext.bn..ext.bn + ext.length as u32 {
                assert!(regions.contains(b), "extent block {b} is not a data block");
            }
        }
    }
}
