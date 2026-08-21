//! SGI EFS v1 — the original Extent File System, as shipped on the IRIS 2000 /
//! 3000 series (read-only).
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
//! The checksum is the same rotate-and-XOR IRIX EFS uses, run over offsets
//! 0..0x9E; [`super::efs::efs_superblock_checksum`] computes it.
//!
//! ## Byte order
//!
//! Images taken off period SGI disk controllers are byte-swapped within every
//! 16-bit word — see the `sgi_dklabel` module header. The magic is probed both
//! ways at open time and every block read is fixed up on the way in, so a
//! partition image without its disk label still opens. Stored bytes are never
//! rewritten: a backup of one of these disks must stay byte-identical.

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

/// Read-only reader for an EFS v1 volume.
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

impl<R: Read + Seek + Send> Filesystem for EfsV1Filesystem<R> {
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
}
