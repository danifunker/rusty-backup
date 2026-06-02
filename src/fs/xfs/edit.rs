//! XFS write primitives (§3 of `docs/xfs_edit_and_repair.md`) — the shared
//! foundation for Track-B editing (`create_directory` here) and R7 orphan
//! reconnection. Everything is **v4-only** (no inode/dir CRCs to recompute)
//! and writes straight through to the device (no in-memory staging), like the
//! repair path.
//!
//! This first slice covers the two primitives a short-form `create_directory`
//! needs — and no more:
//!
//!   1. **Inode-slot allocation** ([`alloc_inode_slot`]): claim a free inode
//!      from an existing inode-btree chunk (flip its `ir_free` bit, decrement
//!      the record freecount + AGI freecount + `sb_ifree`). Allocating a *new*
//!      64-inode chunk when none has a free slot is deferred.
//!   2. **Short-form directory entry insertion** ([`sf_insert_entry`]): append
//!      an entry to an inline directory, computing the dir2 offset cookie the
//!      way `libxfs` does, updating `di_size`/count and the parent's link
//!      count. Conversion to block form when the inline fork overflows is
//!      deferred (returns `DiskFull`).
//!
//! Porting reference: `xfs_repair` @ v3.1.11 + `libxfs/xfs_dir2_sf.c`,
//! `libxfs/xfs_ialloc.c`.

use std::io::{Read, Seek, Write};

use byteorder::{BigEndian, ByteOrder};

use super::ag::XfsAgi;
use super::dir2::XFS_DIR2_DATA_HDR_LEN;
use super::inode::fork_offset;
use super::{read_at_aligned, XfsFilesystem};
use crate::fs::filesystem::FilesystemError;

/// dir2 filetype values (only the two we emit).
const XFS_DIR3_FT_REG_FILE: u8 = 1;
const XFS_DIR3_FT_DIR: u8 = 2;

/// NULL agino sentinel stored in `di_next_unlinked` for a not-unlinked inode.
const NULLAGINO: u32 = 0xFFFF_FFFF;

/// inobt leaf record layout (v4): startino(4) freecount(4) free(8).
const INOBT_REC_SIZE: usize = 16;
const INOBT_HDR_LEN: usize = 16;

/// AGI / superblock counter offsets.
const AGI_FREECOUNT_OFF: usize = 28;
const SB_IFREE_OFF: usize = 136;

/// `xfs_dir2_data_entsize`: the space one entry occupies in the notional data
/// block — `inumber(8) + namelen(1) + name + [ftype] + tag(2)`, rounded up to 8.
/// The short-form *offset cookie* of each entry is its byte offset in that
/// block, so this is how we advance the cookie when appending.
fn data_entsize(namelen: usize, has_ftype: bool) -> usize {
    let raw = 11 + namelen + usize::from(has_ftype);
    raw.div_ceil(8) * 8
}

impl<R: Read + Write + Seek + Send> XfsFilesystem<R> {
    /// Allocate one inode from a free slot in an existing inode-btree chunk.
    /// Returns its inode number. Errors with `DiskFull` when every chunk is
    /// full (allocating a new chunk is not implemented). v4 only.
    ///
    /// Flips the chunk record's `ir_free` bit for the chosen slot, decrements
    /// the record's freecount, the AGI freecount, and `sb_ifree`. The caller is
    /// responsible for initializing the dinode (it is left in its on-disk free
    /// state until then).
    pub(crate) fn alloc_inode_slot(
        &mut self,
        sb: &super::sb::XfsSuperblock,
    ) -> Result<u64, FilesystemError> {
        if sb.is_v5() {
            return Err(FilesystemError::Unsupported(
                "XFS v5 inode allocation not supported (CRC)".into(),
            ));
        }
        let sectsize = sb.sectsize as u64;
        let agblocks = sb.agblocks as u64;
        let blocksize = sb.blocksize as u64;
        let bs = sb.blocksize as usize;
        let ino_shift = sb.agblklog + sb.inopblog;

        for agno in 0..sb.agcount as u64 {
            let agi_byte = self.partition_offset + agno * agblocks * blocksize + 2 * sectsize;
            let mut agi_sec = vec![0u8; sectsize as usize];
            if read_at_aligned(&mut self.reader, agi_byte, sectsize, &mut agi_sec).is_err() {
                continue;
            }
            let agi = match XfsAgi::parse(&agi_sec) {
                Ok(a) => a,
                Err(_) => continue,
            };
            if agi.freecount == 0 {
                continue;
            }
            let leaves = match self.collect_inobt_leaf_blocks(sb, agno, agi.root) {
                Ok(l) => l,
                Err(_) => continue,
            };
            for leaf_agbno in leaves {
                let fsblock = (agno << sb.agblklog) | leaf_agbno as u64;
                let mut block = vec![0u8; bs];
                if self.read_fsblock(fsblock, &mut block).is_err() {
                    continue;
                }
                let numrecs = BigEndian::read_u16(&block[6..8]) as usize;
                let max_leaf = (bs - INOBT_HDR_LEN) / INOBT_REC_SIZE;
                if numrecs > max_leaf {
                    continue;
                }
                for r in 0..numrecs {
                    let off = INOBT_HDR_LEN + r * INOBT_REC_SIZE;
                    let start_agino = BigEndian::read_u32(&block[off..off + 4]) as u64;
                    let freecount = BigEndian::read_u32(&block[off + 4..off + 8]);
                    let free = BigEndian::read_u64(&block[off + 8..off + 16]);
                    if freecount == 0 || free == 0 {
                        continue;
                    }
                    let slot = free.trailing_zeros() as u64; // lowest free slot
                    let ino = (agno << ino_shift) | (start_agino + slot);

                    // Mark allocated: clear the bit, decrement record freecount.
                    let new_free = free & !(1u64 << slot);
                    BigEndian::write_u32(&mut block[off + 4..off + 8], freecount - 1);
                    BigEndian::write_u64(&mut block[off + 8..off + 16], new_free);
                    self.write_fsblock(sb, fsblock, &block)?;

                    // AGI freecount-- and sb_ifree--.
                    BigEndian::write_u32(
                        &mut agi_sec[AGI_FREECOUNT_OFF..AGI_FREECOUNT_OFF + 4],
                        agi.freecount - 1,
                    );
                    self.write_at(agi_byte, &agi_sec)?;
                    self.bump_sb_ifree(sb, -1)?;
                    return Ok(ino);
                }
            }
        }
        Err(FilesystemError::DiskFull(
            "no free inode slots; new-chunk allocation not implemented".into(),
        ))
    }

    /// Initialize a freshly-allocated inode as an empty short-form directory
    /// whose parent is `parent_ino`. Preserves the on-disk inode version and
    /// bumps the generation; writes a clean v4 dinode core + the 6-byte inline
    /// fork (`count=0, i8count=0, parent`). `nlink` is 2 (the new directory's
    /// own `.` plus its entry in the parent); no subdirectories yet.
    pub(crate) fn init_empty_shortform_dir(
        &mut self,
        sb: &super::sb::XfsSuperblock,
        ino: u64,
        parent_ino: u64,
        mode: u16,
        uid: u32,
        gid: u32,
    ) -> Result<(), FilesystemError> {
        if parent_ino > u64::from(u32::MAX) || ino > u64::from(u32::MAX) {
            // Inode numbers beyond 32 bits need the 8-byte short-form layout
            // (i8count); our small-volume target never hits this.
            return Err(FilesystemError::Unsupported(
                "XFS short-form dir with 8-byte inode numbers not supported".into(),
            ));
        }
        let (_core, mut buf) = self.read_inode_buf(ino)?;
        let version = buf[4];
        let gen = BigEndian::read_u32(&buf[92..96]).wrapping_add(1);
        let isz = buf.len();

        // Clean v4 dinode core.
        BigEndian::write_u16(&mut buf[0..2], super::types::XFS_DINODE_MAGIC);
        BigEndian::write_u16(&mut buf[2..4], mode);
        buf[4] = version;
        buf[5] = 1; // di_format = local
                    // di_onlink (v1 nlink) / di_nlink (v2 nlink).
        if version == 1 {
            BigEndian::write_u16(&mut buf[6..8], 2);
            BigEndian::write_u32(&mut buf[16..20], 0);
        } else {
            BigEndian::write_u16(&mut buf[6..8], 0);
            BigEndian::write_u32(&mut buf[16..20], 2);
        }
        BigEndian::write_u32(&mut buf[8..12], uid);
        BigEndian::write_u32(&mut buf[12..16], gid);
        for b in buf.iter_mut().take(56).skip(20) {
            *b = 0; // projid, pad, atime, mtime, ctime
        }
        BigEndian::write_u64(&mut buf[56..64], 6); // di_size = empty sf fork len
        for b in buf.iter_mut().take(92).skip(64) {
            *b = 0; // nblocks, extsize, nextents, anextents, forkoff, aformat, dm*, flags
        }
        buf[83] = 2; // di_aformat = extents (no attr fork)
        BigEndian::write_u32(&mut buf[92..96], gen);
        BigEndian::write_u32(&mut buf[96..100], NULLAGINO); // di_next_unlinked

        // Inline fork: count=0, i8count=0, parent (4 bytes).
        let fs = fork_offset(false);
        for b in buf.iter_mut().take(isz).skip(fs) {
            *b = 0;
        }
        buf[fs] = 0; // count
        buf[fs + 1] = 0; // i8count
        BigEndian::write_u32(&mut buf[fs + 2..fs + 6], parent_ino as u32);

        self.write_inode_region(sb, ino, &buf)
    }

    /// Append an entry `(name -> child_ino)` to short-form directory
    /// `parent_ino`, fixing `di_size`/count and bumping the parent's link count
    /// when `is_dir`. Errors with `DiskFull` when the entry won't fit the inline
    /// fork (block-form conversion is deferred) or `AlreadyExists` on a dup name.
    pub(crate) fn sf_insert_entry(
        &mut self,
        sb: &super::sb::XfsSuperblock,
        parent_ino: u64,
        name: &str,
        child_ino: u64,
        is_dir: bool,
    ) -> Result<(), FilesystemError> {
        let has_ftype = sb.has_ftype();
        let (core, mut buf) = self.read_inode_buf(parent_ino)?;
        if !core.is_dir() || core.format != super::types::DiFormat::Local {
            return Err(FilesystemError::Unsupported(
                "parent is not a short-form directory".into(),
            ));
        }
        if child_ino > u64::from(u32::MAX) {
            return Err(FilesystemError::Unsupported(
                "XFS short-form dir with 8-byte inode numbers not supported".into(),
            ));
        }
        let namelen = name.len();
        if namelen == 0 || namelen > 255 {
            return Err(FilesystemError::InvalidData("bad entry name length".into()));
        }

        let fs = fork_offset(false);
        let fork_len = core.size as usize;
        let fork_end = fs + fork_len;
        if fork_end > buf.len() {
            return Err(FilesystemError::Parse(
                "short-form fork overflows inode".into(),
            ));
        }

        // Reject a duplicate name, then pick the append offset cookie from the
        // raw fork (so removal gaps are respected).
        let (_parent, entries) = super::dir2::parse_shortform(&buf[fs..fork_end], has_ftype)?;
        if entries.iter().any(|e| e.name == name) {
            return Err(FilesystemError::AlreadyExists(name.to_string()));
        }
        let new_off = self.sf_next_offset(&buf[fs..fork_end], has_ftype)?;

        // Build the new entry: namelen(1) offset(2) name[namelen] [ftype] ino(4).
        let ino_width = 4usize; // i8count == 0 path only
        let entry_len = 1 + 2 + namelen + usize::from(has_ftype) + ino_width;
        let new_fork_len = fork_len + entry_len;

        // Fit check against the literal area (bounded by an attr fork if any).
        let avail_end = if core.forkoff > 0 {
            fs + (core.forkoff as usize) * 8
        } else {
            buf.len()
        };
        if fs + new_fork_len > avail_end {
            return Err(FilesystemError::DiskFull(
                "short-form directory full (block-form conversion not implemented)".into(),
            ));
        }

        // Append the entry bytes after the existing fork, bump the count.
        let mut p = fork_end;
        buf[p] = namelen as u8;
        BigEndian::write_u16(&mut buf[p + 1..p + 3], new_off as u16);
        p += 3;
        buf[p..p + namelen].copy_from_slice(name.as_bytes());
        p += namelen;
        if has_ftype {
            buf[p] = if is_dir {
                XFS_DIR3_FT_DIR
            } else {
                XFS_DIR3_FT_REG_FILE
            };
            p += 1;
        }
        BigEndian::write_u32(&mut buf[p..p + ino_width], child_ino as u32);

        // Header count++ and di_size.
        buf[fs] = buf[fs]
            .checked_add(1)
            .ok_or_else(|| FilesystemError::DiskFull("short-form entry count overflow".into()))?;
        BigEndian::write_u64(&mut buf[56..64], new_fork_len as u64);

        // Parent link count: a new subdirectory adds a ".." back-link.
        if is_dir {
            let version = buf[4];
            if version == 1 {
                let n = BigEndian::read_u16(&buf[6..8]).wrapping_add(1);
                BigEndian::write_u16(&mut buf[6..8], n);
            } else {
                let n = BigEndian::read_u32(&buf[16..20]).wrapping_add(1);
                BigEndian::write_u32(&mut buf[16..20], n);
            }
        }

        self.write_inode_region(sb, parent_ino, &buf)
    }

    /// The offset cookie to assign a newly-appended short-form entry: the
    /// maximum of (each existing entry's cookie + its data-block size) and the
    /// post-`.`/`..` base. Walks the raw fork so gaps left by removals are
    /// respected.
    fn sf_next_offset(&self, fork: &[u8], has_ftype: bool) -> Result<u32, FilesystemError> {
        let mut next = (XFS_DIR2_DATA_HDR_LEN
            + data_entsize(1, has_ftype)
            + data_entsize(2, has_ftype)) as u32;
        if fork.len() < 6 {
            return Err(FilesystemError::Parse("short-form fork too small".into()));
        }
        let count = fork[0] as usize;
        let i8count = fork[1];
        let ino_width = if i8count > 0 { 8 } else { 4 };
        let mut pos = 2 + ino_width;
        for _ in 0..count {
            if pos + 3 > fork.len() {
                return Err(FilesystemError::Parse("short-form entry truncated".into()));
            }
            let namelen = fork[pos] as usize;
            let off = BigEndian::read_u16(&fork[pos + 1..pos + 3]) as u32;
            let end = off + data_entsize(namelen, has_ftype) as u32;
            next = next.max(end);
            pos += 3 + namelen + usize::from(has_ftype) + ino_width;
        }
        Ok(next)
    }

    /// Write a buffer at an absolute partition byte offset, sector-aligned.
    fn write_at(&mut self, byte_off: u64, data: &[u8]) -> Result<(), FilesystemError> {
        use std::io::SeekFrom;
        self.reader.seek(SeekFrom::Start(byte_off))?;
        self.reader.write_all(data)?;
        Ok(())
    }

    /// Adjust `sb_ifree` by `delta` in the primary superblock.
    fn bump_sb_ifree(
        &mut self,
        sb: &super::sb::XfsSuperblock,
        delta: i64,
    ) -> Result<(), FilesystemError> {
        let sectsize = sb.sectsize as u64;
        let mut primary = vec![0u8; sectsize as usize];
        read_at_aligned(
            &mut self.reader,
            self.partition_offset,
            sectsize,
            &mut primary,
        )?;
        let cur = BigEndian::read_u64(&primary[SB_IFREE_OFF..SB_IFREE_OFF + 8]);
        let new = (cur as i64 + delta).max(0) as u64;
        BigEndian::write_u64(&mut primary[SB_IFREE_OFF..SB_IFREE_OFF + 8], new);
        self.write_at(self.partition_offset, &primary)
    }

    /// Internal entry point for `create_directory`: allocate an inode, init it
    /// as an empty short-form directory, and insert it into `parent_ino`.
    /// Returns the new directory's inode number. The fit check runs before any
    /// write, so a `DiskFull` parent never leaves a dangling allocated inode.
    pub(crate) fn do_create_directory(
        &mut self,
        parent_ino: u64,
        name: &str,
        mode: u16,
        uid: u32,
        gid: u32,
    ) -> Result<u64, FilesystemError> {
        let sb = self.superblock().clone();

        // Pre-flight: the parent must be a short-form dir with room for the
        // entry, and the name must be unique — checked before allocating so a
        // failure leaves the volume unchanged.
        self.sf_check_insertable(&sb, parent_ino, name)?;

        let ino = self.alloc_inode_slot(&sb)?;
        self.init_empty_shortform_dir(&sb, ino, parent_ino, mode, uid, gid)?;
        self.sf_insert_entry(&sb, parent_ino, name, ino, true)?;
        self.reader.flush()?;
        Ok(ino)
    }

    /// Validate that `name` can be inserted into short-form directory
    /// `parent_ino` (parent is a short-form dir, name unique, room in the inline
    /// fork). Pure check — no writes.
    fn sf_check_insertable(
        &mut self,
        sb: &super::sb::XfsSuperblock,
        parent_ino: u64,
        name: &str,
    ) -> Result<(), FilesystemError> {
        let has_ftype = sb.has_ftype();
        let (core, buf) = self.read_inode_buf(parent_ino)?;
        if !core.is_dir() || core.format != super::types::DiFormat::Local {
            return Err(FilesystemError::Unsupported(
                "parent is not a short-form directory".into(),
            ));
        }
        let namelen = name.len();
        if namelen == 0 || namelen > 255 {
            return Err(FilesystemError::InvalidData("bad entry name length".into()));
        }
        let fs = fork_offset(false);
        let fork_end = fs + core.size as usize;
        let (_parent, entries) = super::dir2::parse_shortform(&buf[fs..fork_end], has_ftype)?;
        if entries.iter().any(|e| e.name == name) {
            return Err(FilesystemError::AlreadyExists(name.to_string()));
        }
        let entry_len = 1 + 2 + namelen + usize::from(has_ftype) + 4;
        let avail_end = if core.forkoff > 0 {
            fs + (core.forkoff as usize) * 8
        } else {
            buf.len()
        };
        if fork_end + entry_len > avail_end {
            return Err(FilesystemError::DiskFull(
                "short-form directory full (block-form conversion not implemented)".into(),
            ));
        }
        Ok(())
    }
}
