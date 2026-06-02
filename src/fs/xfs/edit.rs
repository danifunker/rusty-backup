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

use super::ag::{XfsAgf, XfsAgi};
use super::bmap::{encode_extent, fsblock_to_partition_byte};
use super::btree_build::FreeExtent;
use super::dir2::XFS_DIR2_DATA_HDR_LEN;
use super::freespace_rebuild::{carve_from_largest, coalesce};
use super::inode::fork_offset;
use super::types::{XFS_ABTB_MAGIC, XFS_ABTC_MAGIC, XFS_INODES_PER_CHUNK};
use super::{read_at_aligned, XfsFilesystem};
use crate::fs::filesystem::FilesystemError;

/// Largest single-extent file we write: the on-disk blockcount field is 21 bits.
const MAX_SINGLE_EXTENT_BLOCKS: u64 = (1 << 21) - 1;

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

    // ----- block allocation + create_file -----------------------------------

    /// Allocate `n` **contiguous** free blocks, returning their starting fsblock.
    /// Carves the run off the tail of an AG's largest free extent and rebuilds
    /// that AG's free-space btrees over the remainder (reusing the R2 rebuild),
    /// then resyncs `sb_fdblocks`. Errors `DiskFull` when no AG has a single
    /// free extent large enough (no multi-extent split yet). v4 only.
    pub(crate) fn alloc_blocks(
        &mut self,
        sb: &super::sb::XfsSuperblock,
        n: u32,
    ) -> Result<u64, FilesystemError> {
        if n == 0 {
            return Err(FilesystemError::InvalidData("alloc_blocks(0)".into()));
        }
        let agblocks = sb.agblocks as u64;
        for agno in 0..sb.agcount as u64 {
            let expected_len = if agno == sb.agcount as u64 - 1 {
                sb.dblocks - agno * agblocks
            } else {
                agblocks
            };
            let full_free = match self.current_full_free(sb, agno, expected_len) {
                Ok(f) => f,
                Err(_) => continue,
            };
            // Carve the data run off the largest extent's tail (contiguous,
            // extent-count preserving). Leaves room for the btree rebuild's own
            // carve from the remainder.
            let Some((carved, free_after)) = carve_from_largest(&full_free, n) else {
                continue;
            };
            // Rebuild bno/cnt over the remaining free set + rewrite the AGF.
            // Fails if the remainder can't host the btrees — try the next AG.
            if self
                .rebuild_ag_freespace(sb, agno, expected_len, &free_after)
                .is_err()
            {
                continue;
            }
            self.resync_sb_fdblocks(sb)?;
            let start_agbno = carved[0] as u64;
            return Ok((agno << sb.agblklog) | start_agbno);
        }
        Err(FilesystemError::DiskFull(
            "no allocation group has a single free extent large enough".into(),
        ))
    }

    /// The AG's complete free-block set (AG-relative extents): the bnobt's free
    /// records plus the blocks both space btrees themselves occupy (which a
    /// rebuild reclaims), coalesced — the same reconstruction R2 uses.
    fn current_full_free(
        &mut self,
        sb: &super::sb::XfsSuperblock,
        agno: u64,
        expected_len: u64,
    ) -> Result<Vec<FreeExtent>, FilesystemError> {
        let sectsize = sb.sectsize as u64;
        let agblocks = sb.agblocks as u64;
        let blocksize = sb.blocksize as u64;
        let agf_byte = self.partition_offset + agno * agblocks * blocksize + sectsize;
        let mut agf_sec = vec![0u8; sectsize as usize];
        read_at_aligned(&mut self.reader, agf_byte, sectsize, &mut agf_sec)?;
        let agf = XfsAgf::parse(&agf_sec)?;
        let (recs, bno_blocks) =
            self.walk_alloc_tree(sb, agno, agf.bno_root, expected_len, XFS_ABTB_MAGIC)?;
        let (_cnt_recs, cnt_blocks) =
            self.walk_alloc_tree(sb, agno, agf.cnt_root, expected_len, XFS_ABTC_MAGIC)?;
        let mut all = recs;
        for agbno in bno_blocks.into_iter().chain(cnt_blocks) {
            all.push(FreeExtent {
                startblock: agbno,
                blockcount: 1,
            });
        }
        Ok(coalesce(all))
    }

    /// Write `data_len` bytes from `data` into `[fsblock, fsblock+count)`,
    /// zero-padding the rest of the final block.
    fn write_file_data(
        &mut self,
        sb: &super::sb::XfsSuperblock,
        fsblock: u64,
        count: u32,
        data: &mut dyn Read,
        data_len: u64,
    ) -> Result<(), FilesystemError> {
        use std::io::SeekFrom;
        let bs = sb.blocksize as u64;
        let span = count as u64 * bs;
        let mut buf = vec![0u8; span as usize];
        let want = data_len.min(span) as usize;
        let mut filled = 0usize;
        while filled < want {
            let nr = data.read(&mut buf[filled..want])?;
            if nr == 0 {
                break;
            }
            filled += nr;
        }
        let part_byte = fsblock_to_partition_byte(fsblock, sb.agblocks, sb.agblklog, sb.blocksize);
        self.reader
            .seek(SeekFrom::Start(self.partition_offset + part_byte))?;
        self.reader.write_all(&buf)?;
        Ok(())
    }

    /// Initialize a freshly-allocated inode as a regular file. `extent` is the
    /// single data-fork extent `(fsblock, count)` (`None` for an empty file).
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn init_file_inode(
        &mut self,
        sb: &super::sb::XfsSuperblock,
        ino: u64,
        mode: u16,
        uid: u32,
        gid: u32,
        size: u64,
        extent: Option<(u64, u32)>,
    ) -> Result<(), FilesystemError> {
        let (_core, mut buf) = self.read_inode_buf(ino)?;
        let version = buf[4];
        let gen = BigEndian::read_u32(&buf[92..96]).wrapping_add(1);
        let isz = buf.len();

        BigEndian::write_u16(&mut buf[0..2], super::types::XFS_DINODE_MAGIC);
        BigEndian::write_u16(&mut buf[2..4], mode);
        buf[4] = version;
        buf[5] = 2; // di_format = extents
        if version == 1 {
            BigEndian::write_u16(&mut buf[6..8], 1);
            BigEndian::write_u32(&mut buf[16..20], 0);
        } else {
            BigEndian::write_u16(&mut buf[6..8], 0);
            BigEndian::write_u32(&mut buf[16..20], 1);
        }
        BigEndian::write_u32(&mut buf[8..12], uid);
        BigEndian::write_u32(&mut buf[12..16], gid);
        for b in buf.iter_mut().take(56).skip(20) {
            *b = 0;
        }
        BigEndian::write_u64(&mut buf[56..64], size);
        let (nblocks, nextents) = match extent {
            Some((_, count)) => (count as u64, 1u32),
            None => (0, 0),
        };
        BigEndian::write_u64(&mut buf[64..72], nblocks);
        for b in buf.iter_mut().take(76).skip(72) {
            *b = 0; // extsize
        }
        BigEndian::write_u32(&mut buf[76..80], nextents);
        for b in buf.iter_mut().take(92).skip(80) {
            *b = 0; // anextents, forkoff, aformat(set below), dm*, flags
        }
        buf[83] = 2; // di_aformat = extents (no attr fork)
        BigEndian::write_u32(&mut buf[92..96], gen);
        BigEndian::write_u32(&mut buf[96..100], NULLAGINO);

        let fs = fork_offset(false);
        for b in buf.iter_mut().take(isz).skip(fs) {
            *b = 0;
        }
        if let Some((fsblock, count)) = extent {
            let rec = encode_extent(false, 0, fsblock, count as u64);
            buf[fs..fs + 16].copy_from_slice(&rec);
        }
        self.write_inode_region(sb, ino, &buf)
    }

    /// Internal entry point for `create_file`: pre-flight the parent insert,
    /// allocate the data blocks (one contiguous extent) and write the data,
    /// allocate + initialize the file inode, then insert it into the parent.
    /// Returns the new file's inode number.
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn do_create_file(
        &mut self,
        parent_ino: u64,
        name: &str,
        data: &mut dyn Read,
        data_len: u64,
        mode: u16,
        uid: u32,
        gid: u32,
    ) -> Result<u64, FilesystemError> {
        let sb = self.superblock().clone();
        self.sf_check_insertable(&sb, parent_ino, name)?;

        let bs = sb.blocksize as u64;
        let nblocks = data_len.div_ceil(bs);
        if nblocks > MAX_SINGLE_EXTENT_BLOCKS {
            return Err(FilesystemError::Unsupported(format!(
                "file needs {nblocks} blocks; multi-extent files not implemented"
            )));
        }

        // Allocate data first (so a DiskFull leaves no half-built inode), then
        // the inode, then write data + init + link.
        let extent = if nblocks > 0 {
            let fsblock = self.alloc_blocks(&sb, nblocks as u32)?;
            Some((fsblock, nblocks as u32))
        } else {
            None
        };
        let ino = self.alloc_inode_slot(&sb)?;
        if let Some((fsblock, count)) = extent {
            self.write_file_data(&sb, fsblock, count, data, data_len)?;
        }
        self.init_file_inode(&sb, ino, mode, uid, gid, data_len, extent)?;
        self.sf_insert_entry(&sb, parent_ino, name, ino, false)?;
        self.reader.flush()?;
        Ok(ino)
    }

    // ----- delete_entry ------------------------------------------------------

    /// Remove `name` from short-form directory `parent_ino`, rebuilding the
    /// inline fork without it (count--, di_size), and decrementing the parent
    /// link count when the removed child was a subdirectory. Errors `NotFound`
    /// if the name isn't present.
    fn sf_remove_entry(
        &mut self,
        sb: &super::sb::XfsSuperblock,
        parent_ino: u64,
        name: &str,
        child_was_dir: bool,
    ) -> Result<(), FilesystemError> {
        let has_ftype = sb.has_ftype();
        let (core, mut buf) = self.read_inode_buf(parent_ino)?;
        if !core.is_dir() || core.format != super::types::DiFormat::Local {
            return Err(FilesystemError::Unsupported(
                "parent is not a short-form directory".into(),
            ));
        }
        let fs = fork_offset(false);
        let fork_len = core.size as usize;
        let fork_end = fs + fork_len;

        // Walk the entries capturing raw spans + names; collect survivors.
        let count = buf[fs] as usize;
        let i8count = buf[fs + 1];
        let ino_width = if i8count > 0 { 8 } else { 4 };
        let mut survivors: Vec<(usize, usize)> = Vec::with_capacity(count); // (start,end) in buf
        let mut removed = false;
        let mut pos = fs + 2 + ino_width;
        for _ in 0..count {
            let start = pos;
            if start + 3 > fork_end {
                return Err(FilesystemError::Parse("short-form entry truncated".into()));
            }
            let namelen = buf[start] as usize;
            let name_off = start + 3;
            let next = name_off + namelen + usize::from(has_ftype) + ino_width;
            if next > fork_end {
                return Err(FilesystemError::Parse("short-form entry overflow".into()));
            }
            let ename = String::from_utf8_lossy(&buf[name_off..name_off + namelen]);
            if !removed && ename == name {
                removed = true; // drop this one
            } else {
                survivors.push((start, next));
            }
            pos = next;
        }
        if !removed {
            return Err(FilesystemError::NotFound(name.into()));
        }

        // Rebuild: header (count', i8count, parent) + surviving spans verbatim.
        let header = buf[fs..fs + 2 + ino_width].to_vec();
        let mut new_fork = Vec::with_capacity(fork_len);
        new_fork.extend_from_slice(&header);
        for (s, e) in &survivors {
            new_fork.extend_from_slice(&buf[*s..*e]);
        }
        new_fork[0] = survivors.len() as u8; // count

        buf[fs..fs + new_fork.len()].copy_from_slice(&new_fork);
        for b in buf.iter_mut().take(fork_end).skip(fs + new_fork.len()) {
            *b = 0;
        }
        BigEndian::write_u64(&mut buf[56..64], new_fork.len() as u64);
        if child_was_dir {
            let version = buf[4];
            if version == 1 {
                let n = BigEndian::read_u16(&buf[6..8]).saturating_sub(1);
                BigEndian::write_u16(&mut buf[6..8], n);
            } else {
                let n = BigEndian::read_u32(&buf[16..20]).saturating_sub(1);
                BigEndian::write_u32(&mut buf[16..20], n);
            }
        }
        self.write_inode_region(sb, parent_ino, &buf)
    }

    /// Free a previously-allocated inode: mark its inobt slot free (set the
    /// `ir_free` bit, ++ record freecount + AGI freecount + `sb_ifree`) and zero
    /// its on-disk `di_mode` so it reads as a free inode.
    fn free_inode(
        &mut self,
        sb: &super::sb::XfsSuperblock,
        ino: u64,
    ) -> Result<(), FilesystemError> {
        let sectsize = sb.sectsize as u64;
        let agblocks = sb.agblocks as u64;
        let blocksize = sb.blocksize as u64;
        let bs = sb.blocksize as usize;
        let ino_shift = sb.agblklog + sb.inopblog;
        let agno = ino >> ino_shift;
        let agino = (ino & ((1u64 << ino_shift) - 1)) as u32;

        let agi_byte = self.partition_offset + agno * agblocks * blocksize + 2 * sectsize;
        let mut agi_sec = vec![0u8; sectsize as usize];
        read_at_aligned(&mut self.reader, agi_byte, sectsize, &mut agi_sec)?;
        let agi = XfsAgi::parse(&agi_sec)?;
        let leaves = self.collect_inobt_leaf_blocks(sb, agno, agi.root)?;
        for leaf_agbno in leaves {
            let fsblock = (agno << sb.agblklog) | leaf_agbno as u64;
            let mut block = vec![0u8; bs];
            self.read_fsblock(fsblock, &mut block)?;
            let numrecs = BigEndian::read_u16(&block[6..8]) as usize;
            let max_leaf = (bs - INOBT_HDR_LEN) / INOBT_REC_SIZE;
            if numrecs > max_leaf {
                continue;
            }
            for r in 0..numrecs {
                let off = INOBT_HDR_LEN + r * INOBT_REC_SIZE;
                let start_agino = BigEndian::read_u32(&block[off..off + 4]);
                if agino < start_agino || agino >= start_agino + XFS_INODES_PER_CHUNK as u32 {
                    continue;
                }
                let slot = (agino - start_agino) as u64;
                let freecount = BigEndian::read_u32(&block[off + 4..off + 8]);
                let free = BigEndian::read_u64(&block[off + 8..off + 16]);
                if (free >> slot) & 1 == 1 {
                    return Err(FilesystemError::InvalidData(format!(
                        "inode {ino} already free in inobt"
                    )));
                }
                BigEndian::write_u32(&mut block[off + 4..off + 8], freecount + 1);
                BigEndian::write_u64(&mut block[off + 8..off + 16], free | (1u64 << slot));
                self.write_fsblock(sb, fsblock, &block)?;

                BigEndian::write_u32(
                    &mut agi_sec[AGI_FREECOUNT_OFF..AGI_FREECOUNT_OFF + 4],
                    agi.freecount + 1,
                );
                self.write_at(agi_byte, &agi_sec)?;
                self.bump_sb_ifree(sb, 1)?;

                // Zero the dinode's mode so it reads as free.
                let (_c, mut ibuf) = self.read_inode_buf(ino)?;
                BigEndian::write_u16(&mut ibuf[2..4], 0);
                self.write_inode_region(sb, ino, &ibuf)?;
                return Ok(());
            }
        }
        Err(FilesystemError::NotFound(format!(
            "inode {ino} not found in any inobt chunk"
        )))
    }

    /// Return `extents` (as `(fsblock, count)`) to the free-space btrees by
    /// rebuilding each touched AG's bno/cnt over its current free set plus the
    /// freed blocks, then resyncing `sb_fdblocks`. The inverse of
    /// [`alloc_blocks`].
    fn free_blocks(
        &mut self,
        sb: &super::sb::XfsSuperblock,
        extents: &[(u64, u32)],
    ) -> Result<(), FilesystemError> {
        let agblocks = sb.agblocks as u64;
        let agmask = (1u64 << sb.agblklog) - 1;
        // Group the freed runs by AG (AG-relative).
        let mut by_ag: std::collections::BTreeMap<u64, Vec<FreeExtent>> =
            std::collections::BTreeMap::new();
        for &(fsblock, count) in extents {
            if count == 0 {
                continue;
            }
            let agno = fsblock >> sb.agblklog;
            let agbno = (fsblock & agmask) as u32;
            by_ag.entry(agno).or_default().push(FreeExtent {
                startblock: agbno,
                blockcount: count,
            });
        }
        for (agno, freed) in by_ag {
            let expected_len = if agno == sb.agcount as u64 - 1 {
                sb.dblocks - agno * agblocks
            } else {
                agblocks
            };
            let mut all = self.current_full_free(sb, agno, expected_len)?;
            all.extend(freed);
            let derived = coalesce(all);
            self.rebuild_ag_freespace(sb, agno, expected_len, &derived)?;
        }
        self.resync_sb_fdblocks(sb)?;
        Ok(())
    }

    /// Internal entry point for `delete_entry`: remove `name` (-> `child_ino`)
    /// from short-form directory `parent_ino`, free the child's data blocks, and
    /// free the child inode. Refuses a non-empty / non-short-form directory and
    /// a bmap-btree-format child (multi-extent free not implemented).
    pub(crate) fn do_delete_entry(
        &mut self,
        parent_ino: u64,
        name: &str,
        child_ino: u64,
    ) -> Result<(), FilesystemError> {
        let sb = self.superblock().clone();
        let (cc, cbuf) = self.read_inode_buf(child_ino)?;
        let child_is_dir = cc.is_dir();
        if child_is_dir {
            if cc.format != super::types::DiFormat::Local {
                return Err(FilesystemError::Unsupported(
                    "cannot delete a non-short-form directory yet".into(),
                ));
            }
            let fork = self.data_fork(&cc, &cbuf);
            if fork.first().copied().unwrap_or(0) != 0 {
                return Err(FilesystemError::InvalidData("directory not empty".into()));
            }
        }
        let extents: Vec<(u64, u32)> = match cc.format {
            super::types::DiFormat::Extents => self
                .decode_data_extents(&cc, &cbuf)?
                .iter()
                .map(|e| (e.startblock, e.blockcount as u32))
                .collect(),
            super::types::DiFormat::Btree => {
                return Err(FilesystemError::Unsupported(
                    "cannot delete a bmap-btree-format inode yet".into(),
                ));
            }
            _ => Vec::new(), // local / device: owns no blocks
        };

        self.sf_remove_entry(&sb, parent_ino, name, child_is_dir)?;
        if !extents.is_empty() {
            self.free_blocks(&sb, &extents)?;
        }
        self.free_inode(&sb, child_ino)?;
        self.reader.flush()?;
        Ok(())
    }
}
