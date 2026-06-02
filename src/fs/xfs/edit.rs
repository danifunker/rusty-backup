//! XFS write primitives (§3 of `docs/xfs_edit_and_repair.md`) — the shared
//! foundation for Track-B editing (`create_directory` here) and R7 orphan
//! reconnection. Everything is **v4-only** (no inode/dir CRCs to recompute)
//! and writes straight through to the device (no in-memory staging), like the
//! repair path.
//!
//! Primitives, smallest first:
//!
//!   * **Inode-slot allocation** ([`alloc_inode_slot`]) / freeing
//!     ([`free_inode`]): claim/release an inode in an existing inode-btree
//!     chunk (flip `ir_free`, adjust the record freecount + AGI freecount +
//!     `sb_ifree`). New-chunk allocation when every chunk is full is deferred.
//!   * **Block allocation** ([`alloc_blocks`]) / freeing ([`free_blocks`]):
//!     carve/return contiguous blocks by rebuilding the AG free-space btrees
//!     (reusing the R2 rebuild) + resyncing `sb_fdblocks`.
//!   * **Directory insert** ([`dir_insert_entry`]) / remove ([`sf_remove_entry`]):
//!     short-form inline add/remove, automatic short-form→single-block
//!     conversion on overflow ([`build_block_dir`] writes the data entries,
//!     `bestfree`, and the sorted hash leaf index), and rebuild-in-place inserts
//!     for an already single-block directory. Leaf/node (multi-block)
//!     directories are still `Unsupported`.
//!
//! On top of these: `create_directory`, `create_file` (single contiguous
//! extent), and `delete_entry` (empty short-form dirs / single-or-no-extent
//! files). Porting reference: `xfs_repair` @ v3.1.11 + `libxfs/xfs_dir2_sf.c`,
//! `libxfs/xfs_dir2_block.c`, `libxfs/xfs_ialloc.c`.

use std::io::{Read, Seek, Write};

use byteorder::{BigEndian, ByteOrder};

use super::ag::{XfsAgf, XfsAgi};
use super::bmap::{encode_extent, fsblock_to_partition_byte};
use super::btree_build::{blocks_needed, FreeExtent};
use super::dir2::XFS_DIR2_DATA_HDR_LEN;
use super::freespace_rebuild::{carve_from_largest, coalesce};
use super::inode::fork_offset;
use super::types::{XFS_ABTB_MAGIC, XFS_ABTC_MAGIC, XFS_INODES_PER_CHUNK};
use super::{read_at_aligned, XfsFilesystem};
use crate::fs::filesystem::{Filesystem, FilesystemError};
use crate::fs::fsck::RepairReport;

/// Largest single-extent file we write: the on-disk blockcount field is 21 bits.
const MAX_SINGLE_EXTENT_BLOCKS: u64 = (1 << 21) - 1;

/// A directory's child entries as `(name, inode, ftype)`, excluding `.`/`..`.
type DirEntries = Vec<(String, u64, u8)>;

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

        // Pre-flight (no writes): unique name + the parent can take the entry
        // (short-form, or short-form converting to a single block, or an
        // existing single-block dir) — so a failure leaves the volume unchanged.
        self.dir_can_insert(&sb, parent_ino, name, true)?;

        let ino = self.alloc_inode_slot(&sb)?;
        self.init_empty_shortform_dir(&sb, ino, parent_ino, mode, uid, gid)?;
        self.dir_insert_entry(&sb, parent_ino, name, ino, true)?;
        self.reader.flush()?;
        Ok(ino)
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
    /// Initialize a freshly-allocated inode as a regular file. `extents` is the
    /// data fork as `(startoff, fsblock, count)` records (already in file-offset
    /// order); empty for a zero-length file.
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn init_file_inode(
        &mut self,
        sb: &super::sb::XfsSuperblock,
        ino: u64,
        mode: u16,
        uid: u32,
        gid: u32,
        size: u64,
        extents: &[(u64, u64, u32)],
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
        let nblocks: u64 = extents.iter().map(|&(_, _, c)| c as u64).sum();
        BigEndian::write_u64(&mut buf[64..72], nblocks);
        for b in buf.iter_mut().take(76).skip(72) {
            *b = 0; // extsize
        }
        BigEndian::write_u32(&mut buf[76..80], extents.len() as u32);
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
        // Inline extent records must fit the literal area; the caller bounds the
        // extent count, but guard anyway.
        if fs + extents.len() * 16 > isz {
            return Err(FilesystemError::Unsupported(
                "too many extents for an inline data fork (bmap-btree not implemented)".into(),
            ));
        }
        for (i, &(startoff, fsblock, count)) in extents.iter().enumerate() {
            let rec = encode_extent(false, startoff, fsblock, count as u64);
            buf[fs + i * 16..fs + i * 16 + 16].copy_from_slice(&rec);
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
        self.dir_can_insert(&sb, parent_ino, name, false)?;

        let bs = sb.blocksize as u64;
        let nblocks = data_len.div_ceil(bs);
        // Each extent's blockcount is 21 bits; the data fork holds it inline as
        // up to `max_inline` records before it would need a bmap btree.
        if nblocks > MAX_SINGLE_EXTENT_BLOCKS {
            return Err(FilesystemError::Unsupported(format!(
                "file needs {nblocks} blocks; files larger than one 2^21-block extent are not supported"
            )));
        }

        // Allocate data first (so a DiskFull leaves no half-built inode): one
        // contiguous run when possible, else several (capped at the inline-fork
        // extent limit). Then the inode, then write data + init + link.
        let runs = if nblocks > 0 {
            self.alloc_extents(&sb, nblocks as u32)?
        } else {
            Vec::new()
        };
        // Assign sequential file offsets to the runs.
        let mut extents: Vec<(u64, u64, u32)> = Vec::with_capacity(runs.len());
        let mut off = 0u64;
        for (fsblock, count) in runs {
            extents.push((off, fsblock, count));
            off += count as u64;
        }

        let ino = self.alloc_inode_slot(&sb)?;
        self.write_file_data_extents(&sb, &extents, data, data_len)?;
        self.init_file_inode(&sb, ino, mode, uid, gid, data_len, &extents)?;
        self.dir_insert_entry(&sb, parent_ino, name, ino, false)?;
        self.reader.flush()?;
        Ok(ino)
    }

    /// Allocate `n` blocks as up to `max_inline` contiguous runs. Tries a single
    /// run first (the common case); otherwise carves several runs largest-first
    /// from one allocation group's free extents and rebuilds that AG's
    /// free-space btrees over the remainder. Errors `DiskFull` when no AG can
    /// satisfy `n` within the inline-extent budget.
    fn alloc_extents(
        &mut self,
        sb: &super::sb::XfsSuperblock,
        n: u32,
    ) -> Result<Vec<(u64, u32)>, FilesystemError> {
        // Fast path: one contiguous run.
        match self.alloc_blocks(sb, n) {
            Ok(fsblock) => return Ok(vec![(fsblock, n)]),
            Err(FilesystemError::DiskFull(_)) => {}
            Err(e) => return Err(e),
        }

        let bs = sb.blocksize as usize;
        let max_inline = (sb.inodesize as usize - fork_offset(false)) / 16;
        let agblocks = sb.agblocks as u64;
        for agno in 0..sb.agcount as u64 {
            let expected_len = if agno == sb.agcount as u64 - 1 {
                sb.dblocks - agno * agblocks
            } else {
                agblocks
            };
            let mut full = match self.current_full_free(sb, agno, expected_len) {
                Ok(f) => f,
                Err(_) => continue,
            };
            let total: u64 = full.iter().map(|e| e.blockcount as u64).sum();
            if total < n as u64 {
                continue;
            }
            // Allocate largest-first to minimise the run count; take from the
            // start of each extent, leaving its tail free.
            full.sort_by_key(|e| std::cmp::Reverse(e.blockcount));
            let mut runs: Vec<(u32, u32)> = Vec::new();
            let mut post: Vec<FreeExtent> = Vec::new();
            let mut remaining = n;
            for ext in &full {
                if remaining == 0 {
                    post.push(*ext);
                    continue;
                }
                let take = remaining.min(ext.blockcount);
                runs.push((ext.startblock, take));
                remaining -= take;
                if take < ext.blockcount {
                    post.push(FreeExtent {
                        startblock: ext.startblock + take,
                        blockcount: ext.blockcount - take,
                    });
                }
            }
            if remaining > 0 || runs.len() > max_inline {
                continue; // (shouldn't underflow; too fragmented for inline)
            }
            // The rebuild carves its btree blocks from the largest remaining
            // free extent, so make sure one is big enough.
            let post_largest = post.iter().map(|e| e.blockcount).max().unwrap_or(0) as usize;
            let need_bt = 2 * blocks_needed(post.len(), bs);
            if post_largest <= need_bt {
                continue;
            }
            self.rebuild_ag_freespace(sb, agno, expected_len, &post)?;
            self.resync_sb_fdblocks(sb)?;
            return Ok(runs
                .into_iter()
                .map(|(agbno, c)| ((agno << sb.agblklog) | agbno as u64, c))
                .collect());
        }
        Err(FilesystemError::DiskFull(format!(
            "could not allocate {n} blocks as <= {max_inline} contiguous runs"
        )))
    }

    /// Write `data_len` bytes from `data` across the file's `extents`
    /// (`(startoff, fsblock, count)`), zero-padding the final block of each run.
    fn write_file_data_extents(
        &mut self,
        sb: &super::sb::XfsSuperblock,
        extents: &[(u64, u64, u32)],
        data: &mut dyn Read,
        data_len: u64,
    ) -> Result<(), FilesystemError> {
        let bs = sb.blocksize as u64;
        for &(startoff, fsblock, count) in extents {
            let run_file_start = startoff * bs;
            let run_bytes = count as u64 * bs;
            let want = data_len.saturating_sub(run_file_start).min(run_bytes);
            self.write_file_data(sb, fsblock, count, data, want)?;
        }
        Ok(())
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

        self.dir_remove_entry(&sb, parent_ino, name, child_is_dir)?;
        if !extents.is_empty() {
            self.free_blocks(&sb, &extents)?;
        }
        self.free_inode(&sb, child_ino)?;
        self.reader.flush()?;
        Ok(())
    }

    // ----- format-aware directory insert (short-form OR single-block) --------

    /// Check, without writing, that `name` can be inserted into directory
    /// `parent_ino` (unique name; parent is short-form or single-block; the
    /// result fits — converting short-form→block if the inline fork overflows).
    /// Errors `AlreadyExists` / `DiskFull` / `Unsupported` accordingly.
    pub(crate) fn dir_can_insert(
        &mut self,
        sb: &super::sb::XfsSuperblock,
        parent_ino: u64,
        name: &str,
        new_is_dir: bool,
    ) -> Result<(), FilesystemError> {
        let has_ftype = sb.has_ftype();
        let (core, buf) = self.read_inode_buf(parent_ino)?;
        if !core.is_dir() {
            return Err(FilesystemError::Unsupported(
                "parent is not a directory".into(),
            ));
        }
        let (dotdot, mut entries) = self.gather_dir_entries(sb, &core, &buf, parent_ino)?;
        if entries.iter().any(|(n, _, _)| n == name) {
            return Err(FilesystemError::AlreadyExists(name.into()));
        }
        match core.format {
            super::types::DiFormat::Local => {
                // Inline fit?
                let fs = fork_offset(false);
                let entry_len = 1 + 2 + name.len() + usize::from(has_ftype) + 4;
                let avail_end = if core.forkoff > 0 {
                    fs + (core.forkoff as usize) * 8
                } else {
                    buf.len()
                };
                if fs + core.size as usize + entry_len <= avail_end {
                    return Ok(()); // fits inline
                }
                // Would overflow → must fit one block.
                let ft = if new_is_dir {
                    XFS_DIR3_FT_DIR
                } else {
                    XFS_DIR3_FT_REG_FILE
                };
                entries.push((name.to_string(), 0, ft));
                build_block_dir(
                    &entries,
                    parent_ino,
                    dotdot,
                    sb.dirblksize() as usize,
                    has_ftype,
                )
                .map(|_| ())
            }
            super::types::DiFormat::Extents => {
                let ft = if new_is_dir {
                    XFS_DIR3_FT_DIR
                } else {
                    XFS_DIR3_FT_REG_FILE
                };
                entries.push((name.to_string(), 0, ft));
                build_block_dir(
                    &entries,
                    parent_ino,
                    dotdot,
                    sb.dirblksize() as usize,
                    has_ftype,
                )
                .map(|_| ())
            }
            _ => Err(FilesystemError::Unsupported(
                "parent directory format not editable (btree/leaf/node)".into(),
            )),
        }
    }

    /// Insert `(name -> child_ino)` into directory `parent_ino`, handling
    /// short-form (inline, or converted to a single block on overflow) and
    /// single-block directories. Bumps the parent link count for a subdirectory.
    pub(crate) fn dir_insert_entry(
        &mut self,
        sb: &super::sb::XfsSuperblock,
        parent_ino: u64,
        name: &str,
        child_ino: u64,
        is_dir: bool,
    ) -> Result<(), FilesystemError> {
        let (core, _buf) = self.read_inode_buf(parent_ino)?;
        match core.format {
            super::types::DiFormat::Local => {
                match self.sf_insert_entry(sb, parent_ino, name, child_ino, is_dir) {
                    Err(FilesystemError::DiskFull(_)) => {
                        self.convert_sf_dir_to_block(sb, parent_ino, name, child_ino, is_dir)
                    }
                    other => other,
                }
            }
            super::types::DiFormat::Extents => {
                self.block_insert_entry(sb, parent_ino, name, child_ino, is_dir)
            }
            _ => Err(FilesystemError::Unsupported(
                "parent directory format not editable (btree/leaf/node)".into(),
            )),
        }
    }

    /// Convert an overflowing short-form directory to a single-block directory,
    /// including the new `(name -> child_ino)` entry. Allocates one directory
    /// block, writes it, and rewrites the inode to extents format.
    fn convert_sf_dir_to_block(
        &mut self,
        sb: &super::sb::XfsSuperblock,
        parent_ino: u64,
        name: &str,
        child_ino: u64,
        is_dir: bool,
    ) -> Result<(), FilesystemError> {
        let has_ftype = sb.has_ftype();
        let (core, buf) = self.read_inode_buf(parent_ino)?;
        let (dotdot, mut entries) = self.gather_dir_entries(sb, &core, &buf, parent_ino)?;
        let ft = if is_dir {
            XFS_DIR3_FT_DIR
        } else {
            XFS_DIR3_FT_REG_FILE
        };
        entries.push((name.to_string(), child_ino, ft));

        let dirblksize = sb.dirblksize() as usize;
        let block = build_block_dir(&entries, parent_ino, dotdot, dirblksize, has_ftype)?;
        let blocks_per_dir = (dirblksize as u64).div_ceil(sb.blocksize as u64) as u32;
        let fsblock = self.alloc_blocks(sb, blocks_per_dir)?;
        self.write_dir_block(sb, fsblock, &block)?;
        self.set_dir_inode_single_extent(
            sb,
            parent_ino,
            fsblock,
            blocks_per_dir,
            dirblksize,
            is_dir,
        )
    }

    /// Rebuild an already single-block directory with the new entry appended,
    /// writing it back in place (no reallocation). Errors `DiskFull` if the
    /// directory has outgrown one block (leaf/node format not implemented).
    fn block_insert_entry(
        &mut self,
        sb: &super::sb::XfsSuperblock,
        parent_ino: u64,
        name: &str,
        child_ino: u64,
        is_dir: bool,
    ) -> Result<(), FilesystemError> {
        let has_ftype = sb.has_ftype();
        let (core, buf) = self.read_inode_buf(parent_ino)?;
        let extents = self.decode_data_extents(&core, &buf)?;
        let first = extents
            .first()
            .ok_or_else(|| FilesystemError::Parse("block dir has no extent".into()))?;
        if first.startoff != 0 {
            return Err(FilesystemError::Unsupported(
                "multi-block directory not editable".into(),
            ));
        }
        let dirblksize = sb.dirblksize() as usize;
        let blocks_per_dir = (dirblksize as u64).div_ceil(sb.blocksize as u64) as u32;
        let fsblock = first.startblock;

        let (dotdot, mut entries) =
            self.read_block_dir_entries(sb, fsblock, dirblksize, has_ftype)?;
        if entries.iter().any(|(n, _, _)| n == name) {
            return Err(FilesystemError::AlreadyExists(name.into()));
        }
        let ft = if is_dir {
            XFS_DIR3_FT_DIR
        } else {
            XFS_DIR3_FT_REG_FILE
        };
        entries.push((name.to_string(), child_ino, ft));
        let block = build_block_dir(&entries, parent_ino, dotdot, dirblksize, has_ftype)?;
        self.write_dir_block(sb, fsblock, &block)?;
        let _ = blocks_per_dir;
        if is_dir {
            self.bump_dir_nlink(sb, parent_ino, 1)?;
        }
        Ok(())
    }

    /// Remove `name` from directory `parent_ino`, dispatching by format:
    /// short-form (inline rewrite) or single-block (rebuild in place). A
    /// single-block directory is left in block form even if it shrinks small
    /// (valid, just not re-compacted to short-form).
    fn dir_remove_entry(
        &mut self,
        sb: &super::sb::XfsSuperblock,
        parent_ino: u64,
        name: &str,
        child_was_dir: bool,
    ) -> Result<(), FilesystemError> {
        let (core, _buf) = self.read_inode_buf(parent_ino)?;
        match core.format {
            super::types::DiFormat::Local => {
                self.sf_remove_entry(sb, parent_ino, name, child_was_dir)
            }
            super::types::DiFormat::Extents => {
                self.block_remove_entry(sb, parent_ino, name, child_was_dir)
            }
            _ => Err(FilesystemError::Unsupported(
                "parent directory format not editable (btree/leaf/node)".into(),
            )),
        }
    }

    /// Remove `name` from a single-block directory by rebuilding the block
    /// without it and writing it back in place.
    fn block_remove_entry(
        &mut self,
        sb: &super::sb::XfsSuperblock,
        parent_ino: u64,
        name: &str,
        child_was_dir: bool,
    ) -> Result<(), FilesystemError> {
        let has_ftype = sb.has_ftype();
        let (core, buf) = self.read_inode_buf(parent_ino)?;
        let extents = self.decode_data_extents(&core, &buf)?;
        let first = extents
            .first()
            .ok_or_else(|| FilesystemError::Parse("block dir has no extent".into()))?;
        if first.startoff != 0 {
            return Err(FilesystemError::Unsupported(
                "multi-block directory not editable".into(),
            ));
        }
        let dirblksize = sb.dirblksize() as usize;
        let fsblock = first.startblock;
        let (dotdot, mut entries) =
            self.read_block_dir_entries(sb, fsblock, dirblksize, has_ftype)?;
        let before = entries.len();
        entries.retain(|(n, _, _)| n != name);
        if entries.len() == before {
            return Err(FilesystemError::NotFound(name.into()));
        }
        let block = build_block_dir(&entries, parent_ino, dotdot, dirblksize, has_ftype)?;
        self.write_dir_block(sb, fsblock, &block)?;
        if child_was_dir {
            self.bump_dir_nlink(sb, parent_ino, -1)?;
        }
        Ok(())
    }

    /// Collect a directory's entries `(name, ino, ftype)` (excluding `.`/`..`)
    /// plus the `..` inode, from a short-form or single-block directory.
    fn gather_dir_entries(
        &mut self,
        sb: &super::sb::XfsSuperblock,
        core: &super::inode::XfsDinodeCore,
        buf: &[u8],
        parent_ino: u64,
    ) -> Result<(u64, DirEntries), FilesystemError> {
        let has_ftype = sb.has_ftype();
        match core.format {
            super::types::DiFormat::Local => {
                let fs = fork_offset(false);
                let fork = &buf[fs..fs + core.size as usize];
                Ok(sf_entries_with_ftype(fork, has_ftype)?)
            }
            super::types::DiFormat::Extents => {
                let extents = self.decode_data_extents(core, buf)?;
                let first = extents
                    .first()
                    .ok_or_else(|| FilesystemError::Parse("block dir has no extent".into()))?;
                let dirblksize = sb.dirblksize() as usize;
                self.read_block_dir_entries(sb, first.startblock, dirblksize, has_ftype)
            }
            _ => {
                let _ = parent_ino;
                Err(FilesystemError::Unsupported(
                    "parent directory format not editable".into(),
                ))
            }
        }
    }

    /// Read a single-block directory's entries `(name, ino, ftype)` (skipping
    /// `.`/`..`) and the `..` inode.
    fn read_block_dir_entries(
        &mut self,
        sb: &super::sb::XfsSuperblock,
        fsblock: u64,
        dirblksize: usize,
        has_ftype: bool,
    ) -> Result<(u64, DirEntries), FilesystemError> {
        let mut block = vec![0u8; dirblksize];
        let bs = sb.blocksize as u64;
        for i in 0..(dirblksize as u64 / bs) {
            let part =
                fsblock_to_partition_byte(fsblock + i, sb.agblocks, sb.agblklog, sb.blocksize);
            read_at_aligned(
                &mut self.reader,
                self.partition_offset + part,
                bs,
                &mut block[(i * bs) as usize..((i + 1) * bs) as usize],
            )?;
        }
        block_entries_with_ftype(&block, has_ftype)
    }

    /// Write `block` bytes across the directory block's fsblock(s).
    fn write_dir_block(
        &mut self,
        sb: &super::sb::XfsSuperblock,
        fsblock: u64,
        block: &[u8],
    ) -> Result<(), FilesystemError> {
        use std::io::SeekFrom;
        let bs = sb.blocksize as usize;
        for (i, chunk) in block.chunks(bs).enumerate() {
            let part = fsblock_to_partition_byte(
                fsblock + i as u64,
                sb.agblocks,
                sb.agblklog,
                sb.blocksize,
            );
            self.reader
                .seek(SeekFrom::Start(self.partition_offset + part))?;
            self.reader.write_all(chunk)?;
        }
        Ok(())
    }

    /// Rewrite a directory inode to extents format with a single data extent
    /// `(fsblock, blocks)` of size `dirblksize`. Bumps nlink when `add_subdir`.
    fn set_dir_inode_single_extent(
        &mut self,
        sb: &super::sb::XfsSuperblock,
        ino: u64,
        fsblock: u64,
        blocks: u32,
        dirblksize: usize,
        add_subdir: bool,
    ) -> Result<(), FilesystemError> {
        let (_core, mut buf) = self.read_inode_buf(ino)?;
        buf[5] = 2; // di_format = extents
        BigEndian::write_u64(&mut buf[56..64], dirblksize as u64); // di_size
        BigEndian::write_u64(&mut buf[64..72], blocks as u64); // di_nblocks
        BigEndian::write_u32(&mut buf[76..80], 1); // di_nextents
        if add_subdir {
            let version = buf[4];
            if version == 1 {
                let n = BigEndian::read_u16(&buf[6..8]).wrapping_add(1);
                BigEndian::write_u16(&mut buf[6..8], n);
            } else {
                let n = BigEndian::read_u32(&buf[16..20]).wrapping_add(1);
                BigEndian::write_u32(&mut buf[16..20], n);
            }
        }
        let fs = fork_offset(false);
        for b in buf.iter_mut().skip(fs) {
            *b = 0;
        }
        let rec = encode_extent(false, 0, fsblock, blocks as u64);
        buf[fs..fs + 16].copy_from_slice(&rec);
        self.write_inode_region(sb, ino, &buf)
    }

    /// Bump a directory inode's link count by `delta` (version-correct field).
    fn bump_dir_nlink(
        &mut self,
        sb: &super::sb::XfsSuperblock,
        ino: u64,
        delta: i32,
    ) -> Result<(), FilesystemError> {
        let (_core, mut buf) = self.read_inode_buf(ino)?;
        let version = buf[4];
        if version == 1 {
            let n = (BigEndian::read_u16(&buf[6..8]) as i32 + delta).max(0) as u16;
            BigEndian::write_u16(&mut buf[6..8], n);
        } else {
            let n = (BigEndian::read_u32(&buf[16..20]) as i32 + delta).max(0) as u32;
            BigEndian::write_u32(&mut buf[16..20], n);
        }
        self.write_inode_region(sb, ino, &buf)
    }

    // ----- R7 orphan reconnection -------------------------------------------

    /// R7 (reconnection half): link every allocated-but-unreachable inode into
    /// `/lost+found` (created under the root if absent), naming each by its
    /// inode number. A reconnected subdirectory has its `..` repointed at
    /// `lost+found`; a detached *subtree* reconnects only at its top (children
    /// that are themselves reachable from an orphan are left in place). Link
    /// counts are not adjusted here — the R7 nlink pass runs afterwards and
    /// recomputes them from the now-connected tree. v4 only.
    pub(crate) fn run_orphan_reconnect(&mut self) -> Result<RepairReport, FilesystemError> {
        let mut report = RepairReport {
            fixes_applied: Vec::new(),
            fixes_failed: Vec::new(),
            unrepairable_count: 0,
        };
        let sb = self.superblock().clone();
        if sb.is_v5() {
            return Ok(report);
        }
        let root = match self.root() {
            Ok(r) => r,
            Err(_) => return Ok(report),
        };

        // Reachability BFS from the root + superblock-internal inodes. If any
        // directory can't be listed the reachable set is incomplete, so abort
        // (reconnecting then would wrongly relink genuinely-reachable inodes).
        use std::collections::HashSet;
        let mut reachable: HashSet<u64> = sb.internal_inodes().into_iter().collect();
        reachable.insert(root.location);
        let mut visited: HashSet<u64> = HashSet::new();
        let mut queue = vec![root.clone()];
        // child -> true if it's referenced by some directory we visited that is
        // itself an orphan; used to keep detached subtrees intact.
        while let Some(dir) = queue.pop() {
            if !visited.insert(dir.location) {
                continue;
            }
            let children = match self.list_directory(&dir) {
                Ok(c) => c,
                Err(_) => return Ok(report),
            };
            for child in children {
                reachable.insert(child.location);
                if child.is_directory() {
                    queue.push(child);
                }
            }
        }

        let allocated = self.enumerate_allocated_inodes(&sb);
        let mut orphans: Vec<u64> = allocated
            .into_iter()
            .filter(|i| !reachable.contains(i))
            .collect();
        orphans.sort_unstable();
        if orphans.is_empty() {
            return Ok(report);
        }
        let orphan_set: HashSet<u64> = orphans.iter().copied().collect();

        // Keep detached subtrees intact: an orphan that is a child of another
        // orphan directory reconnects via that parent, not directly.
        let mut child_of_orphan: HashSet<u64> = HashSet::new();
        for &o in &orphans {
            let core = match self.read_inode(o) {
                Ok(c) => c,
                Err(_) => continue,
            };
            if !core.is_dir() {
                continue;
            }
            let entry = match self.child_entry("/", format!("orphan_{o}"), o) {
                Ok(e) => e,
                Err(_) => continue,
            };
            if let Ok(children) = self.list_directory(&entry) {
                for c in children {
                    if orphan_set.contains(&c.location) {
                        child_of_orphan.insert(c.location);
                    }
                }
            }
        }
        let roots: Vec<u64> = orphans
            .iter()
            .copied()
            .filter(|o| !child_of_orphan.contains(o))
            .collect();
        if roots.is_empty() {
            return Ok(report);
        }

        let lf_ino = match self.ensure_lost_found(&sb, root.location) {
            Ok(i) => i,
            Err(e) => {
                report
                    .fixes_failed
                    .push(format!("could not create lost+found: {e}"));
                return Ok(report);
            }
        };

        let mut count = 0u64;
        for o in roots {
            if o == lf_ino {
                continue;
            }
            let is_dir = self.read_inode(o).map(|c| c.is_dir()).unwrap_or(false);
            let name = format!("{o}");
            match self.dir_insert_entry(&sb, lf_ino, &name, o, is_dir) {
                Ok(()) => {
                    if is_dir {
                        if let Err(e) = self.set_dotdot(&sb, o, lf_ino) {
                            report
                                .fixes_failed
                                .push(format!("inode {o}: reconnected but '..' fix failed: {e}"));
                        }
                    }
                    count += 1;
                }
                Err(e) => report
                    .fixes_failed
                    .push(format!("inode {o}: reconnect failed: {e}")),
            }
        }
        if count > 0 {
            self.reader.flush()?;
            report
                .fixes_applied
                .push(format!("reconnected {count} orphan(s) into /lost+found"));
        }
        Ok(report)
    }

    /// Find `/lost+found` under the root (must be a directory) or create it.
    fn ensure_lost_found(
        &mut self,
        sb: &super::sb::XfsSuperblock,
        root_ino: u64,
    ) -> Result<u64, FilesystemError> {
        let root = self.root()?;
        if let Ok(children) = self.list_directory(&root) {
            if let Some(e) = children.iter().find(|e| e.name == "lost+found") {
                if e.is_directory() {
                    return Ok(e.location);
                }
            }
        }
        // Create it: a fresh empty short-form directory linked into the root.
        self.dir_can_insert(sb, root_ino, "lost+found", true)?;
        let ino = self.alloc_inode_slot(sb)?;
        self.init_empty_shortform_dir(sb, ino, root_ino, 0o040755, 0, 0)?;
        self.dir_insert_entry(sb, root_ino, "lost+found", ino, true)?;
        Ok(ino)
    }

    /// Repoint a directory's `..` at `new_parent` (short-form header parent, or
    /// the `..` data entry of a single-block directory).
    fn set_dotdot(
        &mut self,
        sb: &super::sb::XfsSuperblock,
        ino: u64,
        new_parent: u64,
    ) -> Result<(), FilesystemError> {
        let (core, mut buf) = self.read_inode_buf(ino)?;
        match core.format {
            super::types::DiFormat::Local => {
                let fs = fork_offset(false);
                if buf[fs + 1] > 0 {
                    BigEndian::write_u64(&mut buf[fs + 2..fs + 10], new_parent);
                } else {
                    BigEndian::write_u32(&mut buf[fs + 2..fs + 6], new_parent as u32);
                }
                self.write_inode_region(sb, ino, &buf)
            }
            super::types::DiFormat::Extents => {
                let extents = self.decode_data_extents(&core, &buf)?;
                let first = extents
                    .first()
                    .ok_or_else(|| FilesystemError::Parse("block dir has no extent".into()))?;
                let dirblksize = sb.dirblksize() as usize;
                let mut block = vec![0u8; dirblksize];
                let bs = sb.blocksize as u64;
                for i in 0..(dirblksize as u64 / bs) {
                    let part = fsblock_to_partition_byte(
                        first.startblock + i,
                        sb.agblocks,
                        sb.agblklog,
                        sb.blocksize,
                    );
                    read_at_aligned(
                        &mut self.reader,
                        self.partition_offset + part,
                        bs,
                        &mut block[(i * bs) as usize..((i + 1) * bs) as usize],
                    )?;
                }
                // Find the ".." data entry and rewrite its inumber.
                let has_ftype = sb.has_ftype();
                let block_end = block.len();
                let tail = block_end - 8;
                let leaf_count = BigEndian::read_u32(&block[tail..tail + 4]) as usize;
                let data_end = block_end - (leaf_count * 8 + 8);
                let mut pos = XFS_DIR2_DATA_HDR_LEN;
                let mut done = false;
                while pos + 4 <= data_end {
                    if BigEndian::read_u16(&block[pos..pos + 2]) == 0xFFFF {
                        let len = BigEndian::read_u16(&block[pos + 2..pos + 4]) as usize;
                        if len == 0 {
                            break;
                        }
                        pos += len;
                        continue;
                    }
                    let namelen = block[pos + 8] as usize;
                    if &block[pos + 9..pos + 9 + namelen] == b".." {
                        BigEndian::write_u64(&mut block[pos..pos + 8], new_parent);
                        done = true;
                        break;
                    }
                    pos += dir2_data_entsize(namelen, has_ftype);
                }
                if !done {
                    return Err(FilesystemError::Parse(
                        "'..' entry not found in block dir".into(),
                    ));
                }
                self.write_dir_block(sb, first.startblock, &block)
            }
            _ => Err(FilesystemError::Unsupported(
                "cannot repoint '..' of a btree/leaf/node directory".into(),
            )),
        }
    }

    /// Every allocated inode number on the volume (walk every AG's inode btree).
    fn enumerate_allocated_inodes(&mut self, sb: &super::sb::XfsSuperblock) -> Vec<u64> {
        let agblocks = sb.agblocks as u64;
        let blocksize = sb.blocksize as u64;
        let sectsize = sb.sectsize as u64;
        let bs = sb.blocksize as usize;
        let ino_shift = sb.agblklog + sb.inopblog;
        let mut out = Vec::new();
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
            let leaves = match self.collect_inobt_leaf_blocks(sb, agno, agi.root) {
                Ok(l) => l,
                Err(_) => continue,
            };
            let mut block = vec![0u8; bs];
            for leaf_agbno in leaves {
                let fsblock = (agno << sb.agblklog) | leaf_agbno as u64;
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
                    let free = BigEndian::read_u64(&block[off + 8..off + 16]);
                    for slot in 0..XFS_INODES_PER_CHUNK as u64 {
                        if (free >> slot) & 1 == 0 {
                            out.push((agno << ino_shift) | (start_agino + slot));
                        }
                    }
                }
            }
        }
        out
    }
}

/// XFS directory name hash (`xfs_da_hashname`) — used to build the leaf hash
/// index of a single-block directory.
fn dir_hashname(name: &[u8]) -> u32 {
    let mut hash: u32 = 0;
    let mut chunks = name.chunks_exact(4);
    for c in &mut chunks {
        hash = ((c[0] as u32) << 21)
            ^ ((c[1] as u32) << 14)
            ^ ((c[2] as u32) << 7)
            ^ (c[3] as u32)
            ^ hash.rotate_left(7 * 4);
    }
    let rem = chunks.remainder();
    match rem.len() {
        3 => {
            ((rem[0] as u32) << 14)
                ^ ((rem[1] as u32) << 7)
                ^ (rem[2] as u32)
                ^ hash.rotate_left(7 * 3)
        }
        2 => ((rem[0] as u32) << 7) ^ (rem[1] as u32) ^ hash.rotate_left(7 * 2),
        1 => (rem[0] as u32) ^ hash.rotate_left(7),
        _ => hash,
    }
}

/// Data-block size one directory entry occupies: `inumber(8) + namelen(1) +
/// name + [ftype] + tag(2)`, rounded up to 8.
fn dir2_data_entsize(namelen: usize, has_ftype: bool) -> usize {
    let raw = 8 + 1 + namelen + usize::from(has_ftype) + 2;
    raw.div_ceil(8) * 8
}

/// Build a v4 single-block (`XD2B`) directory holding `.`, `..`, and `entries`.
/// `self_ino` is the directory's own inode (its `.`), `dotdot` its parent.
/// Returns `DiskFull` if it doesn't fit one `dirblksize` block.
fn build_block_dir(
    entries: &[(String, u64, u8)],
    self_ino: u64,
    dotdot: u64,
    dirblksize: usize,
    has_ftype: bool,
) -> Result<Vec<u8>, FilesystemError> {
    // Full entry list with synthetic "." and "..".
    let mut all: Vec<(&[u8], u64, u8)> = Vec::with_capacity(entries.len() + 2);
    all.push((b".", self_ino, XFS_DIR3_FT_DIR));
    all.push((b"..", dotdot, XFS_DIR3_FT_DIR));
    for (n, ino, ft) in entries {
        all.push((n.as_bytes(), *ino, *ft));
    }

    let leaf_count = all.len();
    let leaf_section = leaf_count * 8 + 8; // leaf entries + tail
    let data_size: usize = all
        .iter()
        .map(|(n, _, _)| dir2_data_entsize(n.len(), has_ftype))
        .sum();
    let data_start = XFS_DIR2_DATA_HDR_LEN;
    let leaf_start = dirblksize
        .checked_sub(leaf_section)
        .ok_or_else(|| FilesystemError::DiskFull("dir block too small".into()))?;
    if data_start + data_size > leaf_start {
        return Err(FilesystemError::DiskFull(
            "directory entries exceed one block (leaf/node format not implemented)".into(),
        ));
    }

    let mut buf = vec![0u8; dirblksize];
    BigEndian::write_u32(&mut buf[0..4], super::dir2::XFS_DIR2_BLOCK_MAGIC);

    // Data entries from data_start; collect (hashval, address) for the leaf.
    let mut leaf: Vec<(u32, u32)> = Vec::with_capacity(leaf_count);
    let mut pos = data_start;
    for (name, ino, ft) in &all {
        let namelen = name.len();
        let ent = dir2_data_entsize(namelen, has_ftype);
        BigEndian::write_u64(&mut buf[pos..pos + 8], *ino);
        buf[pos + 8] = namelen as u8;
        buf[pos + 9..pos + 9 + namelen].copy_from_slice(name);
        if has_ftype {
            buf[pos + 9 + namelen] = *ft;
        }
        // Trailing tag = this entry's byte offset within the block.
        BigEndian::write_u16(&mut buf[pos + ent - 2..pos + ent], pos as u16);
        leaf.push((dir_hashname(name), (pos >> 3) as u32));
        pos += ent;
    }

    // Free gap between the data area and the leaf section (a multiple of 8,
    // since all the pieces are 8-aligned).
    let gap_off = pos;
    let gap_len = leaf_start - pos;
    if gap_len >= 8 {
        BigEndian::write_u16(&mut buf[gap_off..gap_off + 2], 0xFFFF); // free tag
        BigEndian::write_u16(&mut buf[gap_off + 2..gap_off + 4], gap_len as u16);
        // Unused record's trailing tag = its own offset.
        BigEndian::write_u16(
            &mut buf[gap_off + gap_len - 2..gap_off + gap_len],
            gap_off as u16,
        );
        // bestfree[0] points at this gap.
        BigEndian::write_u16(&mut buf[4..6], gap_off as u16);
        BigEndian::write_u16(&mut buf[6..8], gap_len as u16);
    }

    // Leaf hash index, sorted ascending by hashval.
    leaf.sort_by_key(|&(h, _)| h);
    for (i, (h, a)) in leaf.iter().enumerate() {
        let off = leaf_start + i * 8;
        BigEndian::write_u32(&mut buf[off..off + 4], *h);
        BigEndian::write_u32(&mut buf[off + 4..off + 8], *a);
    }
    // Tail: count, stale.
    let tail = dirblksize - 8;
    BigEndian::write_u32(&mut buf[tail..tail + 4], leaf_count as u32);
    BigEndian::write_u32(&mut buf[tail + 4..tail + 8], 0);

    Ok(buf)
}

/// Walk a short-form fork into `(parent_ino, [(name, ino, ftype)])`.
fn sf_entries_with_ftype(
    fork: &[u8],
    has_ftype: bool,
) -> Result<(u64, DirEntries), FilesystemError> {
    if fork.len() < 6 {
        return Err(FilesystemError::Parse("short-form fork too small".into()));
    }
    let count = fork[0] as usize;
    let i8count = fork[1];
    let ino_width = if i8count > 0 { 8 } else { 4 };
    let read_ino = |b: &[u8]| -> u64 {
        if ino_width == 8 {
            BigEndian::read_u64(b)
        } else {
            BigEndian::read_u32(b) as u64
        }
    };
    let parent = read_ino(&fork[2..2 + ino_width]);
    let mut out = Vec::with_capacity(count);
    let mut pos = 2 + ino_width;
    for _ in 0..count {
        let namelen = fork[pos] as usize;
        let name_off = pos + 3;
        let name_end = name_off + namelen;
        let ft = if has_ftype { fork[name_end] } else { 0 };
        let ino_off = name_end + usize::from(has_ftype);
        let ino = read_ino(&fork[ino_off..ino_off + ino_width]);
        out.push((
            String::from_utf8_lossy(&fork[name_off..name_end]).to_string(),
            ino,
            ft,
        ));
        pos = ino_off + ino_width;
    }
    Ok((parent, out))
}

/// Walk a single-block directory into `(dotdot_ino, [(name, ino, ftype)])`,
/// skipping `.`/`..`.
fn block_entries_with_ftype(
    block: &[u8],
    has_ftype: bool,
) -> Result<(u64, DirEntries), FilesystemError> {
    let block_end = block.len();
    let tail = block_end - 8;
    let leaf_count = BigEndian::read_u32(&block[tail..tail + 4]) as usize;
    let data_end = block_end - (leaf_count * 8 + 8);
    let mut out = Vec::new();
    let mut dotdot = 0u64;
    let mut pos = XFS_DIR2_DATA_HDR_LEN;
    while pos + 4 <= data_end {
        let freetag = BigEndian::read_u16(&block[pos..pos + 2]);
        if freetag == 0xFFFF {
            let len = BigEndian::read_u16(&block[pos + 2..pos + 4]) as usize;
            if len == 0 {
                break;
            }
            pos += len;
            continue;
        }
        let ino = BigEndian::read_u64(&block[pos..pos + 8]);
        let namelen = block[pos + 8] as usize;
        let name = &block[pos + 9..pos + 9 + namelen];
        let ft = if has_ftype {
            block[pos + 9 + namelen]
        } else {
            0
        };
        if name == b".." {
            dotdot = ino;
        } else if name != b"." {
            out.push((String::from_utf8_lossy(name).to_string(), ino, ft));
        }
        pos += dir2_data_entsize(namelen, has_ftype);
    }
    Ok((dotdot, out))
}
