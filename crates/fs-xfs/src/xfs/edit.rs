//! XFS write primitives (§3 of `docs/xfs_edit_and_repair.md`) — the shared
//! foundation for Track-B editing (`create_directory` here) and R7 orphan
//! reconnection. Writes straight through to the device (no in-memory
//! staging), like the repair path.
//!
//! **Format coverage**: v4 and v5 (every write stamps the v5 CRCs). The editable surface first
//! refuses a volume whose features these primitives do not maintain
//! ([`super::sb::XfsSuperblock::edit_blocker`]: finobt, rmapbt, reflink, inobtcnt, sparse
//! inodes, bigtime, dir1), so on what remains (IRIX-era v4, our own v5) they keep it whole.
//!
//! Primitives, smallest first:
//!
//!   * **Inode-slot allocation** ([`alloc_inode_slot`]) / freeing
//!     ([`free_inode`]): claim/release an inode in an existing inode-btree
//!     chunk (flip `ir_free`, adjust the record freecount + AGI freecount +
//!     `sb_ifree`). When every existing chunk is full, `alloc_new_inode_chunk`
//!     carves a fresh 64-inode chunk via [`alloc_blocks_aligned_in_ag`], writes 64
//!     free dinodes, then splices a new record into the AG's inobt and bumps
//!     the AGI + superblock inode counters; the slot search then runs again
//!     and claims slot 0 of the new chunk. Two splice strategies pick
//!     themselves based on tree shape: single-leaf splice when the AGI root
//!     is a single leaf with room, multi-level grow (via
//!     [`build_sblock_btree`]) when the existing tree is multi-level or its
//!     only leaf is full — the latter reclaims the old tree blocks in the
//!     same freespace rebuild that allocates the new ones.
//!   * **Block allocation** ([`alloc_blocks`]) / freeing ([`free_blocks`]):
//!     carve/return contiguous blocks by rebuilding the AG free-space btrees
//!     (reusing the R2 rebuild) + resyncing `sb_fdblocks`.
//!   * **Directory insert** ([`dir_insert_entry`]) / remove ([`dir_remove_entry`]):
//!     short-form inline add/remove with automatic conversion to a single
//!     block on overflow. Leaf and node-form directories are edited in
//!     place by [`dir2_tree`] (written from XFS Algorithms & Data Structures:
//!     data-block free space, stale leaf slots, leaf and node splits, root
//!     push-down, freeindex blocks, root join). The full rebuild — [`plan_dir_layout`]
//!     picks the smallest form that fits and [`write_dir_layout`] lays it
//!     down — remains the path for block form, for a full leaf1 index
//!     (which converts to node form) and for shrinking a directory back
//!     down to leaf, block or short-form after removes.
//!
//! On top of these: `create_directory`, `create_file` (single contiguous
//! extent), and `delete_entry` (empty short-form dirs / single-or-no-extent
//! files). On-disk layouts per XFS Algorithms & Data Structures; behavioural
//! reference, validated against `xfs_repair -n`.

use std::io::{Read, Seek, Write};

use byteorder::{BigEndian, ByteOrder};

use super::ag::{XfsAgf, XfsAgi};
use super::bmap::{encode_extent, fsblock_to_partition_byte, NULLFSBLOCK};
use super::btree_build::{blocks_needed, blocks_needed_for, build_sblock_btree, FreeExtent};
use super::dir2::XFS_DIR2_DATA_HDR_LEN;
use super::freespace_rebuild::{carve_aligned_from_largest, carve_from_largest, coalesce};
use super::types::{XFS_DINODE_MAGIC, XFS_INODES_PER_CHUNK};
use super::{read_at_aligned, XfsFilesystem};
use disk_core::filesystem::{Filesystem, FilesystemError};
use disk_core::fsck::RepairReport;

mod block_alloc;
use dir_layout::*;
mod dir2_tree;
mod dir_edit;
mod dir_layout;
#[cfg(test)]
mod hash_from_disk_tests;
mod inode_alloc;
mod orphan;

/// Largest single-extent file we write: the on-disk blockcount field is 21 bits.
const MAX_SINGLE_EXTENT_BLOCKS: u64 = (1 << 21) - 1;

/// One bmap extent record (`xfs_bmbt_rec`) on disk.
const BMBT_REC_SIZE: usize = 16;

/// A directory's child entries as `(name, inode, ftype)`, excluding `.`/`..`.
type DirEntries = Vec<(String, u64, u8)>;

/// dir2 filetype values (only the two we emit).
const XFS_DIR3_FT_REG_FILE: u8 = 1;
const XFS_DIR3_FT_DIR: u8 = 2;

/// NULL agino sentinel stored in `di_next_unlinked` for a not-unlinked inode.
const NULLAGINO: u32 = 0xFFFF_FFFF;

/// Stamp the three v4 dinode time fields (`di_atime` @ 32..40,
/// `di_mtime` @ 40..48, `di_ctime` @ 48..56) into an inode buffer, each a
/// pair of BE u32 `(tv_sec, tv_nsec)`. `times`, when set, replaces the
/// default `now` stamp with the preserved values so a cross-fs import keeps
/// the source's mtime. `tv_nsec` is written 0 — every source we import from
/// only carries whole-second precision (tar, EFS, host-stat).
fn stamp_v4_times(buf: &mut [u8], times: Option<disk_core::times::UnixTimes>) {
    let resolved = disk_core::times::resolve_or_now(times);
    let atime = resolved.atime_or_now() as u32;
    let mtime = resolved.mtime_or_now() as u32;
    let ctime = resolved.ctime_or_now() as u32;
    BigEndian::write_u32(&mut buf[32..36], atime);
    // buf[36..40] already zero from the sweep above (tv_nsec).
    BigEndian::write_u32(&mut buf[40..44], mtime);
    // buf[44..48] zero.
    BigEndian::write_u32(&mut buf[48..52], ctime);
    // buf[52..56] zero.
}

/// inobt leaf record layout: startino(4) freecount(4) free(8) — same on v4
/// and v5.
const INOBT_REC_SIZE: usize = 16;
/// inobt key is the leading 4-byte `startino` field; same width as a btree
/// pointer.
const INOBT_KEY_SIZE: usize = 4;

/// AGI / superblock counter offsets.
const AGI_COUNT_OFF: usize = 16;
const AGI_ROOT_OFF: usize = 20;
const AGI_LEVEL_OFF: usize = 24;
const AGI_FREECOUNT_OFF: usize = 28;
const SB_ICOUNT_OFF: usize = 128;
const SB_IFREE_OFF: usize = 136;

/// `di_next_unlinked` lives 96 bytes into every dinode.
const DI_NEXT_UNLINKED_OFF: usize = 96;

/// `xfs_dir2_data_entsize`: the space one entry occupies in the notional data
/// block — `inumber(8) + namelen(1) + name + [ftype] + tag(2)`, rounded up to 8.
/// The short-form *offset cookie* of each entry is its byte offset in that
/// block, so this is how we advance the cookie when appending.
fn data_entsize(namelen: usize, has_ftype: bool) -> usize {
    let raw = 11 + namelen + usize::from(has_ftype);
    raw.div_ceil(8) * 8
}

impl<R: Read + Write + Seek + Send> XfsFilesystem<R> {
    /// Write a buffer at an absolute partition byte offset, sector-aligned.
    fn write_at(&mut self, byte_off: u64, data: &[u8]) -> Result<(), FilesystemError> {
        use std::io::SeekFrom;
        self.reader.seek(SeekFrom::Start(byte_off))?;
        self.reader.write_all(data)?;
        Ok(())
    }

    /// Stamp v5 AGI CRC tuple (uuid/lsn/crc) and write the sector. v4 just
    /// writes through. Every AGI mutation funnels through here so the CRC
    /// stays valid on v5 volumes.
    pub(crate) fn write_agi_sector(
        &mut self,
        sb: &super::sb::XfsSuperblock,
        byte_off: u64,
        sec: &mut [u8],
    ) -> Result<(), FilesystemError> {
        if sb.is_v5() {
            super::v5_crc::stamp_agi(sec, sb);
        }
        self.write_at(byte_off, sec)
    }

    /// Stamp v5 AGF CRC tuple (uuid/lsn/crc) and write the sector. v4 just
    /// writes through.
    pub(crate) fn write_agf_sector(
        &mut self,
        sb: &super::sb::XfsSuperblock,
        byte_off: u64,
        sec: &mut [u8],
    ) -> Result<(), FilesystemError> {
        if sb.is_v5() {
            super::v5_crc::stamp_agf(sec, sb);
        }
        self.write_at(byte_off, sec)
    }

    /// Stamp v5 AGFL CRC tuple (uuid/lsn/crc) and write the sector. v4 just
    /// writes through. No current edit path mutates the AGFL contents, so
    /// this helper isn't called yet — kept here so when E.5b's
    /// `alloc_blocks_aligned_in_ag` path needs to touch the AGFL (e.g. when an
    /// allocation drains the AGFL reserve and a refill becomes necessary)
    /// the stamping site is already wired.
    #[allow(dead_code)]
    pub(crate) fn write_agfl_sector(
        &mut self,
        sb: &super::sb::XfsSuperblock,
        byte_off: u64,
        sec: &mut [u8],
    ) -> Result<(), FilesystemError> {
        if sb.is_v5() {
            super::v5_crc::stamp_agfl(sec, sb);
        }
        self.write_at(byte_off, sec)
    }

    /// Stamp v5 SB CRC and write the primary superblock sector. v4 just
    /// writes through.
    pub(crate) fn write_sb_primary(
        &mut self,
        sb: &super::sb::XfsSuperblock,
        sec: &mut [u8],
    ) -> Result<(), FilesystemError> {
        if sb.is_v5() {
            super::v5_crc::stamp_superblock(sec);
        }
        self.write_at(self.partition_offset, sec)
    }

    /// Internal entry point for `create_directory`: allocate an inode, init it
    /// as an empty short-form directory, and insert it into `parent_ino`.
    /// Returns the new directory's inode number. The fit check runs before any
    /// write, so a `DiskFull` parent never leaves a dangling allocated inode.
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn do_create_directory(
        &mut self,
        parent_ino: u64,
        name: &str,
        mode: u16,
        uid: u32,
        gid: u32,
        times: Option<disk_core::times::UnixTimes>,
    ) -> Result<u64, FilesystemError> {
        let sb = self.superblock().clone();

        // Pre-flight (no writes): unique name + the parent can take the entry
        // (short-form, or short-form converting to a single block, or an
        // existing single-block dir) — so a failure leaves the volume unchanged.
        self.dir_can_insert(&sb, parent_ino, name, true)?;

        let ino = self.alloc_inode_slot(&sb)?;
        self.init_empty_shortform_dir(&sb, ino, parent_ino, mode, uid, gid, times)?;
        self.dir_insert_entry(&sb, parent_ino, name, ino, true)?;
        self.reader.flush()?;
        Ok(ino)
    }

    // ----- block allocation + create_file -----------------------------------

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

    /// Initialize a freshly-allocated inode as a regular file. `extents` is
    /// the data fork as `(startoff, fsblock, count)` records (already in
    /// file-offset order); empty for a zero-length file. When
    /// `bmbt_leaf_fsblock` is `Some`, the inode is written in btree format
    /// (§2.1 hole (D)): the inline fork carries an `xfs_bmdr_block` root that
    /// points at the single leaf block, `di_format = 3`, and `di_nblocks`
    /// includes the leaf block. Otherwise the extents are written inline
    /// (`di_format = 2`), and the caller must have bounded the count to the
    /// inline literal area.
    #[allow(clippy::too_many_arguments)]
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
        bmbt_leaf_fsblock: Option<u64>,
        times: Option<disk_core::times::UnixTimes>,
    ) -> Result<(), FilesystemError> {
        let (_core, mut buf) = self.read_inode_buf(ino)?;
        let version = buf[4];
        let gen = BigEndian::read_u32(&buf[92..96]).wrapping_add(1);
        let isz = buf.len();
        let use_btree = bmbt_leaf_fsblock.is_some();

        BigEndian::write_u16(&mut buf[0..2], super::types::XFS_DINODE_MAGIC);
        BigEndian::write_u16(&mut buf[2..4], mode);
        buf[4] = version;
        buf[5] = if use_btree { 3 } else { 2 }; // di_format: btree or extents
        if version == 1 {
            BigEndian::write_u16(&mut buf[6..8], 1);
            BigEndian::write_u32(&mut buf[16..20], 0);
        } else {
            BigEndian::write_u16(&mut buf[6..8], 0);
            BigEndian::write_u32(&mut buf[16..20], 1);
        }
        BigEndian::write_u32(&mut buf[8..12], uid);
        BigEndian::write_u32(&mut buf[12..16], gid);
        // Zero the header slice up through the end of the time fields; the
        // three di_atime/di_mtime/di_ctime pairs at 32..56 then get real
        // seconds (preserved from a cross-fs copy or freshly stamped for a
        // new file). tv_nsec is left at zero — XFS carries nanoseconds on
        // disk but every producer we import from (tar / EFS / host stat on
        // second precision) rounds to whole seconds anyway.
        for b in buf.iter_mut().take(56).skip(20) {
            *b = 0;
        }
        stamp_v4_times(&mut buf, times);
        BigEndian::write_u64(&mut buf[56..64], size);
        // di_nblocks = data extent blocks + (bmbt leaf block, if any).
        let data_nblocks: u64 = extents.iter().map(|&(_, _, c)| c as u64).sum();
        let bmbt_nblocks: u64 = if use_btree { 1 } else { 0 };
        BigEndian::write_u64(&mut buf[64..72], data_nblocks + bmbt_nblocks);
        for b in buf.iter_mut().take(76).skip(72) {
            *b = 0; // extsize
        }
        // di_nextents: total file-offset extents (same value in both formats).
        BigEndian::write_u32(&mut buf[76..80], extents.len() as u32);
        for b in buf.iter_mut().take(92).skip(80) {
            *b = 0; // anextents, forkoff, aformat(set below), dm*, flags
        }
        buf[83] = 2; // di_aformat = extents (no attr fork)
        BigEndian::write_u32(&mut buf[92..96], gen);
        BigEndian::write_u32(&mut buf[96..100], NULLAGINO);

        // v3 inode core: di_changecount / di_lsn / di_flags2 / di_cowextsize
        // / di_pad2 / di_crtime / di_ino / di_uuid live at offsets 104..176.
        // `write_inode_region` stamps di_crc + di_uuid + di_ino + di_lsn +
        // bumps di_changecount; we zero the v3 fields here so a freshly-init
        // inode doesn't inherit a previous occupant's flags/crtime.
        if sb.is_v5() {
            for b in buf.iter_mut().take(176).skip(100) {
                *b = 0;
            }
        }

        // Fork starts at byte 100 in v4 (`fork_offset(false)`) and at byte
        // 176 in v5 (`fork_offset(true)` — the v3 inode core is 176 bytes).
        let fs = sb.fork_offset();
        for b in buf.iter_mut().take(isz).skip(fs) {
            *b = 0;
        }
        if let Some(leaf_fsblock) = bmbt_leaf_fsblock {
            // Btree format: write an in-inode root pointing at the leaf.
            let first_startoff = extents.first().map(|&(off, _, _)| off).ok_or_else(|| {
                FilesystemError::Parse("bmbt root requires at least one extent".into())
            })?;
            write_bmbt_root_to_leaf(&mut buf[fs..isz], first_startoff, leaf_fsblock);
        } else {
            // Inline extents: each record must fit the literal area.
            if fs + extents.len() * BMBT_REC_SIZE > isz {
                return Err(FilesystemError::Unsupported(
                    "too many extents for an inline data fork (bmap-btree not threaded here)"
                        .into(),
                ));
            }
            for (i, &(startoff, fsblock, count)) in extents.iter().enumerate() {
                let rec = encode_extent(false, startoff, fsblock, count as u64);
                buf[fs + i * BMBT_REC_SIZE..fs + (i + 1) * BMBT_REC_SIZE].copy_from_slice(&rec);
            }
        }
        self.write_inode_region(sb, ino, &buf)
    }

    /// Internal entry point for `create_file`: pre-flight the parent insert,
    /// allocate the data blocks (single contiguous extent OR multiple runs,
    /// possibly larger than one extent record), write the data, allocate +
    /// initialize the file inode, then insert it into the parent. When the
    /// resulting extent records overflow the inline literal area, the data
    /// fork is laid out as a single-leaf bmap btree (§2.1 hole (D)). Returns
    /// the new file's inode number.
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
        times: Option<disk_core::times::UnixTimes>,
    ) -> Result<u64, FilesystemError> {
        let sb = self.superblock().clone();
        self.dir_can_insert(&sb, parent_ino, name, false)?;

        let bs = sb.blocksize as u64;
        let nblocks = data_len.div_ceil(bs);

        // Allocate data first (so a DiskFull leaves no half-built inode). The
        // allocator returns AG-local runs; runs_to_extent_records splits any
        // run larger than `MAX_SINGLE_EXTENT_BLOCKS` into multiple consecutive
        // extent records (the on-disk blockcount field is only 21 bits).
        let runs = if nblocks > 0 {
            self.alloc_extents(&sb, nblocks)?
        } else {
            Vec::new()
        };
        let extents = runs_to_extent_records(&runs);

        // Pick between inline and btree data-fork format. v1 supports single-
        // leaf bmbt only: leaves beyond the leaf cap force a clear error so
        // the partial allocation is reclaimed before the operation aborts.
        let max_inline = (sb.inodesize as usize - sb.fork_offset()) / BMBT_REC_SIZE;
        let leaf_max = bmbt_leaf_max(&sb);
        if extents.len() > leaf_max {
            // Roll back the data allocation.
            self.free_blocks(&sb, &runs)?;
            return Err(FilesystemError::Unsupported(format!(
                "file needs {} extents; multi-leaf bmap btree not yet implemented (max {})",
                extents.len(),
                leaf_max
            )));
        }
        let bmbt_leaf_fsblock = if extents.len() > max_inline {
            // Allocate one block for the leaf; on failure, roll back the data.
            match self.alloc_blocks(&sb, 1) {
                Ok(fb) => Some(fb),
                Err(e) => {
                    self.free_blocks(&sb, &runs)?;
                    return Err(e);
                }
            }
        } else {
            None
        };

        // From here on, every failure leaves an inode-shaped on-disk
        // commitment — that's the existing contract, so don't bend the
        // ordering to try to rescue it.
        let ino = self.alloc_inode_slot(&sb)?;
        self.write_file_data_extents(&sb, &extents, data, data_len)?;
        if let Some(leaf_fsblock) = bmbt_leaf_fsblock {
            let mut leaf_bytes = build_bmbt_leaf(&extents, &sb);
            if sb.is_v5() {
                // v5 lblock-crc tuple — `bb_owner` is the file's own inode.
                let blkno = super::v5_crc::fsblock_to_daddr(leaf_fsblock, &sb);
                super::v5_crc::stamp_lblock_crc_header(&mut leaf_bytes, blkno, ino, &sb);
            }
            self.write_fsblock(&sb, leaf_fsblock, &leaf_bytes)?;
        }
        self.init_file_inode(
            &sb,
            ino,
            mode,
            uid,
            gid,
            data_len,
            &extents,
            bmbt_leaf_fsblock,
            times,
        )?;
        self.dir_insert_entry(&sb, parent_ino, name, ino, false)?;
        self.reader.flush()?;
        Ok(ino)
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
        if !child_is_dir && cc.nlink > 1 {
            // Another name still links the file: drop this entry and one link only (XFS-005).
            self.dir_remove_entry(&sb, parent_ino, name, false)?;
            self.bump_dir_nlink(&sb, child_ino, -1)?;
            self.reader.flush()?;
            return Ok(());
        }
        let attr: Vec<(u64, u32)> = self
            .attr_fork_blocks(&cc, &cbuf)?
            .into_iter()
            .map(|(start, count)| (start, count as u32))
            .collect();
        let mut extents: Vec<(u64, u32)> = match cc.format {
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
        extents.extend(attr); // the attribute fork's blocks go with the inode (XFS-004)

        self.dir_remove_entry(&sb, parent_ino, name, child_is_dir)?;
        if !extents.is_empty() {
            self.free_blocks(&sb, &extents)?;
        }
        self.free_inode(&sb, child_ino)?;
        self.reader.flush()?;
        Ok(())
    }

    /// Rename `old_name` -> `new_name` within the same parent directory. The
    /// child inode (and its data) is untouched; only the parent's directory
    /// entry changes.
    ///
    /// Both directory primitives adjust the parent's `nlink` by ±1 when the
    /// child is a directory (the `..` back-reference). For a same-parent rename
    /// that must net to zero, so we pass the child's *real* `is_dir` to both the
    /// insert and the remove. We add the new entry first, then remove the old,
    /// so a mid-operation failure never leaves the child unreferenced.
    pub(crate) fn do_rename(
        &mut self,
        parent_ino: u64,
        old_name: &str,
        new_name: &str,
        child_ino: u64,
    ) -> Result<(), FilesystemError> {
        if old_name == new_name {
            return Ok(());
        }
        if new_name.is_empty() {
            return Err(FilesystemError::InvalidData("name cannot be empty".into()));
        }
        let sb = self.superblock().clone();
        let (cc, _cbuf) = self.read_inode_buf(child_ino)?;
        let is_dir = cc.is_dir();
        // Pre-flight (unique name, parent editable, fits) before any mutation.
        self.dir_can_insert(&sb, parent_ino, new_name, is_dir)?;
        self.dir_insert_entry(&sb, parent_ino, new_name, child_ino, is_dir)?;
        self.dir_remove_entry(&sb, parent_ino, old_name, is_dir)?;
        self.reader.flush()?;
        Ok(())
    }

    // ----- format-aware directory insert (short-form OR single-block) --------
}

/// Directory name hash, per XFS Algorithms & Data Structures, ch. Variable Length
/// Record B+trees: four bytes per round into a 28-bit-rotated hash, then the 1-3 byte tail.
fn dir_hashname(name: &[u8]) -> u32 {
    let mut hash: u32 = 0;
    let mut quads = name.chunks_exact(4);
    for q in &mut quads {
        hash = (u32::from(q[0]) << 21)
            ^ (u32::from(q[1]) << 14)
            ^ (u32::from(q[2]) << 7)
            ^ u32::from(q[3])
            ^ hash.rotate_left(28);
    }
    match *quads.remainder() {
        [a, b, c] => {
            (u32::from(a) << 14) ^ (u32::from(b) << 7) ^ u32::from(c) ^ hash.rotate_left(21)
        }
        [a, b] => (u32::from(a) << 7) ^ u32::from(b) ^ hash.rotate_left(14),
        [a] => u32::from(a) ^ hash.rotate_left(7),
        _ => hash,
    }
}

/// Data-block size one directory entry occupies: `inumber(8) + namelen(1) +
/// name + [ftype] + tag(2)`, rounded up to 8.
fn dir2_data_entsize(namelen: usize, has_ftype: bool) -> usize {
    let raw = 8 + 1 + namelen + usize::from(has_ftype) + 2;
    raw.div_ceil(8) * 8
}

/// Maximum extent records in one bmap btree leaf block — what fits after
/// the long-form `xfs_btree_block` header. v5 has 48 fewer payload bytes.
fn bmbt_leaf_max(sb: &super::sb::XfsSuperblock) -> usize {
    (sb.blocksize as usize - sb.lblock_hdr_len()) / BMBT_REC_SIZE
}

/// Convert raw `(fsblock, count)` allocation runs into bmap extent records
/// `(startoff, fsblock, count)`, splitting any run whose `count` exceeds the
/// 21-bit on-disk `blockcount` field into multiple consecutive records that
/// describe the same physical blocks. File offsets advance sequentially over
/// the runs (the data-fork layout assumed by every other write path here).
fn runs_to_extent_records(runs: &[(u64, u32)]) -> Vec<(u64, u64, u32)> {
    let mut out: Vec<(u64, u64, u32)> = Vec::with_capacity(runs.len());
    let mut file_off = 0u64;
    for &(fsblock, count) in runs {
        let mut remaining = count as u64;
        let mut start_fsblock = fsblock;
        while remaining > 0 {
            let chunk = remaining.min(MAX_SINGLE_EXTENT_BLOCKS);
            out.push((file_off, start_fsblock, chunk as u32));
            file_off += chunk;
            start_fsblock += chunk;
            remaining -= chunk;
        }
    }
    out
}

/// Build a bmap btree leaf block holding every extent record; sibling
/// pointers are `NULLFSBLOCK` since hole (D) only supports a single leaf.
/// The caller has guaranteed `extents.len() <= bmbt_leaf_max(sb)`.
///
/// v4 (`XFS_BMAP_MAGIC`): 24-byte hdr (magic + level + numrecs + leftsib +
/// rightsib), records pack from byte 24.
///
/// v5 (`XFS_BMAP_CRC_MAGIC`): 72-byte hdr — the v4 prefix at bytes 0..24,
/// then `bb_blkno`/`bb_lsn`/`bb_uuid`/`bb_owner`/`bb_crc` at bytes 24..72.
/// The caller stamps the CRC tuple (`v5_crc::stamp_lblock_crc_header`)
/// after this builder returns, once the leaf's destination fsblock is
/// known.
fn build_bmbt_leaf(extents: &[(u64, u64, u32)], sb: &super::sb::XfsSuperblock) -> Vec<u8> {
    let bs = sb.blocksize as usize;
    let mut buf = vec![0u8; bs];
    BigEndian::write_u32(&mut buf[0..4], sb.bmbt_magic());
    BigEndian::write_u16(&mut buf[4..6], 0); // level 0 = leaf
    BigEndian::write_u16(&mut buf[6..8], extents.len() as u16);
    BigEndian::write_u64(&mut buf[8..16], NULLFSBLOCK); // leftsib
    BigEndian::write_u64(&mut buf[16..24], NULLFSBLOCK); // rightsib
    let hdr = sb.lblock_hdr_len();
    for (i, &(off, fsblock, count)) in extents.iter().enumerate() {
        let rec = encode_extent(false, off, fsblock, count as u64);
        let pos = hdr + i * BMBT_REC_SIZE;
        buf[pos..pos + BMBT_REC_SIZE].copy_from_slice(&rec);
    }
    buf
}

/// Write the in-inode `xfs_bmdr_block` root pointing at a single leaf. Layout:
/// `level(2) + numrecs(2)` then `maxrecs` keys (each 8 bytes — `startoff`) then
/// `maxrecs` pointers (each 8 bytes — leaf fsblock). Only the level-1 root
/// case with `numrecs=1` is in scope. `maxrecs = (fork_len - 4) / 16` matches
/// the read-side `parse_bmbt_root`.
fn write_bmbt_root_to_leaf(fork: &mut [u8], first_startoff: u64, leaf_fsblock: u64) {
    write_bmbt_root(fork, &[(first_startoff, leaf_fsblock)]);
}

/// Leaves an in-inode bmbt root of `fork_len` bytes can point at.
fn bmbt_root_max(fork_len: usize) -> usize {
    fork_len.saturating_sub(4) / 16
}

/// Write a level-1 in-inode bmbt root: `(first startoff, leaf fsblock)` per
/// leaf, keys packed first and pointers after the key area (`xfs_bmdr_block`).
fn write_bmbt_root(fork: &mut [u8], leaves: &[(u64, u64)]) {
    BigEndian::write_u16(&mut fork[0..2], 1); // level 1 (above leaves)
    BigEndian::write_u16(&mut fork[2..4], leaves.len() as u16); // numrecs
    let maxrecs = bmbt_root_max(fork.len());
    let ptrs_off = 4 + maxrecs * 8;
    for (i, &(startoff, fsblock)) in leaves.iter().enumerate() {
        BigEndian::write_u64(&mut fork[4 + i * 8..12 + i * 8], startoff);
        BigEndian::write_u64(&mut fork[ptrs_off + i * 8..ptrs_off + i * 8 + 8], fsblock);
    }
}

#[cfg(test)]
mod tests {
    //! Unit tests for the dir-block builders. The integration round-trip on
    //! the v5 fixture lives in `super::tests` (see
    //! `dir3_block_builder_round_trips_through_dir2_reader` etc.).
    use super::*;
    use crate::xfs::dir2;

    /// A name that is not UTF-8 makes a directory rebuild refuse rather than rewrite it (XFS-006).
    #[test]
    fn strict_name_refuses_non_utf8() {
        assert_eq!(strict_name(b"caf\xc3\xa9").unwrap(), "caf\u{e9}");
        assert!(matches!(
            strict_name(b"caf\xe9"),
            Err(FilesystemError::Unsupported(_))
        ));
    }

    /// Look up the `XfsSuperblock` constants we need to fabricate a
    /// stamper-ready superblock without an actual on-disk image.
    fn fake_v5_sb(meta_uuid_byte: u8) -> crate::xfs::sb::XfsSuperblock {
        crate::xfs::sb::XfsSuperblock {
            magicnum: 0x5846_5342,
            blocksize: 4096,
            dblocks: 0,
            rblocks: 0,
            rextents: 0,
            logstart: 0,
            logblocks: 0,
            rootino: 0,
            rbmino: 0,
            rsumino: 0,
            uquotino: 0,
            gquotino: 0,
            agblocks: 0,
            agcount: 0,
            versionnum: 0x0005,
            sectsize: 512,
            inodesize: 512,
            inopblock: 8,
            fname: [0; 12],
            blocklog: 12,
            sectlog: 9,
            inodelog: 9,
            inopblog: 3,
            agblklog: 16,
            dirblklog: 0,
            inoalignmt: 0,
            features2: 0,
            features_compat: 0,
            features_ro_compat: 0,
            features_incompat: 0,
            sb_uuid: [meta_uuid_byte; 16],
            sb_meta_uuid: [meta_uuid_byte; 16],
        }
    }

    /// §2.1 hole (E.3): `build_block_dir` with `is_v5=true` produces an XDB3
    /// header, the v5 stamper validates, and the dir2 reader (which already
    /// knows dir3) walks every entry back.
    #[test]
    fn build_block_dir_v5_round_trips_through_reader_after_stamp() {
        let sb = fake_v5_sb(0x77);
        let entries = vec![
            ("alpha".to_string(), 0x1100u64, super::XFS_DIR3_FT_REG_FILE),
            ("beta".to_string(), 0x1200u64, super::XFS_DIR3_FT_DIR),
            ("gamma".to_string(), 0x1300u64, super::XFS_DIR3_FT_REG_FILE),
        ];
        let dirblksize = 4096;
        let self_ino = 0xABCD;
        let dotdot = 0xBEEF;
        let has_ftype = true;
        let mut buf =
            build_block_dir(&entries, self_ino, dotdot, dirblksize, has_ftype, true).unwrap();

        // Magic = XDB3 (v5 single-block dir).
        assert_eq!(
            BigEndian::read_u32(&buf[0..4]),
            dir2::XFS_DIR3_BLOCK_MAGIC,
            "expected XDB3 magic"
        );

        // Stamp the xfs_dir3_blk_hdr — owner = parent / self_ino, blkno = some
        // synthetic daddr. After stamping, crc_valid must return true.
        let blkno = 0xDEAD_BEEFu64;
        crate::xfs::v5_crc::stamp_dir3_blk_hdr(&mut buf, blkno, self_ino, &sb);
        assert!(
            crate::xfs::v5_crc::crc_valid(&buf, crate::xfs::v5_crc::DIR3_BLK_CRC_OFF),
            "dir3 block CRC mismatch after build+stamp"
        );

        // Round-trip via the dir2 reader, which dispatches by magic and
        // handles dir3's 64-byte header.
        let parsed = dir2::parse_block(&buf, has_ftype).expect("reader walks XDB3 block");
        let names: Vec<&str> = parsed.iter().map(|e| e.name.as_str()).collect();
        for n in ["alpha", "beta", "gamma"] {
            assert!(names.contains(&n), "missing entry {n} in {names:?}");
        }
    }

    /// `build_leaf_data_block` v5 — XDD3 magic, bestfree[0] at offset 48,
    /// stamper validates.
    #[test]
    fn build_leaf_data_block_v5_stamps_and_parses() {
        let sb = fake_v5_sb(0x88);
        let entries = vec![
            ("alpha".to_string(), 0x2100u64, super::XFS_DIR3_FT_REG_FILE),
            ("beta".to_string(), 0x2200u64, super::XFS_DIR3_FT_REG_FILE),
        ];
        let mut data = build_leaf_data_block(
            &entries, 0x222, // self_ino (only used when extra_dot_dotdot is true)
            0x111, // dotdot
            1,     // file_block_in_dir
            4096,  // dirblksize
            true,  // has_ftype
            false, // no `.`/`..` for non-first data block
            true,  // v5
        )
        .unwrap();
        let block = &mut data.bytes;
        assert_eq!(BigEndian::read_u32(&block[0..4]), dir2::XFS_DIR3_DATA_MAGIC);

        // bestfree[0] = (offset, length) at bytes 48..52 on v5.
        let bf_off = BigEndian::read_u16(&block[48..50]);
        let bf_len = BigEndian::read_u16(&block[50..52]);
        assert!(bf_off >= 64, "bestfree[0].offset should sit past v5 header");
        assert!(bf_len > 0, "bestfree[0].length should be the tail gap");

        let blkno = 0xCAFE_BABEu64;
        crate::xfs::v5_crc::stamp_dir3_blk_hdr(block, blkno, 0x111, &sb);
        assert!(
            crate::xfs::v5_crc::crc_valid(block, crate::xfs::v5_crc::DIR3_BLK_CRC_OFF),
            "XDD3 CRC mismatch after stamp"
        );

        // dir2 reader's parse_data_block already handles the XDD3 / 64-byte
        // header — confirm both entries surface.
        let parsed = dir2::parse_data_block(block, true).expect("walk XDD3 data block");
        let names: Vec<&str> = parsed.iter().map(|e| e.name.as_str()).collect();
        for n in ["alpha", "beta"] {
            assert!(names.contains(&n), "missing entry {n} in {names:?}");
        }
    }

    /// `build_leaf1_block` v5 — magic 0x3DF1 at offset 8..10 in the
    /// `xfs_da3_blkinfo`, 56-byte header before the entries, stamper
    /// validates.
    #[test]
    fn build_leaf1_block_v5_stamps_at_da3_offset() {
        let sb = fake_v5_sb(0x99);
        let entries = vec![(0x1111, 0x100), (0x2222, 0x200), (0x3333, 0x300)];
        let bests = [256u16, 512u16];
        let mut buf = build_leaf1_block(&entries, &bests, 4096, true).unwrap();
        // Entries start after the 64-byte xfs_dir3_leaf_hdr (pad included);
        // bests sit before the u32 bestcount tail, in data-block order.
        assert_eq!(BigEndian::read_u32(&buf[64..68]), 0x1111);
        assert_eq!(BigEndian::read_u32(&buf[4096 - 4..]), 2, "bestcount");
        assert_eq!(BigEndian::read_u16(&buf[4096 - 8..4096 - 6]), 256);
        assert_eq!(BigEndian::read_u16(&buf[4096 - 6..4096 - 4]), 512);

        // Magic at offset 8..10 = XFS_DIR3_LEAF1_MAGIC.
        assert_eq!(
            BigEndian::read_u16(&buf[8..10]),
            XFS_DIR3_LEAF1_MAGIC,
            "expected 0x3DF1 dir3 leaf1 magic"
        );
        // ldlh `count` lives at offset 56 (right after the 56-byte da3 hdr).
        assert_eq!(
            BigEndian::read_u16(&buf[56..58]),
            3,
            "leaf entry count should round-trip"
        );

        let blkno = 0xFEED_FACEu64;
        crate::xfs::v5_crc::stamp_da3_blkinfo(&mut buf, blkno, 0x222, &sb);
        assert!(
            crate::xfs::v5_crc::crc_valid(&buf, crate::xfs::v5_crc::DA3_CRC_OFF),
            "da3_blkinfo CRC mismatch after stamp"
        );
    }

    /// F-018's deep run hit the 21-extent inline cap: the directory's extent
    /// list now moves to a bmap-btree leaf and comes back inline, leaf freed.
    #[test]
    fn directory_extent_list_moves_to_a_bmbt_leaf_and_back() {
        use disk_core::filesystem::{CreateDirectoryOptions, EditableFilesystem, Filesystem};
        let img = crate::xfs::format::create_blank_xfs(32 << 20, "bmbt").unwrap();
        let mut fs = XfsFilesystem::open(std::io::Cursor::new(img), 0).unwrap();
        let root = fs.root().unwrap();
        let dir = fs
            .create_directory(&root, "d", &CreateDirectoryOptions::default())
            .unwrap();
        let sb = fs.sb.clone();
        let fdblocks = |fs: &mut XfsFilesystem<std::io::Cursor<Vec<u8>>>| -> u64 {
            let mut primary = vec![0u8; sb.sectsize as usize];
            read_at_aligned(
                &mut fs.reader,
                fs.partition_offset,
                sb.sectsize as u64,
                &mut primary,
            )
            .unwrap();
            BigEndian::read_u64(&primary[144..152]) // sb_fdblocks
        };
        // Single-block runs allocated so no two coalesce: more than one
        // 4 KiB leaf holds (251 records), so the root points at two leaves.
        let n = bmbt_leaf_max(&sb) as u64 + 40;
        let mut extents: Vec<(u64, u64, u32)> = Vec::new();
        for i in 0..n {
            let b = fs.alloc_blocks(&sb, 1).unwrap();
            extents.push((i, b, 1));
        }
        let free_with_runs = fdblocks(&mut fs);
        let size = n * u64::from(sb.dirblksize());
        fs.write_dir_inode_multi_extent(&sb, dir.location, &extents, size, n)
            .unwrap();
        let (core, buf) = fs.read_inode_buf(dir.location).unwrap();
        assert_eq!(core.format, super::super::types::DiFormat::Btree);
        assert_eq!(core.nextents as u64, n);
        assert_eq!(core.nblocks, n + 2, "both bmbt leaves count in di_nblocks");
        assert_eq!(fs.dir_bmbt_leaves(&sb, &core, &buf).unwrap().len(), 2);
        let back: Vec<(u64, u64, u32)> = fs
            .decode_data_extents(&core, &buf)
            .unwrap()
            .iter()
            .map(|e| (e.startoff, e.startblock, e.blockcount as u32))
            .collect();
        assert_eq!(back, extents, "records round-trip through the leaf");
        assert_eq!(
            fdblocks(&mut fs),
            free_with_runs - 2,
            "two blocks went to leaves"
        );
        // Shrinking to three extents goes back inline and returns the leaf.
        fs.write_dir_inode_multi_extent(&sb, dir.location, &extents[..3], size, 3)
            .unwrap();
        let (core, buf) = fs.read_inode_buf(dir.location).unwrap();
        assert_eq!(core.format, super::super::types::DiFormat::Extents);
        assert_eq!(core.nblocks, 3);
        assert_eq!(fs.decode_data_extents(&core, &buf).unwrap().len(), 3);
        assert_eq!(
            fdblocks(&mut fs),
            free_with_runs,
            "the leaf blocks are free again"
        );
    }
}
