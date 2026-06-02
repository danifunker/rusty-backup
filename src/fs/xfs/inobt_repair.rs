//! R3 — in-place inode-allocation-btree (`inobt`) repair
//! (`docs/xfs_edit_and_repair.md` §5).
//!
//! The inobt's leaf records carry, per 64-inode chunk, a `freecount` and a
//! 64-bit `free` mask (set bit = free inode). Two corruptions are common and
//! fully recoverable *in place*, because the chunk's inodes on disk are the
//! authority:
//!
//!   * **freecount ≠ popcount(free mask)** — a stale/garbled summary field.
//!   * **a free-mask bit disagreeing with the inode** — the mask claims an
//!     allocated inode is free (or vice-versa).
//!
//! For every inobt leaf record we recompute the free mask straight from the
//! inodes (`di_mode == 0` ⇒ free, except the superblock's internal inodes
//! which are always allocated) and the freecount from that mask, rewrite the
//! changed leaves in their existing blocks, then fix the AGI `count`/`freecount`
//! summaries and the superblock `sb_icount`/`sb_ifree` counters. Nothing moves,
//! so this is decoupled from the free-space btrees (R2) — an inode being free
//! or not does not change which blocks the chunk occupies.
//!
//! This slice requires a **walkable** inobt (we descend it to find the leaf
//! blocks). A trashed inobt *structure* — bad magic/pointers — needs full
//! inode-chunk rediscovery by scanning and is deliberately out of scope here;
//! we report and skip such an AG, writing nothing.
//!
//! v4 only: v5 inobt blocks carry a CRC we don't recompute, so rewriting a
//! leaf there would invalidate it. Porting reference: `xfs_repair` @ v3.1.11
//! `repair/dino_chunks.c` (free-mask derivation) + `repair/phase5.c`
//! (`build_ino_tree` / AGI counters).

use std::collections::HashSet;
use std::io::{Read, Seek, SeekFrom, Write};

use byteorder::{BigEndian, ByteOrder};

use super::ag::XfsAgi;
use super::bmap::fsblock_to_partition_byte;
use super::sb::XfsSuperblock;
use super::types::{NULLAGBLOCK, XFS_IBT_MAGIC, XFS_INODES_PER_CHUNK};
use super::{read_at_aligned, XfsFilesystem};
use crate::fs::filesystem::FilesystemError;
use crate::fs::fsck::RepairReport;

/// inobt leaf record layout (v4 short header): startino(4) freecount(4) free(8).
const INOBT_REC_SIZE: usize = 16;
const INOBT_HDR_LEN: usize = 16; // XFS_BTREE_SBLOCK_LEN
const REC_FREECOUNT_OFF: usize = 4;
const REC_FREE_OFF: usize = 8;

/// AGI on-disk field offsets within the AGI sector.
const AGI_COUNT_OFF: usize = 16;
const AGI_FREECOUNT_OFF: usize = 28;

/// Superblock counter offsets.
const SB_ICOUNT_OFF: usize = 128;
const SB_IFREE_OFF: usize = 136;

/// di_mode is a big-endian u16 at byte 2 of every inode.
const DI_MODE_OFF: usize = 2;

impl<R: Read + Write + Seek + Send> XfsFilesystem<R> {
    /// R3 entry point: recompute every AG's inobt leaf free masks from the
    /// inodes and rewrite the changed leaves + AGI/superblock counters. A
    /// no-op on a healthy volume; writes nothing for an AG whose inobt is not
    /// walkable (reported, left to a future structure-rebuild).
    pub(crate) fn run_inobt_repair(&mut self) -> Result<RepairReport, FilesystemError> {
        let mut report = RepairReport {
            fixes_applied: Vec::new(),
            fixes_failed: Vec::new(),
            unrepairable_count: 0,
        };
        let sb = self.superblock().clone();
        if sb.is_v5() {
            // v5 inobt blocks are CRC-protected; rewriting would invalidate
            // them. R3 is v4-only (silent, not a failure).
            return Ok(report);
        }

        let internal: HashSet<u64> = sb.internal_inodes().into_iter().collect();
        let sectsize = sb.sectsize as u64;
        let agblocks = sb.agblocks as u64;
        let blocksize = sb.blocksize as u64;

        let mut any_change = false;

        for agno in 0..sb.agcount as u64 {
            let ag_byte = self.partition_offset + agno * agblocks * blocksize;
            let agi_byte = ag_byte + 2 * sectsize;
            let mut agi_sec = vec![0u8; sectsize as usize];
            if read_at_aligned(&mut self.reader, agi_byte, sectsize, &mut agi_sec).is_err() {
                report
                    .fixes_failed
                    .push(format!("AG {agno}: AGI unreadable; inobt repair skipped"));
                continue;
            }
            let agi = match XfsAgi::parse(&agi_sec) {
                Ok(a) => a,
                Err(_) => {
                    report
                        .fixes_failed
                        .push(format!("AG {agno}: AGI unparseable; inobt repair skipped"));
                    continue;
                }
            };

            let leaves = match self.collect_inobt_leaf_blocks(&sb, agno, agi.root) {
                Ok(l) => l,
                Err(e) => {
                    // Unwalkable structure: defer to a future rebuild, no write.
                    report.fixes_failed.push(format!(
                        "AG {agno}: inobt not walkable ({e}); structure rebuild not implemented, skipped"
                    ));
                    continue;
                }
            };

            let mut ag_count: u64 = 0;
            let mut ag_freecount: u64 = 0;
            let mut leaves_fixed = 0u32;
            let mut ag_failed = false;

            for leaf_agbno in leaves {
                match self.repair_inobt_leaf(&sb, agno, leaf_agbno, &internal) {
                    Ok(LeafResult {
                        count,
                        freecount,
                        rewritten,
                    }) => {
                        ag_count += count;
                        ag_freecount += freecount;
                        if rewritten {
                            leaves_fixed += 1;
                        }
                    }
                    Err(e) => {
                        report.fixes_failed.push(format!(
                            "AG {agno}: inobt leaf {leaf_agbno} repair failed: {e}"
                        ));
                        ag_failed = true;
                        break;
                    }
                }
            }
            if ag_failed {
                continue;
            }

            if leaves_fixed > 0 {
                any_change = true;
                report.fixes_applied.push(format!(
                    "AG {agno}: recomputed {leaves_fixed} inobt leaf free-mask(s) from inodes"
                ));
            }

            // Fix the AGI summary counters from the recomputed totals.
            if ag_count as u32 != agi.count || ag_freecount as u32 != agi.freecount {
                BigEndian::write_u32(
                    &mut agi_sec[AGI_COUNT_OFF..AGI_COUNT_OFF + 4],
                    ag_count as u32,
                );
                BigEndian::write_u32(
                    &mut agi_sec[AGI_FREECOUNT_OFF..AGI_FREECOUNT_OFF + 4],
                    ag_freecount as u32,
                );
                self.reader.seek(SeekFrom::Start(agi_byte))?;
                self.reader.write_all(&agi_sec)?;
                any_change = true;
                report
                    .fixes_applied
                    .push(format!("AG {agno}: corrected AGI count/freecount"));
            }
        }

        // Resync the global inode counters from a fresh sum of every AGI's
        // (now-corrected) count/freecount. Done as a final pass rather than
        // accumulated inline so a skipped/unwalkable AG can't undercount the
        // total — its AGI is still read here.
        if any_change && self.resync_sb_inode_counts(&sb)? {
            report
                .fixes_applied
                .push("superblock: resynced sb_icount/sb_ifree".into());
        }

        self.reader.flush()?;
        Ok(report)
    }

    /// Recompute `sb_icount`/`sb_ifree` as the sum of every AGI's count /
    /// freecount and write the primary superblock if they changed. Returns
    /// whether a write happened.
    fn resync_sb_inode_counts(&mut self, sb: &XfsSuperblock) -> Result<bool, FilesystemError> {
        let sectsize = sb.sectsize as u64;
        let agblocks = sb.agblocks as u64;
        let blocksize = sb.blocksize as u64;
        let mut icount: u64 = 0;
        let mut ifree: u64 = 0;
        let mut agi_sec = vec![0u8; sectsize as usize];
        for agno in 0..sb.agcount as u64 {
            let agi_byte = self.partition_offset + agno * agblocks * blocksize + 2 * sectsize;
            read_at_aligned(&mut self.reader, agi_byte, sectsize, &mut agi_sec)?;
            let agi = XfsAgi::parse(&agi_sec)?;
            icount += agi.count as u64;
            ifree += agi.freecount as u64;
        }
        let mut primary = vec![0u8; sectsize as usize];
        read_at_aligned(
            &mut self.reader,
            self.partition_offset,
            sectsize,
            &mut primary,
        )?;
        let cur_icount = BigEndian::read_u64(&primary[SB_ICOUNT_OFF..SB_ICOUNT_OFF + 8]);
        let cur_ifree = BigEndian::read_u64(&primary[SB_IFREE_OFF..SB_IFREE_OFF + 8]);
        if cur_icount == icount && cur_ifree == ifree {
            return Ok(false);
        }
        BigEndian::write_u64(&mut primary[SB_ICOUNT_OFF..SB_ICOUNT_OFF + 8], icount);
        BigEndian::write_u64(&mut primary[SB_IFREE_OFF..SB_IFREE_OFF + 8], ifree);
        self.reader.seek(SeekFrom::Start(self.partition_offset))?;
        self.reader.write_all(&primary)?;
        Ok(true)
    }

    /// Descend the inobt from `root`, returning the AG-relative block numbers
    /// of its leaf (level 0) blocks. Errors on bad magic, an over-long node, or
    /// a cycle — the "unwalkable structure" signal the caller skips on.
    fn collect_inobt_leaf_blocks(
        &mut self,
        sb: &XfsSuperblock,
        agno: u64,
        root: u32,
    ) -> Result<Vec<u32>, FilesystemError> {
        let bs = sb.blocksize as usize;
        let max_intern = (bs - INOBT_HDR_LEN) / (4 + 4); // 4-byte key + 4-byte ptr
        let mut block = vec![0u8; bs];
        let mut leaves = Vec::new();
        let mut stack = vec![root];
        let mut visited: HashSet<u32> = HashSet::new();

        while let Some(agbno) = stack.pop() {
            if agbno == NULLAGBLOCK || agbno as u64 >= sb.agblocks as u64 {
                continue;
            }
            if !visited.insert(agbno) {
                return Err(FilesystemError::Parse(format!(
                    "AG {agno} inobt block {agbno} visited twice (cycle)"
                )));
            }
            let fsblock = (agno << sb.agblklog) | agbno as u64;
            self.read_fsblock(fsblock, &mut block)?;
            let magic = BigEndian::read_u32(&block[0..4]);
            if magic != XFS_IBT_MAGIC {
                return Err(FilesystemError::Parse(format!(
                    "bad inobt magic 0x{magic:08X} at AG {agno} block {agbno}"
                )));
            }
            let level = BigEndian::read_u16(&block[4..6]);
            let numrecs = BigEndian::read_u16(&block[6..8]) as usize;
            if level == 0 {
                leaves.push(agbno);
            } else {
                if numrecs > max_intern {
                    return Err(FilesystemError::Parse(format!(
                        "AG {agno} inobt node numrecs {numrecs} > max {max_intern}"
                    )));
                }
                let ptr_base = INOBT_HDR_LEN + max_intern * 4;
                for i in 0..numrecs {
                    let off = ptr_base + i * 4;
                    stack.push(BigEndian::read_u32(&block[off..off + 4]));
                }
            }
        }
        Ok(leaves)
    }

    /// Recompute every record's free mask/freecount in one inobt leaf block
    /// from its inodes, rewriting the block only if something changed.
    fn repair_inobt_leaf(
        &mut self,
        sb: &XfsSuperblock,
        agno: u64,
        leaf_agbno: u32,
        internal: &HashSet<u64>,
    ) -> Result<LeafResult, FilesystemError> {
        let bs = sb.blocksize as usize;
        let max_leaf = (bs - INOBT_HDR_LEN) / INOBT_REC_SIZE;
        let fsblock = (agno << sb.agblklog) | leaf_agbno as u64;
        let mut block = vec![0u8; bs];
        self.read_fsblock(fsblock, &mut block)?;

        let numrecs = BigEndian::read_u16(&block[6..8]) as usize;
        if numrecs > max_leaf {
            return Err(FilesystemError::Parse(format!(
                "inobt leaf numrecs {numrecs} > max {max_leaf}"
            )));
        }

        let mut count: u64 = 0;
        let mut freecount: u64 = 0;
        let mut changed = false;

        for r in 0..numrecs {
            let off = INOBT_HDR_LEN + r * INOBT_REC_SIZE;
            let start_agino = BigEndian::read_u32(&block[off..off + 4]);
            let old_freecount = BigEndian::read_u32(&block[off + REC_FREECOUNT_OFF..off + 8]);
            let old_free = BigEndian::read_u64(&block[off + REC_FREE_OFF..off + 16]);

            let new_free = self.compute_chunk_free_mask(sb, agno, start_agino, internal)?;
            let new_freecount = new_free.count_ones();

            count += XFS_INODES_PER_CHUNK as u64;
            freecount += new_freecount as u64;

            if new_free != old_free || new_freecount != old_freecount {
                BigEndian::write_u32(&mut block[off + REC_FREECOUNT_OFF..off + 8], new_freecount);
                BigEndian::write_u64(&mut block[off + REC_FREE_OFF..off + 16], new_free);
                changed = true;
            }
        }

        if changed {
            let part_byte =
                fsblock_to_partition_byte(fsblock, sb.agblocks, sb.agblklog, sb.blocksize);
            self.reader
                .seek(SeekFrom::Start(self.partition_offset + part_byte))?;
            self.reader.write_all(&block)?;
        }

        Ok(LeafResult {
            count,
            freecount,
            rewritten: changed,
        })
    }

    /// Build a chunk's 64-bit free mask (set bit = free) by reading every
    /// inode's `di_mode`: mode 0 ⇒ free, with the superblock's internal inodes
    /// (root / rt bitmap / rt summary / quota) forced allocated regardless.
    fn compute_chunk_free_mask(
        &mut self,
        sb: &XfsSuperblock,
        agno: u64,
        start_agino: u32,
        internal: &HashSet<u64>,
    ) -> Result<u64, FilesystemError> {
        let inodesize = sb.inodesize as usize;
        let chunk_agbno = (start_agino as u64) >> sb.inopblog;
        let blocks_per_chunk =
            ((XFS_INODES_PER_CHUNK as u64) * inodesize as u64).div_ceil(sb.blocksize as u64);
        let span = blocks_per_chunk * sb.blocksize as u64;
        let first_fsblock = (agno << sb.agblklog) | chunk_agbno;
        let part_byte =
            fsblock_to_partition_byte(first_fsblock, sb.agblocks, sb.agblklog, sb.blocksize);

        let mut buf = vec![0u8; span as usize];
        read_at_aligned(
            &mut self.reader,
            self.partition_offset + part_byte,
            span,
            &mut buf,
        )?;

        let ino_shift = sb.agblklog + sb.inopblog;
        let mut mask: u64 = 0;
        for slot in 0..XFS_INODES_PER_CHUNK {
            let mode_off = slot * inodesize + DI_MODE_OFF;
            let mode = BigEndian::read_u16(&buf[mode_off..mode_off + 2]);
            let ino = (agno << ino_shift) | (start_agino as u64 + slot as u64);
            let is_free = mode == 0 && !internal.contains(&ino);
            if is_free {
                mask |= 1u64 << slot;
            }
        }
        Ok(mask)
    }
}

/// Per-leaf tally returned by `repair_inobt_leaf`.
struct LeafResult {
    count: u64,
    freecount: u64,
    rewritten: bool,
}
