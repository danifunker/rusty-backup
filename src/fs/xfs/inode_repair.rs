//! R5 — inode core repair (`docs/xfs_edit_and_repair.md` §5).
//!
//! `xfs_repair` phase 4 (`repair/dinode.c` `process_dinode`) validates and
//! repairs the inode core against its forks. This first slice handles the two
//! cached counters that are *soundly* derivable from the data fork and that
//! `xfs_repair -n` flags when stale — `di_nblocks` and `di_nextents` — taking
//! care to recompute each only where the fork actually determines it:
//!
//!   * **`di_nextents`** — recomputed only for **Local** forks (must be 0) and
//!     **Btree** forks (the btree walk counts records independently of the
//!     stored value). An **inline-extents** fork has no terminator — the only
//!     thing that says how many records exist *is* `di_nextents` — so the count
//!     cannot be recovered from the fork bytes, and we leave it alone (a
//!     too-large value is an unrecoverable fork corruption `xfs_repair` clears).
//!   * **`di_nblocks`** — recomputed for **Local** forks (0) and **inline
//!     extents** (the block-count sum of the extent list, which is correct when
//!     `di_nextents` is intact — the common "stale nblocks" corruption). A
//!     **Btree** data fork additionally owns its intermediate/leaf btree
//!     blocks, which we don't count here, so we skip `di_nblocks` rather than
//!     undercount. Skipped, too, when a non-local attr fork owns blocks.
//!
//! Both are pure recomputes from the on-disk extent list, so — like R3's
//! free-mask repair and R4b's AGF/AGI counters — a clean inode is a byte-for-
//! byte no-op and the fix is provable by re-running the verifier (and
//! externally by `xfs_repair -n`).
//!
//! Deliberately conservative: if any data extent points outside its AG (the
//! verifier's `ExtentPastVolume`), the fork itself is damaged and needs
//! *truncation*, not a counter that ratifies a bad list — so we skip the inode
//! and leave it for a later slice.
//!
//! v4 only: a v5 inode core carries a CRC we don't recompute, so rewriting one
//! would invalidate it (silent skip, not a failure). Porting reference:
//! `xfs_repair` @ v3.1.11 `repair/dinode.c`.

use std::io::{Read, Seek, SeekFrom, Write};

use byteorder::{BigEndian, ByteOrder};

use super::ag::XfsAgi;
use super::inode::inode_byte_offset;
use super::types::DiFormat;
use super::{read_at_aligned, XfsFilesystem, SECTOR};
use crate::fs::filesystem::FilesystemError;
use crate::fs::fsck::RepairReport;

/// inobt leaf record layout (v4 short header): startino(4) freecount(4) free(8).
const INOBT_REC_SIZE: usize = 16;
const INOBT_HDR_LEN: usize = 16; // XFS_BTREE_SBLOCK_LEN

/// di_nblocks (u64) and di_nextents (u32) offsets within an on-disk inode.
const DI_NBLOCKS_OFF: usize = 64;
const DI_NEXTENTS_OFF: usize = 76;

impl<R: Read + Write + Seek + Send> XfsFilesystem<R> {
    /// R5 entry point: walk every allocated inode and rewrite `di_nblocks` /
    /// `di_nextents` where they disagree with the data-fork extent list. A
    /// no-op on a healthy volume.
    pub(crate) fn run_inode_core_repair(&mut self) -> Result<RepairReport, FilesystemError> {
        let mut report = RepairReport {
            fixes_applied: Vec::new(),
            fixes_failed: Vec::new(),
            unrepairable_count: 0,
        };
        let sb = self.superblock().clone();
        if sb.is_v5() {
            // v5 inode cores are CRC-protected; rewriting would invalidate
            // them. R5 is v4-only (silent, not a failure).
            return Ok(report);
        }

        let sectsize = sb.sectsize as u64;
        let agblocks = sb.agblocks as u64;
        let blocksize = sb.blocksize as u64;
        let bs = sb.blocksize as usize;
        let ino_shift = sb.agblklog + sb.inopblog;

        let mut any_change = false;

        for agno in 0..sb.agcount as u64 {
            let ag_byte = self.partition_offset + agno * agblocks * blocksize;
            let agi_byte = ag_byte + 2 * sectsize;
            let mut agi_sec = vec![0u8; sectsize as usize];
            if read_at_aligned(&mut self.reader, agi_byte, sectsize, &mut agi_sec).is_err() {
                continue;
            }
            let agi = match XfsAgi::parse(&agi_sec) {
                Ok(a) => a,
                Err(_) => continue,
            };

            // R5 runs after R3, so the inobt should be walkable; if it still
            // isn't, R3 already reported it — skip this AG quietly.
            let leaves = match self.collect_inobt_leaf_blocks(&sb, agno, agi.root) {
                Ok(l) => l,
                Err(_) => continue,
            };

            let mut fixed: u64 = 0;
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
                    for slot in 0..super::types::XFS_INODES_PER_CHUNK as u64 {
                        if (free >> slot) & 1 == 1 {
                            continue; // free inode
                        }
                        let ino = (agno << ino_shift) | (start_agino + slot);
                        match self.repair_inode_core(&sb, ino) {
                            Ok(true) => fixed += 1,
                            Ok(false) => {}
                            Err(e) => {
                                report.fixes_failed.push(format!("inode {ino}: {e}"));
                            }
                        }
                    }
                }
            }
            if fixed > 0 {
                any_change = true;
                report.fixes_applied.push(format!(
                    "AG {agno}: recomputed di_nblocks/di_nextents on {fixed} inode(s)"
                ));
            }
        }

        if any_change {
            self.reader.flush()?;
        }
        Ok(report)
    }

    /// Recompute one inode's `di_nblocks` / `di_nextents` from its data fork,
    /// rewriting the inode only if a counter changed. Returns whether a write
    /// happened. Skips (Ok(false)) any inode we can't safely account for
    /// (zeroed, btree data fork for nblocks, non-local attr fork, or a data
    /// fork with an out-of-bounds extent).
    fn repair_inode_core(
        &mut self,
        sb: &super::sb::XfsSuperblock,
        ino: u64,
    ) -> Result<bool, FilesystemError> {
        let (core, mut inode_buf) = self.read_inode_buf(ino)?;
        if core.mode == 0 {
            // Allocated-but-zeroed: a structural inconsistency the verifier
            // flags (AllocatedInodeZeroed); not R5's job to paper over.
            return Ok(false);
        }

        // `nextents_new` is the soundly-recomputable data-fork extent count, or
        // None when it can't be derived independently of the stored value.
        // `data_blocks` is the data-fork block total when fully countable.
        //
        // Inline extents have no terminator — the only thing that says how many
        // records there are *is* `di_nextents` — so the count can't be recovered
        // from the fork bytes (a too-large value just reads trailing garbage).
        // We therefore recompute `di_nextents` only for Local (must be 0) and
        // Btree (the walk counts records independently of the stored value), and
        // recompute `di_nblocks` from the inline-extent list (which is correct
        // when `di_nextents` is intact — the common "stale nblocks" corruption).
        let (nextents_new, data_blocks): (Option<u32>, Option<u64>) = match core.format {
            DiFormat::Local => (Some(0), Some(0)),
            DiFormat::Extents => {
                let extents = match self.decode_data_extents(&core, &inode_buf) {
                    Ok(e) => e,
                    // Undecodable fork — verifier reports ExtentDecodeFailed;
                    // leave it alone here.
                    Err(_) => return Ok(false),
                };
                if !self.extents_in_bounds(sb, &extents) {
                    // Out-of-bounds extent: the fork needs truncation, not a
                    // counter that ratifies a bad list. Deferred slice.
                    return Ok(false);
                }
                let blocks: u64 = extents.iter().map(|e| e.blockcount).sum();
                (None, Some(blocks))
            }
            DiFormat::Btree => {
                let extents = match self.decode_data_extents(&core, &inode_buf) {
                    Ok(e) => e,
                    Err(_) => return Ok(false),
                };
                if !self.extents_in_bounds(sb, &extents) {
                    return Ok(false);
                }
                // Btree data fork also owns its bmbt blocks; we can't count
                // those here, so recompute nextents only and leave nblocks.
                (Some(extents.len() as u32), None)
            }
            DiFormat::Other(_) => return Ok(false),
        };

        // di_nblocks is fully accountable only when the attr fork owns no
        // blocks (absent or local) and we have a data-fork block total.
        let attr_owns_blocks =
            core.forkoff != 0 && !matches!(DiFormat::from_raw(core.aformat), DiFormat::Local);
        let nblocks_new = match data_blocks {
            Some(b) if !attr_owns_blocks => Some(b),
            _ => None,
        };

        let mut changed = false;
        if let Some(ne) = nextents_new {
            if core.nextents != ne {
                BigEndian::write_u32(&mut inode_buf[DI_NEXTENTS_OFF..DI_NEXTENTS_OFF + 4], ne);
                changed = true;
            }
        }
        if let Some(nb) = nblocks_new {
            if core.nblocks != nb {
                BigEndian::write_u64(&mut inode_buf[DI_NBLOCKS_OFF..DI_NBLOCKS_OFF + 8], nb);
                changed = true;
            }
        }

        if changed {
            self.write_inode_region(sb, ino, &inode_buf)?;
        }
        Ok(changed)
    }

    /// True if every extent lies within its allocation group (the same bound
    /// the verifier's `claim_extent` enforces before flagging `ExtentPastVolume`).
    fn extents_in_bounds(
        &self,
        sb: &super::sb::XfsSuperblock,
        extents: &[super::bmap::XfsBmbtIrec],
    ) -> bool {
        let agblocks = sb.agblocks as u64;
        let agmask = (1u64 << sb.agblklog) - 1;
        for e in extents {
            if e.blockcount == 0 {
                continue;
            }
            let last = e.startblock + e.blockcount - 1;
            for fsb in [e.startblock, last] {
                let agno = fsb >> sb.agblklog;
                let agbno = fsb & agmask;
                if agno >= sb.agcount as u64 || agbno >= agblocks {
                    return false;
                }
            }
        }
        true
    }

    /// Read-modify-write one inode's `inodesize` bytes back to disk. Mirrors
    /// `read_inode_buf`'s offset math; reads the containing sector span, patches
    /// the inode region, and writes the span (inodes are smaller than a sector).
    fn write_inode_region(
        &mut self,
        sb: &super::sb::XfsSuperblock,
        ino: u64,
        inode_buf: &[u8],
    ) -> Result<(), FilesystemError> {
        let part_off = inode_byte_offset(
            ino,
            sb.agblocks,
            sb.agblklog,
            sb.inopblog,
            sb.blocksize,
            sb.inodesize,
        );
        let isz = sb.inodesize as u64;
        let sector_start = (part_off / SECTOR) * SECTOR;
        let span_end = part_off + isz;
        let span_len = (span_end - sector_start).div_ceil(SECTOR) * SECTOR;
        let mut buf = vec![0u8; span_len as usize];
        read_at_aligned(
            &mut self.reader,
            self.partition_offset + sector_start,
            span_len,
            &mut buf,
        )?;
        let off_in = (part_off - sector_start) as usize;
        buf[off_in..off_in + isz as usize].copy_from_slice(&inode_buf[..isz as usize]);
        self.reader
            .seek(SeekFrom::Start(self.partition_offset + sector_start))?;
        self.reader.write_all(&buf)?;
        Ok(())
    }
}
