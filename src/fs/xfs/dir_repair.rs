//! R6 — directory / connectivity repair (`docs/xfs_edit_and_repair.md` §5).
//!
//! `xfs_repair` phase 6 walks the directory tree and drops entries that point
//! at inodes which aren't allocated ("dangling" entries — the verifier's
//! `DanglingEntry`), then fixes the affected directories' link counts. This
//! first slice handles the tractable, by-far-most-common vintage case:
//! **short-form (inline) directories**.
//!
//! For every allocated short-form directory we read each entry's child inode;
//! an entry whose child is free (`di_mode == 0`) or unreadable is dropped. The
//! inline fork is rebuilt from the surviving entries (preserving each one's
//! on-disk offset cookie, exactly as `libxfs` `xfs_dir2_sf_removename` does),
//! `di_size` is set to the new fork length, and the directory's link count is
//! **recomputed** as `2 + (number of surviving subdirectory entries)` — the
//! standard Unix directory nlink. The surviving children are allocated, so
//! their type is read straight from disk; this needs no `ftype` feature and is
//! correct even when a dropped entry was itself a subdirectory.
//!
//! Link count lives in different fields by inode version: **v1** inodes
//! (di_version == 1, the norm on real IRIX disks) keep it in `di_onlink`
//! (offset 6, u16); **v2** inodes keep it in `di_nlink` (offset 16, u32). We
//! write whichever the inode uses.
//!
//! Block / leaf / node (multi-block) directory repair and orphan reconnection
//! (R7) are deferred — those need the dir2 data/leaf/free + hash-btree write
//! primitives. v4 only (a v5 inode core carries a CRC we don't recompute).
//! Porting reference: `xfs_repair` @ v3.1.11 `repair/phase6.c`.

use std::io::{Read, Seek, Write};

use byteorder::{BigEndian, ByteOrder};

use super::ag::XfsAgi;
use super::types::{DiFormat, XFS_INODES_PER_CHUNK};
use super::{read_at_aligned, XfsFilesystem};
use crate::fs::filesystem::{Filesystem, FilesystemError};
use crate::fs::fsck::RepairReport;

/// inobt leaf record layout (v4 short header): startino(4) freecount(4) free(8).
const INOBT_REC_SIZE: usize = 16;
const INOBT_HDR_LEN: usize = 16;

/// Inode-core field offsets.
const DI_SIZE_OFF: usize = 56; // di_size (u64)
const DI_ONLINK_OFF: usize = 6; // v1 nlink (u16)
const DI_NLINK_OFF: usize = 16; // v2 nlink (u32)

impl<R: Read + Write + Seek + Send> XfsFilesystem<R> {
    /// R6 entry point: drop dangling entries from every short-form directory and
    /// fix the directory's `di_size` / link count. No-op on a healthy volume.
    pub(crate) fn run_dir_repair(&mut self) -> Result<RepairReport, FilesystemError> {
        let mut report = RepairReport {
            fixes_applied: Vec::new(),
            fixes_failed: Vec::new(),
            unrepairable_count: 0,
        };
        let sb = self.superblock().clone();
        if sb.is_v5() {
            // v5 inode cores are CRC-protected; rewriting would invalidate
            // them. R6 is v4-only (silent, not a failure).
            return Ok(report);
        }

        let sectsize = sb.sectsize as u64;
        let agblocks = sb.agblocks as u64;
        let blocksize = sb.blocksize as u64;
        let bs = sb.blocksize as usize;
        let ino_shift = sb.agblklog + sb.inopblog;
        let mut any_change = false;

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
            let leaves = match self.collect_inobt_leaf_blocks(&sb, agno, agi.root) {
                Ok(l) => l,
                Err(_) => continue, // R3 already reported an unwalkable inobt
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
                        if (free >> slot) & 1 == 1 {
                            continue; // free inode
                        }
                        let ino = (agno << ino_shift) | (start_agino + slot);
                        match self.repair_shortform_dir(&sb, ino) {
                            Ok(Some(dropped)) => {
                                any_change = true;
                                report.fixes_applied.push(format!(
                                    "inode {ino}: dropped {dropped} dangling shortform dir entry(ies), fixed nlink"
                                ));
                            }
                            Ok(None) => {}
                            Err(e) => report.fixes_failed.push(format!("inode {ino}: {e}")),
                        }
                    }
                }
            }
        }

        if any_change {
            self.reader.flush()?;
        }
        Ok(report)
    }

    /// R7 (link-count half): recompute every *reachable* inode's link count
    /// from the directory graph and rewrite the ones that disagree
    /// (`xfs_repair` phase 7). A file's count is the number of directory
    /// entries pointing at it; a directory's is `2 + immediate subdirectories`.
    ///
    /// Runs after R6 so the graph is already free of dangling entries. Only
    /// *reached* inodes are corrected — an orphan (allocated but unreachable)
    /// keeps its on-disk nlink, because zeroing it would look like a free inode;
    /// reconnecting orphans into `lost+found` is the deferred other half of R7
    /// (needs inode-allocation + directory-insertion primitives). If any
    /// directory in the walk is unreadable the file tallies would undercount,
    /// so we abort and write nothing. v4 only (v5 inode CRC).
    pub(crate) fn run_nlink_repair(&mut self) -> Result<RepairReport, FilesystemError> {
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
            Err(_) => return Ok(report), // verifier already flags an unreadable root
        };

        // BFS the directory tree, tallying counted link counts.
        use std::collections::{HashMap, HashSet};
        let mut file_links: HashMap<u64, u32> = HashMap::new();
        let mut dir_nlink: HashMap<u64, u32> = HashMap::new();
        let mut reached: HashSet<u64> = HashSet::new();
        let mut visited: HashSet<u64> = HashSet::new();
        let mut queue = vec![root.clone()];
        reached.insert(root.location);
        while let Some(dir) = queue.pop() {
            if !visited.insert(dir.location) {
                continue;
            }
            let children = match self.list_directory(&dir) {
                Ok(c) => c,
                // Incomplete graph -> file link tallies would undercount; abort
                // without writing anything.
                Err(_) => return Ok(report),
            };
            let mut nsub = 0u32;
            for child in children {
                reached.insert(child.location);
                if child.is_directory() {
                    nsub += 1;
                    queue.push(child);
                } else {
                    *file_links.entry(child.location).or_default() += 1;
                }
            }
            dir_nlink.insert(dir.location, 2 + nsub);
        }

        let mut fixed = 0u64;
        for ino in reached {
            let counted = match (dir_nlink.get(&ino), file_links.get(&ino)) {
                (Some(&n), _) => n,
                (None, Some(&n)) => n,
                // Reached but tallied nowhere (only the root before its own
                // visit, already inserted in dir_nlink) — shouldn't happen.
                (None, None) => continue,
            };
            match self.fix_inode_nlink(&sb, ino, counted) {
                Ok(true) => fixed += 1,
                Ok(false) => {}
                Err(e) => report.fixes_failed.push(format!("inode {ino}: {e}")),
            }
        }
        if fixed > 0 {
            self.reader.flush()?;
            report
                .fixes_applied
                .push(format!("recomputed link counts on {fixed} inode(s)"));
        }
        Ok(report)
    }

    /// Read inode `ino`, and if its stored link count differs from `counted`,
    /// rewrite the version-correct nlink field. Returns whether it changed.
    fn fix_inode_nlink(
        &mut self,
        sb: &super::sb::XfsSuperblock,
        ino: u64,
        counted: u32,
    ) -> Result<bool, FilesystemError> {
        let (core, mut buf) = self.read_inode_buf(ino)?;
        if core.mode == 0 {
            return Ok(false);
        }
        let stored = if core.version == 1 {
            BigEndian::read_u16(&buf[DI_ONLINK_OFF..DI_ONLINK_OFF + 2]) as u32
        } else {
            BigEndian::read_u32(&buf[DI_NLINK_OFF..DI_NLINK_OFF + 4])
        };
        if stored == counted {
            return Ok(false);
        }
        if core.version == 1 {
            BigEndian::write_u16(&mut buf[DI_ONLINK_OFF..DI_ONLINK_OFF + 2], counted as u16);
        } else {
            BigEndian::write_u32(&mut buf[DI_NLINK_OFF..DI_NLINK_OFF + 4], counted);
        }
        self.write_inode_region(sb, ino, &buf)?;
        Ok(true)
    }

    /// Examine one inode; if it's an allocated short-form directory with entries
    /// pointing at free inodes, rebuild its inline fork without them and fix
    /// `di_size` + link count. Returns the number of entries dropped (`None`
    /// when nothing was wrong / the inode isn't a short-form directory).
    fn repair_shortform_dir(
        &mut self,
        sb: &super::sb::XfsSuperblock,
        ino: u64,
    ) -> Result<Option<usize>, FilesystemError> {
        let (core, inode_buf) = self.read_inode_buf(ino)?;
        if core.mode == 0 || !core.is_dir() || core.format != DiFormat::Local {
            return Ok(None);
        }

        let fork = self.data_fork(&core, &inode_buf);
        let has_ftype = sb.has_ftype();
        let entries = match parse_shortform_spans(fork, has_ftype) {
            Ok(e) => e,
            // A short-form fork we can't parse is structural damage beyond this
            // slice; leave it for a future block/leaf-aware pass.
            Err(_) => return Ok(None),
        };

        // Classify each entry: surviving (child allocated) keeps its raw bytes
        // and contributes to the recomputed nlink if it's a subdirectory; a
        // dangling entry (child free / unreadable) is dropped.
        let mut survivors: Vec<&[u8]> = Vec::with_capacity(entries.recs.len());
        let mut surviving_subdirs: u32 = 0;
        let mut dropped = 0usize;
        for e in &entries.recs {
            let child_alive = match self.read_inode_buf(e.inumber) {
                Ok((c, _)) => c.mode != 0,
                Err(_) => false,
            };
            if !child_alive {
                dropped += 1;
                continue;
            }
            // Allocated child: read its real type (no ftype dependency).
            if let Ok((c, _)) = self.read_inode_buf(e.inumber) {
                if c.is_dir() {
                    surviving_subdirs += 1;
                }
            }
            survivors.push(&fork[e.start..e.end]);
        }
        if dropped == 0 {
            return Ok(None);
        }

        // Rebuild the inline fork: header (count', i8count, parent) + surviving
        // entry bytes verbatim (offsets preserved, as libxfs removename does).
        let mut new_fork = Vec::with_capacity(fork.len());
        new_fork.push(survivors.len() as u8); // count
        new_fork.push(entries.i8count); // i8count (parent width unchanged)
        new_fork.extend_from_slice(&fork[2..2 + entries.parent_width]);
        for s in &survivors {
            new_fork.extend_from_slice(s);
        }

        // Splice the new fork into the inode buffer, zeroing the old tail, then
        // fix di_size and the (version-correct) link count.
        let fork_start = super::inode::fork_offset(false); // v4
        let old_end = fork_start + core.size as usize;
        let mut buf = inode_buf;
        if old_end > buf.len() || fork_start + new_fork.len() > buf.len() {
            return Err(FilesystemError::Parse(format!(
                "inode {ino}: shortform fork overflows inode"
            )));
        }
        buf[fork_start..fork_start + new_fork.len()].copy_from_slice(&new_fork);
        for byte in buf
            .iter_mut()
            .take(old_end)
            .skip(fork_start + new_fork.len())
        {
            *byte = 0;
        }
        BigEndian::write_u64(
            &mut buf[DI_SIZE_OFF..DI_SIZE_OFF + 8],
            new_fork.len() as u64,
        );
        let new_nlink = 2 + surviving_subdirs;
        if core.version == 1 {
            BigEndian::write_u16(&mut buf[DI_ONLINK_OFF..DI_ONLINK_OFF + 2], new_nlink as u16);
        } else {
            BigEndian::write_u32(&mut buf[DI_NLINK_OFF..DI_NLINK_OFF + 4], new_nlink);
        }

        self.write_inode_region(sb, ino, &buf)?;
        Ok(Some(dropped))
    }
}

/// One short-form entry's byte span within the fork plus its child inumber.
struct SfEntry {
    start: usize,
    end: usize,
    inumber: u64,
}

/// Parsed short-form fork: parent inode width, i8count, and per-entry spans.
struct SfSpans {
    parent_width: usize,
    i8count: u8,
    recs: Vec<SfEntry>,
}

/// Walk a short-form dir fork capturing each entry's raw `[start,end)` byte
/// span and child inumber (mirrors `dir2::parse_shortform`, but byte-oriented
/// so the survivors can be re-emitted verbatim).
fn parse_shortform_spans(fork: &[u8], has_ftype: bool) -> Result<SfSpans, FilesystemError> {
    if fork.len() < 6 {
        return Err(FilesystemError::Parse("shortform fork too small".into()));
    }
    let count = fork[0] as usize;
    let i8count = fork[1];
    let ino_width = if i8count > 0 { 8 } else { 4 };
    let hdr_size = 2 + ino_width;
    if fork.len() < hdr_size {
        return Err(FilesystemError::Parse("shortform header overflow".into()));
    }
    let mut recs = Vec::with_capacity(count);
    let mut pos = hdr_size;
    for _ in 0..count {
        let start = pos;
        if pos + 3 > fork.len() {
            return Err(FilesystemError::Parse("shortform entry truncated".into()));
        }
        let namelen = fork[pos] as usize;
        let ino_off = pos + 3 + namelen + usize::from(has_ftype);
        let next = ino_off + ino_width;
        if next > fork.len() {
            return Err(FilesystemError::Parse("shortform entry overflow".into()));
        }
        let inumber = if ino_width == 8 {
            BigEndian::read_u64(&fork[ino_off..ino_off + 8])
        } else {
            BigEndian::read_u32(&fork[ino_off..ino_off + 4]) as u64
        };
        recs.push(SfEntry {
            start,
            end: next,
            inumber,
        });
        pos = next;
    }
    Ok(SfSpans {
        parent_width: ino_width,
        i8count,
        recs,
    })
}
