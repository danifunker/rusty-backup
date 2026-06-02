//! R2 — live free-space btree rebuild (`docs/xfs_edit_and_repair.md` §5).
//!
//! This is the first *structural* btree write. It rebuilds an allocation
//! group's `bnobt` (free-space-by-block) and `cntbt` (free-space-by-count)
//! btrees from the authoritative inode-ownership map, then rewrites the AGF
//! roots/levels/freeblks/longest/btreeblks and the superblock free-block
//! counter. The pure block-packing core lives in [`super::btree_build`]; this
//! module supplies the *policy*: which blocks are free, which blocks the new
//! trees live in, and when it is safe to write at all.
//!
//! Porting reference: `xfs_repair` @ v3.1.11 `repair/phase5.c`
//! (`mk_incore_fstree` / `build_freespace_tree` / `build_agf_agfl`).
//!
//! ## How it differs from offline `xfs_repair`
//!
//! `xfs_repair` rebuilds *every* AG btree (inobt + bno + cnt) and the AGFL
//! from scratch every run, distrusting all of them. We are conservative: we
//! keep the inode-allocation btree and the AGFL exactly where they are
//! (marking their blocks in use) and rebuild *only* the two free-space btrees.
//! New btree blocks are carved off the end of the AG's largest free extent, so
//! the free-extent *count* never changes mid-build and the block math is a
//! one-shot calculation rather than `xfs_repair`'s iterative cursor fixed
//! point. The result is internally consistent (bnobt sum == agf_freeblks ==
//! Σ agf_freeblks == sb_fdblocks) — which is exactly what `xfs_repair -n`
//! checks — even though the absolute block layout differs from a fresh repair.
//!
//! ## Safety gating
//!
//! The rebuild only runs when the inode-ownership map can be trusted
//! completely: v4 only, no btree-format (`di_format == 3`) inodes (whose bmap
//! btree blocks we can't yet enumerate to mark in use), no double-allocated
//! blocks, no out-of-bounds extents, and every inode/inobt read must succeed.
//! If any of those fail we record a note and write *nothing* — R4/R4b still
//! run. An AG is rewritten only when its derived free space disagrees with the
//! on-disk bnobt (or the bnobt is unreadable), so a clean volume is a no-op.

use std::io::{Read, Seek, SeekFrom, Write};

use byteorder::{BigEndian, ByteOrder};

use super::ag::{XfsAgf, XfsAgi};
use super::btree_build::{blocks_needed, build_alloc_btree, FreeExtent};
use super::sb::XfsSuperblock;
use super::types::{
    DiFormat, XFS_ABTB_MAGIC, XFS_ABTC_MAGIC, XFS_AGF_MAGIC, XFS_IBT_CRC_MAGIC, XFS_IBT_MAGIC,
    XFS_INODES_PER_CHUNK,
};
use super::{read_at_aligned, XfsFilesystem};
use crate::fs::filesystem::FilesystemError;
use crate::fs::fsck::RepairReport;

/// AGF on-disk field offsets (v4 short header) within the AGF sector.
const AGF_ROOTS_OFF: usize = 16; // roots[BNO]=16, roots[CNT]=20
const AGF_LEVELS_OFF: usize = 28; // levels[BNO]=28, levels[CNT]=32
const AGF_FREEBLKS_OFF: usize = 52;
const AGF_LONGEST_OFF: usize = 56;
const AGF_BTREEBLKS_OFF: usize = 60;

/// Superblock free-data-block counter offset (`sb_fdblocks`).
const SB_FDBLOCKS_OFF: usize = 144;

/// Per-AG header info gathered up front, reused by the rebuild loop.
struct AgInfo {
    expected_len: u64,
    /// AG-relative inode-btree root (`None` → AGI unreadable, can't enumerate).
    agi_root: Option<u32>,
    /// AG-relative bnobt / cntbt roots from the AGF, for the "needs rebuild?"
    /// compare (both trees' own blocks are added back as free before matching).
    agf_bno_root: Option<u32>,
    agf_cnt_root: Option<u32>,
    /// AGFL window `(flfirst, flcount)` — its blocks stay reserved (in use).
    agf_fl: Option<(u32, u32)>,
}

/// Whole-volume block-in-use map, indexed by partition-linear block number
/// (`agno * agblocks + agbno`). `true` = owned by metadata / inode data and
/// therefore *not* free. Old bnobt/cntbt blocks are deliberately left `false`
/// so the rebuild reclaims them. Shared with R3's multi-level inobt rebuild,
/// which builds an equivalent map (scan-based) to find blocks for the new tree.
pub(crate) struct InUseMap {
    used: Vec<bool>,
}

impl InUseMap {
    pub(crate) fn new(dblocks: u64) -> Self {
        Self {
            used: vec![false; dblocks as usize],
        }
    }
    /// Mark `linear` in use. Returns `true` if it was *already* in use — a
    /// cross-link / double allocation that makes the map untrustworthy.
    pub(crate) fn mark(&mut self, linear: u64) -> bool {
        match self.used.get_mut(linear as usize) {
            Some(slot) => {
                let was = *slot;
                *slot = true;
                was
            }
            None => false,
        }
    }
    pub(crate) fn is_used(&self, linear: u64) -> bool {
        self.used.get(linear as usize).copied().unwrap_or(true)
    }
}

impl<R: Read + Write + Seek + Send> XfsFilesystem<R> {
    /// R2 entry point: rebuild every AG's free-space btrees that disagree with
    /// the inode-derived free map. Safe (writes nothing) whenever the map
    /// can't be fully trusted; notes the reason in the report either way.
    pub(crate) fn run_freespace_rebuild(&mut self) -> Result<RepairReport, FilesystemError> {
        let mut report = RepairReport {
            fixes_applied: Vec::new(),
            fixes_failed: Vec::new(),
            unrepairable_count: 0,
        };
        let sb = self.superblock().clone();

        if sb.is_v5() {
            // v5 adds finobt/rmapbt/refcountbt metadata we don't model, so a
            // block-completeness map would be wrong. R2 simply doesn't apply to
            // v5 — return silently (not a failure) and leave it to R4/R4b.
            return Ok(report);
        }

        let ags = self.read_ag_info(&sb)?;

        // Build the trustworthy in-use map, or bail with a note.
        let map = match self.build_inuse_map(&sb, &ags) {
            Ok(map) => map,
            Err(reason) => {
                report.fixes_failed.push(format!(
                    "free-space btree rebuild skipped (no write): {reason}"
                ));
                return Ok(report);
            }
        };

        let agblocks = sb.agblocks as u64;
        let mut any_changed = false;

        for (agno, ag) in ags.iter().enumerate() {
            let agno = agno as u64;
            // Derive this AG's free extents (AG-relative) from the in-use map.
            let derived = derive_free_extents(&map, agno, agblocks, ag.expected_len);

            // Decide whether this AG actually needs a rewrite. The on-disk
            // bnobt deliberately omits the blocks the bno/cnt trees themselves
            // occupy, whereas `derived` reclaims them as free. So reconstruct
            // what a *correct* on-disk tree would describe — its free records
            // plus its own (and the cntbt's) blocks added back — and compare
            // that to `derived`. Equal means the AG is already right; skip it.
            // An unreadable tree means we must rebuild. All reads, no writes.
            if self.ag_freespace_matches(&sb, agno, ag, &derived) {
                continue;
            }

            match self.rebuild_ag_freespace(&sb, agno, ag, &derived) {
                Ok(freeblks) => {
                    any_changed = true;
                    report.fixes_applied.push(format!(
                        "AG {agno}: rebuilt free-space btrees ({} extents, {freeblks} free blocks)",
                        derived.len()
                    ));
                }
                Err(e) => report
                    .fixes_failed
                    .push(format!("AG {agno}: free-space rebuild failed: {e}")),
            }
        }

        if any_changed {
            if let Err(e) = self.resync_sb_fdblocks(&sb) {
                report
                    .fixes_failed
                    .push(format!("sb_fdblocks resync failed: {e}"));
            } else {
                report
                    .fixes_applied
                    .push("superblock: resynced sb_fdblocks from rebuilt AGFs".into());
            }
        }

        self.reader.flush()?;
        Ok(report)
    }

    /// Read each AG's header sector group, capturing the info the rebuild
    /// needs (inobt root, bnobt root, AGFL window, AG length).
    fn read_ag_info(&mut self, sb: &XfsSuperblock) -> Result<Vec<AgInfo>, FilesystemError> {
        let agcount = sb.agcount as u64;
        let agblocks = sb.agblocks as u64;
        let blocksize = sb.blocksize as u64;
        let sectsize = sb.sectsize as u64;
        let span = 4 * sectsize;
        let mut buf = vec![0u8; span as usize];
        let mut out = Vec::with_capacity(agcount as usize);

        for agno in 0..agcount {
            let expected_len = if agno == agcount - 1 {
                sb.dblocks - agno * agblocks
            } else {
                agblocks
            };
            let ag_byte = self.partition_offset + agno * agblocks * blocksize;
            read_at_aligned(&mut self.reader, ag_byte, span, &mut buf)?;

            let agf_off = sectsize as usize;
            let (agf_bno_root, agf_cnt_root, agf_fl) =
                match XfsAgf::parse(&buf[agf_off..agf_off + sectsize as usize]) {
                    Ok(agf) => (
                        Some(agf.bno_root),
                        Some(agf.cnt_root),
                        Some((agf.flfirst, agf.flcount)),
                    ),
                    Err(_) => (None, None, None),
                };
            let agi_off = 2 * sectsize as usize;
            let agi_root = XfsAgi::parse(&buf[agi_off..agi_off + sectsize as usize])
                .ok()
                .map(|agi| agi.root);

            out.push(AgInfo {
                expected_len,
                agi_root,
                agf_bno_root,
                agf_cnt_root,
                agf_fl,
            });
        }
        Ok(out)
    }

    /// Build the whole-volume in-use map from inode ownership, returning the
    /// map on success or a human-readable reason the rebuild must be skipped.
    fn build_inuse_map(&mut self, sb: &XfsSuperblock, ags: &[AgInfo]) -> Result<InUseMap, String> {
        let agblocks = sb.agblocks as u64;
        let mut map = InUseMap::new(sb.dblocks);
        let blocks_per_chunk =
            ((XFS_INODES_PER_CHUNK as u64) * sb.inodesize as u64).div_ceil(sb.blocksize as u64);

        // AG-header reservation: the sb/AGF/AGI/AGFL sectors at the front of
        // each AG (block 0 when blocksize >= 4*sectsize, which holds on every
        // v4 image we target). Round up defensively.
        let header_blocks = (4 * sb.sectsize as u64)
            .div_ceil(sb.blocksize as u64)
            .max(1);

        // Internal log: a contiguous immovable metadata run.
        if sb.logstart != 0 {
            let log_agno = sb.logstart >> sb.agblklog;
            let log_agbno = sb.logstart & ((1u64 << sb.agblklog) - 1);
            let log_linear = log_agno * agblocks + log_agbno;
            for i in 0..sb.logblocks as u64 {
                map.mark(log_linear + i);
            }
        }

        for (agno, ag) in ags.iter().enumerate() {
            let agno = agno as u64;
            for blk in 0..header_blocks {
                map.mark(agno * agblocks + blk);
            }
            // Keep the existing AGFL reserved.
            if let Some((flfirst, flcount)) = ag.agf_fl {
                self.mark_agfl(sb, agno, flfirst, flcount, &mut map)?;
            }

            let Some(root) = ag.agi_root else {
                return Err(format!("AG {agno} inode btree root unreadable"));
            };

            // Walk the inobt: mark its own blocks, collect the chunk records.
            let mut recs: Vec<(u32, u64)> = Vec::new(); // (start_agino, free_mask)
            let mut walk_err: Option<String> = None;
            let mut collision = false;
            self.walk_sblock_btree(
                sb,
                agno,
                root,
                ag.expected_len,
                [XFS_IBT_MAGIC, XFS_IBT_CRC_MAGIC],
                16,
                4,
                |rec| {
                    recs.push((
                        BigEndian::read_u32(&rec[0..4]),
                        BigEndian::read_u64(&rec[8..16]),
                    ));
                },
                |linear| {
                    if map.mark(linear) {
                        collision = true;
                    }
                },
            )
            .unwrap_or_else(|e| walk_err = Some(format!("AG {agno} inode btree: {e}")));
            if let Some(e) = walk_err {
                return Err(e);
            }
            if collision {
                return Err(format!("AG {agno}: cross-linked inode-btree block"));
            }

            for (start_agino, free_mask) in recs {
                let chunk_agbno = (start_agino as u64) >> sb.inopblog;
                for blk in 0..blocks_per_chunk {
                    if map.mark(agno * agblocks + chunk_agbno + blk) {
                        return Err(format!("AG {agno}: cross-linked inode-chunk block"));
                    }
                }
                for slot in 0..XFS_INODES_PER_CHUNK {
                    if (free_mask >> slot) & 1 == 1 {
                        continue; // free inode slot
                    }
                    let agino = start_agino as u64 + slot as u64;
                    let ino = (agno << (sb.agblklog + sb.inopblog)) | agino;
                    self.mark_inode_blocks(sb, ino, &mut map)?;
                }
            }
        }

        Ok(map)
    }

    /// Mark one allocated inode's data-fork blocks in use. Returns a skip
    /// reason if the inode is btree-format (unmappable bmap blocks), if a
    /// block is cross-linked, or on any read/decode failure.
    pub(crate) fn mark_inode_blocks(
        &mut self,
        sb: &XfsSuperblock,
        ino: u64,
        map: &mut InUseMap,
    ) -> Result<(), String> {
        let (core, inode_buf) = self
            .read_inode_buf(ino)
            .map_err(|e| format!("inode {ino}: {e}"))?;
        if core.mode == 0 {
            return Err(format!("inode {ino} allocated in inobt but zeroed"));
        }
        match core.format {
            DiFormat::Local | DiFormat::Other(_) => return Ok(()), // owns no blocks
            DiFormat::Btree => {
                return Err(format!(
                    "inode {ino} is bmap-btree format (di_format 3) — \
                     intermediate map blocks not yet accounted; skipping rebuild"
                ));
            }
            DiFormat::Extents => {}
        }
        let extents = self
            .decode_data_extents(&core, &inode_buf)
            .map_err(|e| format!("inode {ino}: {e}"))?;
        let agblocks = sb.agblocks as u64;
        let agmask = (1u64 << sb.agblklog) - 1;
        for ext in &extents {
            for i in 0..ext.blockcount {
                let fsb = ext.startblock + i;
                let agno = fsb >> sb.agblklog;
                let agbno = fsb & agmask;
                if agno >= sb.agcount as u64 || agbno >= agblocks {
                    return Err(format!("inode {ino}: extent block {fsb} outside volume"));
                }
                if map.mark(agno * agblocks + agbno) {
                    return Err(format!("inode {ino}: block {fsb} double-allocated"));
                }
            }
        }
        Ok(())
    }

    /// Mark the AGFL's live entries in use. AGFL blocks are pre-reserved
    /// free-list blocks we keep where they are. A collision here means the
    /// AGFL points at owned space — untrustworthy, so we skip.
    pub(crate) fn mark_agfl(
        &mut self,
        sb: &XfsSuperblock,
        agno: u64,
        flfirst: u32,
        flcount: u32,
        map: &mut InUseMap,
    ) -> Result<(), String> {
        if flcount == 0 {
            return Ok(());
        }
        let agblocks = sb.agblocks as u64;
        let sectsize = sb.sectsize as u64;
        let blocksize = sb.blocksize as u64;
        let agfl_byte = self.partition_offset + agno * agblocks * blocksize + 3 * sectsize;
        let mut sec = vec![0u8; sectsize as usize];
        read_at_aligned(&mut self.reader, agfl_byte, sectsize, &mut sec)
            .map_err(|e| format!("AG {agno}: AGFL read failed: {e}"))?;
        let arr_off = if sb.is_v5() { 36 } else { 0 };
        let nslots = ((sectsize as usize - arr_off) / 4) as u32;
        if nslots == 0 {
            return Ok(());
        }
        for i in 0..flcount {
            let idx = (flfirst + i) % nslots;
            let off = arr_off + (idx as usize) * 4;
            let agbno = BigEndian::read_u32(&sec[off..off + 4]);
            if (agbno as u64) < agblocks && map.mark(agno * agblocks + agbno as u64) {
                return Err(format!(
                    "AG {agno}: AGFL block {agbno} also owned by an inode"
                ));
            }
        }
        Ok(())
    }

    /// True if the AG's on-disk free-space btrees already describe exactly the
    /// inode-derived free space (`derived`) — in which case there is nothing to
    /// fix. Pure read.
    ///
    /// The on-disk bnobt records omit the blocks the bno + cnt trees occupy
    /// (those blocks are metadata, not free), but `derived` reclaims them as
    /// free. So we walk both trees, take the bnobt's free records, add back
    /// every block both trees occupy, coalesce, and compare to `derived`. Any
    /// unreadable/missing tree means "needs rebuild" → returns false.
    fn ag_freespace_matches(
        &mut self,
        sb: &XfsSuperblock,
        agno: u64,
        ag: &AgInfo,
        derived: &[FreeExtent],
    ) -> bool {
        let (Some(bno_root), Some(cnt_root)) = (ag.agf_bno_root, ag.agf_cnt_root) else {
            return false;
        };
        let bno = self.walk_alloc_tree(sb, agno, bno_root, ag.expected_len, XFS_ABTB_MAGIC);
        let cnt = self.walk_alloc_tree(sb, agno, cnt_root, ag.expected_len, XFS_ABTC_MAGIC);
        let (Ok((bno_recs, bno_blocks)), Ok((cnt_recs, cnt_blocks))) = (bno, cnt) else {
            return false;
        };
        let mut want = derived.to_vec();
        want.sort_by_key(|e| e.startblock);

        // Both trees describe the same free space; each must reconstruct to
        // `want` once its own blocks (and the sibling tree's) are added back as
        // free. Checking only one would miss a corrupt sibling (e.g. an intact
        // bnobt hiding a wrong cntbt).
        let own: Vec<FreeExtent> = bno_blocks
            .iter()
            .chain(&cnt_blocks)
            .map(|&agbno| FreeExtent {
                startblock: agbno,
                blockcount: 1,
            })
            .collect();
        let reconstruct = |recs: &[FreeExtent]| {
            let mut all = recs.to_vec();
            all.extend(own.iter().copied());
            coalesce(all)
        };
        reconstruct(&bno_recs) == want && reconstruct(&cnt_recs) == want
    }

    /// Walk an AG-relative alloc btree (`XFS_ABTB_MAGIC` / `XFS_ABTC_MAGIC`),
    /// returning its leaf records as `(start, len)` extents and the AG-relative
    /// block numbers of every btree block visited. Pure read.
    fn walk_alloc_tree(
        &mut self,
        sb: &XfsSuperblock,
        agno: u64,
        root: u32,
        expected_len: u64,
        magic: u32,
    ) -> Result<(Vec<FreeExtent>, Vec<u32>), FilesystemError> {
        let crc_magic = if magic == XFS_ABTB_MAGIC {
            super::types::XFS_ABTB_CRC_MAGIC
        } else {
            super::types::XFS_ABTC_CRC_MAGIC
        };
        let agblocks = sb.agblocks as u64;
        let mut recs = Vec::new();
        let mut blocks = Vec::new();
        self.walk_sblock_btree(
            sb,
            agno,
            root,
            expected_len,
            [magic, crc_magic],
            8,
            8,
            |rec| {
                recs.push(FreeExtent {
                    startblock: BigEndian::read_u32(&rec[0..4]),
                    blockcount: BigEndian::read_u32(&rec[4..8]),
                });
            },
            |linear| blocks.push((linear - agno * agblocks) as u32),
        )?;
        Ok((recs, blocks))
    }

    /// Rebuild one AG's bno + cnt btrees over `derived` free extents and
    /// rewrite the AGF. Returns the AG's new free-block total.
    fn rebuild_ag_freespace(
        &mut self,
        sb: &XfsSuperblock,
        agno: u64,
        ag: &AgInfo,
        derived: &[FreeExtent],
    ) -> Result<u64, FilesystemError> {
        let bs = sb.blocksize as usize;
        // Per-tree block count is the same for bno and cnt (same record set).
        let per_tree = blocks_needed(derived.len(), bs);
        let total_needed = 2 * per_tree;

        // Carve the btree blocks off the end of the largest free extent so the
        // extent *count* is preserved (one shortened extent, never split). This
        // keeps the block math a single calculation; see the module header.
        let (carved, reduced) =
            carve_from_largest(derived, total_needed as u32).ok_or_else(|| {
                FilesystemError::Unsupported(format!(
                    "AG {agno}: largest free extent too small to hold rebuilt btrees \
                 (need {total_needed} blocks)"
                ))
            })?;
        debug_assert_eq!(carved.len(), total_needed);
        // The shortened largest extent might change which tree is bigger? No —
        // count is unchanged, so blocks_needed is unchanged.
        debug_assert_eq!(blocks_needed(reduced.len(), bs), per_tree);

        let bno_blocks = &carved[0..per_tree];
        let cnt_blocks = &carved[per_tree..total_needed];

        // bno: ordered by startblock. cnt: ordered by (count, startblock).
        let mut bno_recs = reduced.clone();
        bno_recs.sort_by_key(|e| e.startblock);
        let mut cnt_recs = reduced.clone();
        cnt_recs.sort_by(|a, b| {
            a.blockcount
                .cmp(&b.blockcount)
                .then(a.startblock.cmp(&b.startblock))
        });

        let bno = build_alloc_btree(&bno_recs, XFS_ABTB_MAGIC, bs, agno as u32, bno_blocks);
        let cnt = build_alloc_btree(&cnt_recs, XFS_ABTC_MAGIC, bs, agno as u32, cnt_blocks);

        // Write every built block at its fsblock location.
        for blk in bno.blocks.iter().chain(cnt.blocks.iter()) {
            let fsblock = (agno << sb.agblklog) | blk.agbno as u64;
            self.write_fsblock(sb, fsblock, &blk.bytes)?;
        }

        // Recompute the AGF summary fields from the reduced free set.
        let freeblks: u64 = reduced.iter().map(|e| e.blockcount as u64).sum();
        let longest: u32 = reduced.iter().map(|e| e.blockcount).max().unwrap_or(0);
        let btreeblks = (bno.blocks.len() as u32 - 1) + (cnt.blocks.len() as u32 - 1);

        self.write_agf_fields(
            sb,
            agno,
            ag.expected_len,
            bno.root_agbno,
            bno.levels,
            cnt.root_agbno,
            cnt.levels,
            freeblks as u32,
            longest,
            btreeblks,
        )?;

        Ok(freeblks)
    }

    /// Patch the AGF sector's free-space-btree summary fields in place.
    #[allow(clippy::too_many_arguments)]
    fn write_agf_fields(
        &mut self,
        sb: &XfsSuperblock,
        agno: u64,
        _expected_len: u64,
        bno_root: u32,
        bno_levels: u32,
        cnt_root: u32,
        cnt_levels: u32,
        freeblks: u32,
        longest: u32,
        btreeblks: u32,
    ) -> Result<(), FilesystemError> {
        let sectsize = sb.sectsize as u64;
        let agblocks = sb.agblocks as u64;
        let blocksize = sb.blocksize as u64;
        let agf_byte = self.partition_offset + agno * agblocks * blocksize + sectsize;

        let mut agf = vec![0u8; sectsize as usize];
        read_at_aligned(&mut self.reader, agf_byte, sectsize, &mut agf)?;
        if BigEndian::read_u32(&agf[0..4]) != XFS_AGF_MAGIC {
            return Err(FilesystemError::Parse(format!(
                "AG {agno}: AGF magic lost before rewrite"
            )));
        }
        BigEndian::write_u32(&mut agf[AGF_ROOTS_OFF..AGF_ROOTS_OFF + 4], bno_root);
        BigEndian::write_u32(&mut agf[AGF_ROOTS_OFF + 4..AGF_ROOTS_OFF + 8], cnt_root);
        BigEndian::write_u32(&mut agf[AGF_LEVELS_OFF..AGF_LEVELS_OFF + 4], bno_levels);
        BigEndian::write_u32(&mut agf[AGF_LEVELS_OFF + 4..AGF_LEVELS_OFF + 8], cnt_levels);
        BigEndian::write_u32(&mut agf[AGF_FREEBLKS_OFF..AGF_FREEBLKS_OFF + 4], freeblks);
        BigEndian::write_u32(&mut agf[AGF_LONGEST_OFF..AGF_LONGEST_OFF + 4], longest);
        BigEndian::write_u32(
            &mut agf[AGF_BTREEBLKS_OFF..AGF_BTREEBLKS_OFF + 4],
            btreeblks,
        );

        self.reader.seek(SeekFrom::Start(agf_byte))?;
        self.reader.write_all(&agf)?;
        Ok(())
    }

    /// Write one filesystem block at `fsblock`.
    pub(crate) fn write_fsblock(
        &mut self,
        sb: &XfsSuperblock,
        fsblock: u64,
        data: &[u8],
    ) -> Result<(), FilesystemError> {
        use super::bmap::fsblock_to_partition_byte;
        let part_byte = fsblock_to_partition_byte(fsblock, sb.agblocks, sb.agblklog, sb.blocksize);
        self.reader
            .seek(SeekFrom::Start(self.partition_offset + part_byte))?;
        self.reader.write_all(data)?;
        Ok(())
    }

    /// Recompute `sb_fdblocks` and write it into the primary superblock.
    ///
    /// `sb_fdblocks` counts both the free blocks described by the bnobt
    /// (`agf_freeblks`) *and* the AGFL's pre-reserved blocks (`agf_flcount`),
    /// which the bnobt deliberately excludes. Omitting the AGFL term leaves
    /// `xfs_repair -n` reporting `sb_fdblocks N, counted M` (off by Σ flcount).
    fn resync_sb_fdblocks(&mut self, sb: &XfsSuperblock) -> Result<(), FilesystemError> {
        let sectsize = sb.sectsize as u64;
        let agblocks = sb.agblocks as u64;
        let blocksize = sb.blocksize as u64;
        let mut total: u64 = 0;
        let mut agf = vec![0u8; sectsize as usize];
        for agno in 0..sb.agcount as u64 {
            let agf_byte = self.partition_offset + agno * agblocks * blocksize + sectsize;
            read_at_aligned(&mut self.reader, agf_byte, sectsize, &mut agf)?;
            let parsed = XfsAgf::parse(&agf)?;
            total += parsed.freeblks as u64 + parsed.flcount as u64;
        }
        let mut primary = vec![0u8; sectsize as usize];
        read_at_aligned(
            &mut self.reader,
            self.partition_offset,
            sectsize,
            &mut primary,
        )?;
        BigEndian::write_u64(&mut primary[SB_FDBLOCKS_OFF..SB_FDBLOCKS_OFF + 8], total);
        self.reader.seek(SeekFrom::Start(self.partition_offset))?;
        self.reader.write_all(&primary)?;
        Ok(())
    }
}

/// Scan the in-use map for AG `agno` over `[0, expected_len)` and emit the
/// runs of free blocks as AG-relative `(start, len)` extents.
pub(crate) fn derive_free_extents(
    map: &InUseMap,
    agno: u64,
    agblocks: u64,
    expected_len: u64,
) -> Vec<FreeExtent> {
    let base = agno * agblocks;
    let mut out = Vec::new();
    let mut run_start: Option<u32> = None;
    for agbno in 0..expected_len {
        let free = !map.is_used(base + agbno);
        match (free, run_start) {
            (true, None) => run_start = Some(agbno as u32),
            (false, Some(start)) => {
                out.push(FreeExtent {
                    startblock: start,
                    blockcount: agbno as u32 - start,
                });
                run_start = None;
            }
            _ => {}
        }
    }
    if let Some(start) = run_start {
        out.push(FreeExtent {
            startblock: start,
            blockcount: expected_len as u32 - start,
        });
    }
    out
}

/// Carve `n` blocks off the end of the largest free extent, returning the
/// carved AG-relative block numbers and the reduced extent set. Returns `None`
/// if the largest extent has fewer than `n` blocks (degenerate AG). Carving
/// from the tail of a single extent keeps the extent *count* unchanged (the
/// extent shrinks, or vanishes only if fully consumed — which `n < len`
/// prevents), so the rebuilt tree's block math stays a one-shot calculation.
pub(crate) fn carve_from_largest(
    extents: &[FreeExtent],
    n: u32,
) -> Option<(Vec<u32>, Vec<FreeExtent>)> {
    if n == 0 {
        return Some((Vec::new(), extents.to_vec()));
    }
    let (idx, _) = extents
        .iter()
        .enumerate()
        .max_by_key(|(_, e)| e.blockcount)?;
    let big = extents[idx];
    if big.blockcount <= n {
        // Fully consuming the extent would drop the count and re-perturb the
        // math; refuse and let the caller skip this AG.
        return None;
    }
    let keep = big.blockcount - n;
    let carved: Vec<u32> = (0..n).map(|i| big.startblock + keep + i).collect();
    let mut reduced = extents.to_vec();
    reduced[idx] = FreeExtent {
        startblock: big.startblock,
        blockcount: keep,
    };
    Some((carved, reduced))
}

/// Sort by startblock and merge adjacent/touching extents into maximal runs.
/// Input extents must be non-overlapping (they come from disjoint sources:
/// free records plus the trees' own distinct blocks).
fn coalesce(mut extents: Vec<FreeExtent>) -> Vec<FreeExtent> {
    extents.sort_by_key(|e| e.startblock);
    let mut out: Vec<FreeExtent> = Vec::with_capacity(extents.len());
    for e in extents {
        match out.last_mut() {
            Some(last) if last.startblock + last.blockcount == e.startblock => {
                last.blockcount += e.blockcount;
            }
            _ => out.push(e),
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    fn fe(s: u32, c: u32) -> FreeExtent {
        FreeExtent {
            startblock: s,
            blockcount: c,
        }
    }

    #[test]
    fn derive_runs_from_map() {
        // AG 0, 10 blocks: used = {0,3,8,9}; free runs = (1,2),(4,4).
        let mut map = InUseMap::new(10);
        for b in [0, 3, 8, 9] {
            map.mark(b);
        }
        let got = derive_free_extents(&map, 0, 10, 10);
        assert_eq!(got, vec![fe(1, 2), fe(4, 4)]);
    }

    #[test]
    fn carve_shrinks_largest_tail() {
        let exts = vec![fe(1, 2), fe(4, 4), fe(12, 100)];
        let (carved, reduced) = carve_from_largest(&exts, 3).unwrap();
        assert_eq!(carved, vec![109, 110, 111]); // tail of the 100-block extent
        assert_eq!(reduced, vec![fe(1, 2), fe(4, 4), fe(12, 97)]);
    }

    #[test]
    fn carve_refuses_when_largest_too_small() {
        let exts = vec![fe(1, 2), fe(4, 3)];
        assert!(carve_from_largest(&exts, 4).is_none());
    }

    #[test]
    fn coalesce_merges_touching_runs() {
        // bnobt free record (12,8178) plus the trees' own blocks 1,2 and the
        // carved tail 8190,8191 → reconstructs the original (1,2)+(12,8180).
        let got = coalesce(vec![
            fe(12, 8178),
            fe(1, 1),
            fe(2, 1),
            fe(8190, 1),
            fe(8191, 1),
        ]);
        assert_eq!(got, vec![fe(1, 2), fe(12, 8180)]);
    }
}
