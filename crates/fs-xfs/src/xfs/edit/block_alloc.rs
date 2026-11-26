//! Block allocation and freeing, through a rebuild of the AG's free-space btrees.

use super::*;

impl<R: Read + Write + Seek + Send> XfsFilesystem<R> {
    /// Allocate `n` **contiguous** free blocks, returning their starting fsblock.
    /// Carves the run off the tail of an AG's largest free extent and rebuilds
    /// that AG's free-space btrees over the remainder (reusing the R2 rebuild),
    /// then resyncs `sb_fdblocks`. Errors `DiskFull` when no AG has a single
    /// free extent large enough (no multi-extent split yet).
    pub(crate) fn alloc_blocks(
        &mut self,
        sb: &super::super::sb::XfsSuperblock,
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

    /// Allocate exactly `[want, want + n)` when those blocks are free, so a
    /// directory grows contiguously; `None` when they are not.
    pub(super) fn alloc_blocks_at(
        &mut self,
        sb: &super::super::sb::XfsSuperblock,
        want: u64,
        n: u32,
    ) -> Result<Option<u64>, FilesystemError> {
        let agno = want >> sb.agblklog;
        let agbno = (want & ((1u64 << sb.agblklog) - 1)) as u32;
        if agno >= sb.agcount as u64 || n == 0 {
            return Ok(None);
        }
        let agblocks = sb.agblocks as u64;
        let expected_len = if agno == sb.agcount as u64 - 1 {
            sb.dblocks - agno * agblocks
        } else {
            agblocks
        };
        if u64::from(agbno) + u64::from(n) > expected_len {
            return Ok(None);
        }
        let Ok(full_free) = self.current_full_free(sb, agno, expected_len) else {
            return Ok(None);
        };
        let Some(idx) = full_free
            .iter()
            .position(|e| e.startblock <= agbno && agbno + n <= e.startblock + e.blockcount)
        else {
            return Ok(None);
        };
        let host = full_free[idx];
        let mut reduced = full_free.clone();
        reduced.remove(idx);
        if agbno > host.startblock {
            reduced.push(FreeExtent {
                startblock: host.startblock,
                blockcount: agbno - host.startblock,
            });
        }
        let host_end = host.startblock + host.blockcount;
        if agbno + n < host_end {
            reduced.push(FreeExtent {
                startblock: agbno + n,
                blockcount: host_end - (agbno + n),
            });
        }
        let reduced = coalesce(reduced);
        if self
            .rebuild_ag_freespace(sb, agno, expected_len, &reduced)
            .is_err()
        {
            return Ok(None);
        }
        self.resync_sb_fdblocks(sb)?;
        Ok(Some(want))
    }

    /// Allocate `n` blocks from the *head* of the largest free extent, leaving
    /// the blocks after them free for `alloc_blocks_at` (a tail carve would fragment).
    pub(super) fn alloc_blocks_from_head(
        &mut self,
        sb: &super::super::sb::XfsSuperblock,
        n: u32,
    ) -> Result<u64, FilesystemError> {
        let agblocks = sb.agblocks as u64;
        for agno in 0..sb.agcount as u64 {
            let expected_len = if agno == sb.agcount as u64 - 1 {
                sb.dblocks - agno * agblocks
            } else {
                agblocks
            };
            let Ok(full_free) = self.current_full_free(sb, agno, expected_len) else {
                continue;
            };
            let Some(host) = full_free.iter().max_by_key(|e| e.blockcount).copied() else {
                continue;
            };
            if host.blockcount <= n {
                continue;
            }
            let start = (agno << sb.agblklog) | u64::from(host.startblock);
            if self.alloc_blocks_at(sb, start, n)?.is_some() {
                return Ok(start);
            }
        }
        self.alloc_blocks(sb, n)
    }

    /// The AG's complete free-block set (AG-relative extents): the bnobt's free
    /// records plus the blocks both space btrees themselves occupy (which a
    /// rebuild reclaims), coalesced — the same reconstruction R2 uses.
    pub(super) fn current_full_free(
        &mut self,
        sb: &super::super::sb::XfsSuperblock,
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
            self.walk_alloc_tree(sb, agno, agf.bno_root, expected_len, sb.bnobt_magic())?;
        let (_cnt_recs, cnt_blocks) =
            self.walk_alloc_tree(sb, agno, agf.cnt_root, expected_len, sb.cntbt_magic())?;
        let mut all = recs;
        for agbno in bno_blocks.into_iter().chain(cnt_blocks) {
            all.push(FreeExtent {
                startblock: agbno,
                blockcount: 1,
            });
        }
        // Allocate only from records that lie inside the AG and do not overlap (XFS-007).
        all.sort_by_key(|e| e.startblock);
        let mut end = 0u64;
        for e in &all {
            let (start, len) = (u64::from(e.startblock), u64::from(e.blockcount));
            if len == 0 || start + len > expected_len || start < end {
                return Err(FilesystemError::InvalidData(format!(
                    "AG {agno}: free-space record [{start}, +{len}) is outside the AG or overlaps \
                     another; check the volume first"
                )));
            }
            end = start + len;
        }
        Ok(coalesce(all))
    }

    /// Allocate `n` total blocks as one or more contiguous runs (returns
    /// `(fsblock, count)` per run, file-offset assignment is the caller's
    /// job). Tries a single contiguous extent first via [`alloc_blocks`]; if
    /// that fails (`DiskFull`), walks AGs largest-first and carves multiple
    /// runs from one AG's free set. The run count is capped at
    /// [`bmbt_leaf_max`] — the on-disk ceiling for a single-leaf bmap btree
    /// (§2.1 hole (D)). Errors `DiskFull` when no AG can satisfy `n` within
    /// that cap. The largest individual run is left uncapped here; oversize
    /// runs are split into per-record-sized extents downstream by
    /// [`runs_to_extent_records`].
    pub(super) fn alloc_extents(
        &mut self,
        sb: &super::super::sb::XfsSuperblock,
        n: u64,
    ) -> Result<Vec<(u64, u32)>, FilesystemError> {
        // Fast path: one contiguous run when n fits a u32 (alloc_blocks's
        // interface). Single-extent allocations larger than 2^32 blocks are
        // theoretically possible but absurd for our target volumes, so this
        // is a reasonable cap on the fast-path attempt.
        if n <= u64::from(u32::MAX) {
            match self.alloc_blocks(sb, n as u32) {
                Ok(fsblock) => return Ok(vec![(fsblock, n as u32)]),
                Err(FilesystemError::DiskFull(_)) => {}
                Err(e) => return Err(e),
            }
        }

        let bs = sb.blocksize as usize;
        let leaf_max = bmbt_leaf_max(sb);
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
            if total < n {
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
                let take = remaining.min(ext.blockcount as u64) as u32;
                runs.push((ext.startblock, take));
                remaining -= take as u64;
                if take < ext.blockcount {
                    post.push(FreeExtent {
                        startblock: ext.startblock + take,
                        blockcount: ext.blockcount - take,
                    });
                }
            }
            if remaining > 0 || runs.len() > leaf_max {
                continue;
            }
            // The rebuild carves its btree blocks from the largest remaining
            // free extent, so make sure one is big enough.
            let post_largest = post.iter().map(|e| e.blockcount).max().unwrap_or(0) as usize;
            let need_bt = 2 * blocks_needed(post.len(), bs, sb.is_v5());
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
            "could not allocate {n} blocks as <= {leaf_max} contiguous runs"
        )))
    }

    /// Return `extents` (as `(fsblock, count)`) to the free-space btrees by
    /// rebuilding each touched AG's bno/cnt over its current free set plus the
    /// freed blocks, then resyncing `sb_fdblocks`. The inverse of
    /// [`alloc_blocks`].
    pub(super) fn free_blocks(
        &mut self,
        sb: &super::super::sb::XfsSuperblock,
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
            let ag_len = if agno + 1 == sb.agcount as u64 {
                sb.dblocks.saturating_sub(agno * agblocks)
            } else {
                agblocks
            };
            if agno >= sb.agcount as u64 || u64::from(agbno) + u64::from(count) > ag_len {
                return Err(FilesystemError::InvalidData(format!(
                    "extent at fsblock {fsblock} (+{count}) is outside the volume; check it first"
                )));
            }
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

    /// Whether some AG has one free run of `n` blocks (read-only probe).
    pub(super) fn can_alloc_run(
        &mut self,
        sb: &super::super::sb::XfsSuperblock,
        n: u32,
    ) -> Result<bool, FilesystemError> {
        let agblocks = sb.agblocks as u64;
        for agno in 0..sb.agcount as u64 {
            let expected_len = if agno == sb.agcount as u64 - 1 {
                sb.dblocks - agno * agblocks
            } else {
                agblocks
            };
            let Ok(free) = self.current_full_free(sb, agno, expected_len) else {
                continue;
            };
            if carve_from_largest(&free, n).is_some() {
                return Ok(true);
            }
        }
        Ok(false)
    }
}
