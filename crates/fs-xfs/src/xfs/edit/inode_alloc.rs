//! Inode allocation: claiming and freeing slots in inode chunks, carving new chunks, and growing the inobt.

use super::*;

impl<R: Read + Write + Seek + Send> XfsFilesystem<R> {
    /// Allocate one inode. Tries existing chunks first; when every chunk in
    /// every AG is full, allocates a fresh 64-inode chunk via
    /// [`alloc_new_inode_chunk`] and re-runs the slot search against it.
    /// Returns the new inode number.
    pub(crate) fn alloc_inode_slot(
        &mut self,
        sb: &super::super::sb::XfsSuperblock,
    ) -> Result<u64, FilesystemError> {
        // v5 lifted in §2.1 (E.5b): inobt-leaf rewrites + AGI / SB writes are
        // CRC-aware; new-chunk allocation routes through `init_free_inode_chunk`
        // which stamps each v3 inode core (E.2). The grow / single-leaf splice
        // paths both stamp the rewritten leaf's sblock-crc header before write.
        if let Some(ino) = self.try_claim_slot_from_existing_chunks(sb)? {
            return Ok(ino);
        }
        // No free slots anywhere: extend an AG's inobt with a brand-new chunk,
        // then claim slot 0 of it.
        self.alloc_new_inode_chunk(sb)?;
        self.try_claim_slot_from_existing_chunks(sb)?
            .ok_or_else(|| {
                FilesystemError::DiskFull(
                    "alloc_new_inode_chunk left no claimable slot (internal inconsistency)".into(),
                )
            })
    }

    /// First-pass slot search: walk every AG's existing inobt, claim the
    /// lowest free slot in the first record that has one. Returns `Ok(None)`
    /// when every chunk is full (the trigger for `alloc_new_inode_chunk`).
    /// Lifted from the old `alloc_inode_slot` body.
    pub(super) fn try_claim_slot_from_existing_chunks(
        &mut self,
        sb: &super::super::sb::XfsSuperblock,
    ) -> Result<Option<u64>, FilesystemError> {
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
            let hdr_len = sb.sblock_hdr_len();
            for leaf_agbno in leaves {
                let fsblock = (agno << sb.agblklog) | leaf_agbno as u64;
                let mut block = vec![0u8; bs];
                if self.read_fsblock(fsblock, &mut block).is_err() {
                    continue;
                }
                let numrecs = BigEndian::read_u16(&block[6..8]) as usize;
                let max_leaf = (bs - hdr_len) / INOBT_REC_SIZE;
                if numrecs > max_leaf {
                    continue;
                }
                for r in 0..numrecs {
                    let off = hdr_len + r * INOBT_REC_SIZE;
                    let start_agino = BigEndian::read_u32(&block[off..off + 4]) as u64;
                    let freecount = BigEndian::read_u32(&block[off + 4..off + 8]);
                    let free = BigEndian::read_u64(&block[off + 8..off + 16]);
                    if freecount == 0 || free == 0 {
                        continue;
                    }
                    let slot = free.trailing_zeros() as u64; // lowest free slot
                    let ino = (agno << ino_shift) | (start_agino + slot);
                    if matches!(self.read_inode_buf(ino), Ok((core, _)) if core.mode != 0) {
                        return Err(FilesystemError::InvalidData(format!(
                            "inode {ino} is marked free but is in use; check the volume first"
                        )));
                    }

                    // Mark allocated: clear the bit, decrement record freecount.
                    let new_free = free & !(1u64 << slot);
                    BigEndian::write_u32(&mut block[off + 4..off + 8], freecount - 1);
                    BigEndian::write_u64(&mut block[off + 8..off + 16], new_free);
                    // v5: re-stamp the leaf's sblock-crc tuple before writing.
                    if sb.is_v5() {
                        super::super::v5_crc::stamp_sblock_hdr_for_ag(
                            &mut block, sb, agno, leaf_agbno,
                        );
                    }
                    self.write_fsblock(sb, fsblock, &block)?;

                    // AGI freecount-- and sb_ifree--.
                    BigEndian::write_u32(
                        &mut agi_sec[AGI_FREECOUNT_OFF..AGI_FREECOUNT_OFF + 4],
                        agi.freecount - 1,
                    );
                    self.write_agi_sector(sb, agi_byte, &mut agi_sec)?;
                    self.bump_sb_ifree(sb, -1)?;
                    return Ok(Some(ino));
                }
            }
        }
        Ok(None)
    }

    /// Extend an AG's inobt with a brand-new 64-inode chunk. Carves
    /// `blocks_per_chunk` aligned contiguous blocks via
    /// [`alloc_blocks_aligned_in_ag`], writes 64 free dinodes there, then splices a
    /// fresh inobt record (all 64 bits set ⇒ all free) into the AG's inobt.
    /// Updates AGI `count`/`freecount` and superblock `sb_icount`/`sb_ifree`
    /// by `+64` each — the caller's slot claim later rebalances `freecount`/
    /// `sb_ifree` by `-1`.
    ///
    /// Two record-splice strategies, picked by the existing tree's shape:
    ///   * **Single-leaf splice** ([`splice_inobt_single_leaf_record`]) — when
    ///     `agi.level == 1` and the root leaf has at least one open slot, the
    ///     new record drops in alongside the existing ones with no allocation
    ///     and no AGI root/level change.
    ///   * **Multi-level grow** ([`grow_inobt_with_new_record`]) — when the
    ///     existing tree is already multi-level OR its only leaf is full, gather
    ///     every existing record + the new one, allocate fresh contiguous tree
    ///     blocks (and reclaim the old tree blocks in the same freespace
    ///     rebuild), then rebuild a balanced tree via [`build_sblock_btree`]
    ///     and relocate the AGI root + level.
    pub(crate) fn alloc_new_inode_chunk(
        &mut self,
        sb: &super::super::sb::XfsSuperblock,
    ) -> Result<(), FilesystemError> {
        // §2.1 (E.5b): v5 lifted. `init_free_inode_chunk` stamps each v3
        // inode core (E.2), `alloc_blocks_aligned_in_ag` rebuilds the host AG's
        // freespace btrees through the v5-aware `build_alloc_btree` (E.4),
        // and the splice / grow paths both stamp the AGI sector + the
        // inobt leaf or fresh tree blocks on v5 (this slice + E.4).
        let bs = sb.blocksize as u64;
        let bs_usize = bs as usize;
        let inodesize = sb.inodesize as u64;
        let blocks_per_chunk = ((XFS_INODES_PER_CHUNK as u64) * inodesize).div_ceil(bs) as u32;
        // sb_inoalignmt is the on-disk chunk alignment; fall back to the
        // chunk's own block count when it's unset (older v4 images without
        // the ALIGN feature). The fallback gives natural chunk-stride
        // alignment, which is the minimum every walker assumes.
        let alignment = if sb.inoalignmt > 0 {
            sb.inoalignmt
        } else {
            blocks_per_chunk
        };
        let max_leaf = (bs_usize - sb.sblock_hdr_len()) / INOBT_REC_SIZE;

        // di_version: every inode in a v4 volume shares the version byte the
        // root inode carries. Reading it once gives the right value for both
        // v1 (16-bit nlink, di_onlink) and v2 (32-bit nlink, di_nlink) inode
        // formats without having to read sb_features2 directly.
        let (_root_core, root_buf) = self.read_inode_buf(sb.rootino)?;
        let di_version = root_buf[4];

        // The chunk and its inobt record must share an AG, so try each AG in
        // turn and let a failed inobt growth hand the chunk back (F-018).
        for agno in 0..sb.agcount as u64 {
            match self.alloc_inode_chunk_in_ag(
                sb,
                agno,
                blocks_per_chunk,
                alignment,
                max_leaf,
                di_version,
            ) {
                Ok(()) => return Ok(()),
                Err(FilesystemError::DiskFull(_)) => continue,
                Err(e) => return Err(e),
            }
        }
        Err(FilesystemError::DiskFull(
            "no allocation group has room for an inode chunk and its inobt growth".into(),
        ))
    }

    /// One AG's attempt at [`alloc_new_inode_chunk`]: carve the chunk there,
    /// initialise it, splice or grow the AG's inobt; on `DiskFull` the chunk goes back.
    pub(super) fn alloc_inode_chunk_in_ag(
        &mut self,
        sb: &super::super::sb::XfsSuperblock,
        agno: u64,
        blocks_per_chunk: u32,
        alignment: u32,
        max_leaf: usize,
        di_version: u8,
    ) -> Result<(), FilesystemError> {
        let bs = sb.blocksize as u64;
        let bs_usize = bs as usize;
        let chunk_fsblock =
            self.alloc_blocks_aligned_in_ag(sb, agno, blocks_per_chunk, alignment)?;
        let chunk_agbno = (chunk_fsblock & ((1u64 << sb.agblklog) - 1)) as u32;
        let start_agino = ((chunk_agbno as u64) << sb.inopblog) as u32;

        // Initialize the chunk: 64 free dinodes (magic + version +
        // di_next_unlinked = NULLAGINO, mode 0 == free).
        self.init_free_inode_chunk(sb, chunk_fsblock, di_version, blocks_per_chunk as usize)?;

        // Re-read the AGI of the chosen AG so we can pick a splice strategy.
        let sectsize = sb.sectsize as u64;
        let agblocks = sb.agblocks as u64;
        let agi_byte = self.partition_offset + agno * agblocks * bs + 2 * sectsize;
        let mut agi_sec = vec![0u8; sectsize as usize];
        read_at_aligned(&mut self.reader, agi_byte, sectsize, &mut agi_sec)?;
        let agi = XfsAgi::parse(&agi_sec)?;

        // Pick the splice strategy. Single-leaf splice when (a) the tree is
        // single-level, (b) its only leaf is the AGI root, and (c) the leaf
        // has at least one open slot. Anything else → multi-level grow.
        let single_leaf_fits = if agi.level == 1 {
            let leaves = self.collect_inobt_leaf_blocks(sb, agno, agi.root)?;
            if leaves.as_slice() == [agi.root] {
                let leaf_fsblock = (agno << sb.agblklog) | agi.root as u64;
                let mut leaf = vec![0u8; bs_usize];
                self.read_fsblock(leaf_fsblock, &mut leaf)?;
                let numrecs = BigEndian::read_u16(&leaf[6..8]) as usize;
                numrecs < max_leaf
            } else {
                false
            }
        } else {
            false
        };

        let recorded = if single_leaf_fits {
            self.splice_inobt_single_leaf_record(sb, agno, agi.root, start_agino, max_leaf)
        } else {
            self.grow_inobt_with_new_record(sb, agno, agi.root, start_agino)
        };
        if let Err(e) = recorded {
            if matches!(e, FilesystemError::DiskFull(_)) {
                self.free_blocks(sb, &[(chunk_fsblock, blocks_per_chunk)])?;
            }
            return Err(e);
        }
        // Either path may have rewritten the AGI root and level; refresh.
        read_at_aligned(&mut self.reader, agi_byte, sectsize, &mut agi_sec)?;
        let agi = XfsAgi::parse(&agi_sec)?;

        // AGI count += 64, freecount += 64. The single-leaf path leaves AGI
        // root/level untouched; the multi-level grow already wrote new values
        // but left count/freecount alone, so this final step is unconditional.
        let new_count = agi
            .count
            .checked_add(XFS_INODES_PER_CHUNK as u32)
            .ok_or_else(|| FilesystemError::Parse("AGI count overflow".into()))?;
        let new_freecount = agi
            .freecount
            .checked_add(XFS_INODES_PER_CHUNK as u32)
            .ok_or_else(|| FilesystemError::Parse("AGI freecount overflow".into()))?;
        BigEndian::write_u32(&mut agi_sec[AGI_COUNT_OFF..AGI_COUNT_OFF + 4], new_count);
        BigEndian::write_u32(
            &mut agi_sec[AGI_FREECOUNT_OFF..AGI_FREECOUNT_OFF + 4],
            new_freecount,
        );
        self.write_agi_sector(sb, agi_byte, &mut agi_sec)?;

        // sb_icount += 64, sb_ifree += 64.
        self.bump_sb_inode_counters(sb, XFS_INODES_PER_CHUNK as i64, XFS_INODES_PER_CHUNK as i64)
    }

    /// Splice a fresh all-free chunk record `(start_agino, freecount=64,
    /// free=u64::MAX)` into the existing single-leaf inobt at `root_agbno`.
    /// Reads the leaf, appends the record, sorts by `start_agino`, and rewrites
    /// the leaf in place. Bumps only the leaf's `numrecs`; the leaf header
    /// (level, sibling pointers) is left untouched. Trailing-byte zero fill
    /// keeps a future bump from reading stale data.
    pub(super) fn splice_inobt_single_leaf_record(
        &mut self,
        sb: &super::super::sb::XfsSuperblock,
        agno: u64,
        root_agbno: u32,
        new_start_agino: u32,
        max_leaf: usize,
    ) -> Result<(), FilesystemError> {
        let bs_usize = sb.blocksize as usize;
        let hdr_len = sb.sblock_hdr_len();
        let leaf_fsblock = (agno << sb.agblklog) | root_agbno as u64;
        let mut leaf = vec![0u8; bs_usize];
        self.read_fsblock(leaf_fsblock, &mut leaf)?;
        let numrecs = BigEndian::read_u16(&leaf[6..8]) as usize;

        let mut records: Vec<(u32, u32, u64)> = Vec::with_capacity(numrecs + 1);
        for r in 0..numrecs {
            let off = hdr_len + r * INOBT_REC_SIZE;
            let s = BigEndian::read_u32(&leaf[off..off + 4]);
            let fc = BigEndian::read_u32(&leaf[off + 4..off + 8]);
            let free = BigEndian::read_u64(&leaf[off + 8..off + 16]);
            if s == new_start_agino {
                return Err(FilesystemError::Parse(format!(
                    "XFS new-chunk allocation: AG {agno} already has chunk at \
                     start_agino {new_start_agino}"
                )));
            }
            records.push((s, fc, free));
        }
        records.push((new_start_agino, XFS_INODES_PER_CHUNK as u32, u64::MAX));
        records.sort_by_key(|&(s, _, _)| s);

        BigEndian::write_u16(&mut leaf[6..8], (numrecs + 1) as u16);
        for (i, &(s, fc, free)) in records.iter().enumerate() {
            let off = hdr_len + i * INOBT_REC_SIZE;
            BigEndian::write_u32(&mut leaf[off..off + 4], s);
            BigEndian::write_u32(&mut leaf[off + 4..off + 8], fc);
            BigEndian::write_u64(&mut leaf[off + 8..off + 16], free);
        }
        let used = hdr_len + records.len() * INOBT_REC_SIZE;
        let stale_end = hdr_len + (numrecs + 1).min(max_leaf) * INOBT_REC_SIZE;
        for b in &mut leaf[used..stale_end.max(used)] {
            *b = 0;
        }
        // v5 leaf carries an sblock-crc tuple — restamp after the record
        // mutation so the on-disk CRC matches the rewritten payload.
        if sb.is_v5() {
            super::super::v5_crc::stamp_sblock_hdr_for_ag(&mut leaf, sb, agno, root_agbno);
        }
        self.write_fsblock(sb, leaf_fsblock, &leaf)?;
        Ok(())
    }

    /// Multi-level inobt growth: the existing tree can't take one more record
    /// without restructuring (root leaf full, or already multi-level). Gather
    /// every existing record, add the new all-free chunk, allocate fresh
    /// contiguous tree blocks (reclaiming the old tree blocks in the same
    /// freespace rebuild via [`swap_inobt_blocks_in_ag`]), build a balanced
    /// tree with [`build_sblock_btree`], write its blocks, and relocate the
    /// AGI root + level. AGI count/freecount are not touched here — the caller
    /// applies a single +64 delta after dispatch.
    pub(super) fn grow_inobt_with_new_record(
        &mut self,
        sb: &super::super::sb::XfsSuperblock,
        agno: u64,
        old_root: u32,
        new_start_agino: u32,
    ) -> Result<(), FilesystemError> {
        let bs_usize = sb.blocksize as usize;
        let sectsize = sb.sectsize as u64;
        let agblocks = sb.agblocks as u64;
        let bs = sb.blocksize as u64;

        // (1) Gather every existing record from every leaf, refuse a duplicate
        // start_agino, then add the new all-free record and key-sort.
        let old_leaves = self.collect_inobt_leaf_blocks(sb, agno, old_root)?;
        let read_hdr_len = sb.sblock_hdr_len();
        let mut records: Vec<(u32, u32, u64)> = Vec::new();
        for &leaf_agbno in &old_leaves {
            let leaf_fsblock = (agno << sb.agblklog) | leaf_agbno as u64;
            let mut leaf = vec![0u8; bs_usize];
            self.read_fsblock(leaf_fsblock, &mut leaf)?;
            let nr = BigEndian::read_u16(&leaf[6..8]) as usize;
            for r in 0..nr {
                let off = read_hdr_len + r * INOBT_REC_SIZE;
                let s = BigEndian::read_u32(&leaf[off..off + 4]);
                let fc = BigEndian::read_u32(&leaf[off + 4..off + 8]);
                let free = BigEndian::read_u64(&leaf[off + 8..off + 16]);
                if s == new_start_agino {
                    return Err(FilesystemError::Parse(format!(
                        "XFS new-chunk allocation: AG {agno} already has chunk at \
                         start_agino {new_start_agino}"
                    )));
                }
                records.push((s, fc, free));
            }
        }
        records.push((new_start_agino, XFS_INODES_PER_CHUNK as u32, u64::MAX));
        records.sort_by_key(|&(s, _, _)| s);

        // (2) Pack the records into the flat byte buffer build_sblock_btree
        // wants (each entry: startino(4) freecount(4) free(8)).
        let mut packed = vec![0u8; records.len() * INOBT_REC_SIZE];
        for (i, &(s, fc, free)) in records.iter().enumerate() {
            let off = i * INOBT_REC_SIZE;
            BigEndian::write_u32(&mut packed[off..off + 4], s);
            BigEndian::write_u32(&mut packed[off + 4..off + 8], fc);
            BigEndian::write_u64(&mut packed[off + 8..off + 16], free);
        }

        // (3) Enumerate every block the OLD tree occupies (leaves + internal
        // nodes) so the freespace swap can reclaim them.
        let old_blocks = self.collect_inobt_all_blocks(sb, agno, old_root)?;

        // (4) Reserve `need` contiguous AG-local blocks for the new tree AND
        // free the old tree blocks in one freespace rebuild — the only way to
        // avoid stranding either side when the AG's freespace is tight.
        let is_v5 = sb.is_v5();
        let need = blocks_needed_for(
            records.len(),
            bs_usize,
            INOBT_REC_SIZE,
            INOBT_KEY_SIZE,
            is_v5,
        );
        let new_tree_start_agbno =
            self.swap_inobt_blocks_in_ag(sb, agno, &old_blocks, need as u32)?;

        // (5) Build the new tree on the carved block run. Magic + sb-stamp
        // arg dispatch on `is_v5`: v4 emits `XFS_IBT_MAGIC` + no CRC; v5
        // emits `XFS_IBT_CRC_MAGIC` + the standard CRC tuple via
        // `build_sblock_btree`'s `Some(sb)` path.
        let carved_agbnos: Vec<u32> = (0..need as u32).map(|i| new_tree_start_agbno + i).collect();
        let sb_for_v5: Option<&super::super::sb::XfsSuperblock> =
            if is_v5 { Some(sb) } else { None };
        let tree = build_sblock_btree(
            &packed,
            INOBT_REC_SIZE,
            INOBT_KEY_SIZE,
            sb.inobt_magic(),
            bs_usize,
            agno as u32,
            &carved_agbnos,
            sb_for_v5,
        );

        // (6) Write every new tree block. Until the AGI root/level update at
        // step (7), the AGI still points at the old (now-freed) root — readers
        // would see garbage if the freed blocks were reused. Within this method
        // no concurrent access happens (we hold &mut self), and we don't
        // return until AGI is updated, so the on-disk window is closed.
        for blk in &tree.blocks {
            let fsblock = (agno << sb.agblklog) | blk.agbno as u64;
            self.write_fsblock(sb, fsblock, &blk.bytes)?;
        }

        // (7) Rewrite AGI root + level. Count/freecount are bumped by the
        // caller after dispatch, so we leave them alone here.
        let agi_byte = self.partition_offset + agno * agblocks * bs + 2 * sectsize;
        let mut agi_sec = vec![0u8; sectsize as usize];
        read_at_aligned(&mut self.reader, agi_byte, sectsize, &mut agi_sec)?;
        BigEndian::write_u32(
            &mut agi_sec[AGI_ROOT_OFF..AGI_ROOT_OFF + 4],
            tree.root_agbno,
        );
        BigEndian::write_u32(&mut agi_sec[AGI_LEVEL_OFF..AGI_LEVEL_OFF + 4], tree.levels);
        self.write_agi_sector(sb, agi_byte, &mut agi_sec)?;
        Ok(())
    }

    /// Reserve `n` contiguous AG-local blocks for the new inobt and return the
    /// freed `old_blocks` to the free set, all in one freespace rebuild. Adds
    /// the freed blocks to this AG's current full-free set, coalesces, carves
    /// `n` blocks from the largest extent of the merged set, then rebuilds
    /// bno/cnt + resyncs `sb_fdblocks`. Returns the AG-relative starting block
    /// of the carved run.
    ///
    /// Both alloc and free are AG-scoped: the inobt belongs to one specific AG
    /// (the AGI of that AG points at its root), so every new tree block has to
    /// live in that AG, and every reclaimed old block does too. A pure
    /// `alloc_blocks` would try other AGs on `DiskFull`; this stays put.
    pub(super) fn swap_inobt_blocks_in_ag(
        &mut self,
        sb: &super::super::sb::XfsSuperblock,
        agno: u64,
        old_blocks: &[u32],
        n: u32,
    ) -> Result<u32, FilesystemError> {
        let agblocks = sb.agblocks as u64;
        let expected_len = if agno == sb.agcount as u64 - 1 {
            sb.dblocks - agno * agblocks
        } else {
            agblocks
        };
        let mut full_free = self.current_full_free(sb, agno, expected_len)?;
        // Add the old tree blocks back to the free set as single-block extents;
        // coalesce will merge them with neighbouring runs.
        for &agbno in old_blocks {
            full_free.push(FreeExtent {
                startblock: agbno,
                blockcount: 1,
            });
        }
        let merged = coalesce(full_free);
        let (carved, free_after) = carve_from_largest(&merged, n).ok_or_else(|| {
            FilesystemError::DiskFull(format!(
                "AG {agno}: no free extent large enough for {n}-block inobt growth"
            ))
        })?;
        self.rebuild_ag_freespace(sb, agno, expected_len, &free_after)?;
        self.resync_sb_fdblocks(sb)?;
        Ok(carved[0])
    }

    /// Carve `n` contiguous blocks in AG `agno` starting on a multiple of
    /// `alignment` (the inode-chunk stride); `DiskFull` when the AG cannot.
    pub(super) fn alloc_blocks_aligned_in_ag(
        &mut self,
        sb: &super::super::sb::XfsSuperblock,
        agno: u64,
        n: u32,
        alignment: u32,
    ) -> Result<u64, FilesystemError> {
        if n == 0 || alignment == 0 {
            return Err(FilesystemError::InvalidData(
                "alloc_blocks_aligned_in_ag: zero n or alignment".into(),
            ));
        }
        let agblocks = sb.agblocks as u64;
        let expected_len = if agno == sb.agcount as u64 - 1 {
            sb.dblocks - agno * agblocks
        } else {
            agblocks
        };
        let no_room = || {
            FilesystemError::DiskFull(format!(
                "AG {agno} has no free extent admitting a {n}-block run aligned to {alignment}"
            ))
        };
        let full_free = self
            .current_full_free(sb, agno, expected_len)
            .map_err(|_| no_room())?;
        let (carved_start, free_after) =
            carve_aligned_from_largest(&full_free, n, alignment).ok_or_else(no_room)?;
        self.rebuild_ag_freespace(sb, agno, expected_len, &free_after)
            .map_err(|_| no_room())?;
        self.resync_sb_fdblocks(sb)?;
        Ok((agno << sb.agblklog) | carved_start as u64)
    }

    /// Initialize a fresh inode chunk: write 64 free dinodes at the chunk's
    /// starting fsblock. Each dinode carries `XFS_DINODE_MAGIC`, the volume's
    /// `di_version` (matching every other inode), `di_mode = 0` (the on-disk
    /// "free slot" marker), and `di_next_unlinked = NULLAGINO`. All other
    /// fields are zero, as a freshly allocated inode chunk must be for `xfs_repair -n`.
    ///
    /// **v5 (CRC) volumes**: each slot's v3 core also needs its CRC-header
    /// tuple stamped (`di_uuid` / `di_ino` / `di_lsn` / `di_crc`) so the
    /// inobt-walk readers and `xfs_repair` accept the chunk as a fresh
    /// 64-slot extension; the CRC is stamped inline once the body is written.
    /// v4 callers skip the stamp (no `di_crc` field).
    pub(super) fn init_free_inode_chunk(
        &mut self,
        sb: &super::super::sb::XfsSuperblock,
        chunk_fsblock: u64,
        di_version: u8,
        blocks_per_chunk: usize,
    ) -> Result<(), FilesystemError> {
        let inodesize = sb.inodesize as usize;
        let bs = sb.blocksize as usize;
        let span = blocks_per_chunk * bs;
        let mut buf = vec![0u8; span];
        // chunk_fsblock's starting agino = the first inode number in this chunk.
        // Each slot's di_ino = start_agino + slot, embedded as a full inumber.
        let agno = chunk_fsblock >> sb.agblklog;
        let chunk_agbno = chunk_fsblock & ((1u64 << sb.agblklog) - 1);
        let start_agino = chunk_agbno << sb.inopblog;
        let ino_shift = sb.agblklog + sb.inopblog;
        let start_ino = (agno << ino_shift) | start_agino;

        for slot in 0..XFS_INODES_PER_CHUNK {
            let base = slot * inodesize;
            BigEndian::write_u16(&mut buf[base..base + 2], XFS_DINODE_MAGIC);
            // di_mode (offset 2..4) and everything else stays zero.
            buf[base + 4] = di_version;
            BigEndian::write_u32(
                &mut buf[base + DI_NEXT_UNLINKED_OFF..base + DI_NEXT_UNLINKED_OFF + 4],
                NULLAGINO,
            );
            if sb.is_v5() {
                let slot_buf = &mut buf[base..base + inodesize];
                super::super::v5_crc::stamp_inode_v3(slot_buf, start_ino + slot as u64, sb);
            }
        }
        let part_byte =
            fsblock_to_partition_byte(chunk_fsblock, sb.agblocks, sb.agblklog, sb.blocksize);
        self.write_at(self.partition_offset + part_byte, &buf)
    }

    /// Bump `sb_icount` and `sb_ifree` by the given deltas in the primary
    /// superblock (a single read+write). Either delta of 0 is a skip.
    pub(super) fn bump_sb_inode_counters(
        &mut self,
        sb: &super::super::sb::XfsSuperblock,
        icount_delta: i64,
        ifree_delta: i64,
    ) -> Result<(), FilesystemError> {
        if icount_delta == 0 && ifree_delta == 0 {
            return Ok(());
        }
        let sectsize = sb.sectsize as u64;
        let mut primary = vec![0u8; sectsize as usize];
        read_at_aligned(
            &mut self.reader,
            self.partition_offset,
            sectsize,
            &mut primary,
        )?;
        if icount_delta != 0 {
            let cur = BigEndian::read_u64(&primary[SB_ICOUNT_OFF..SB_ICOUNT_OFF + 8]);
            let new = (cur as i64 + icount_delta).max(0) as u64;
            BigEndian::write_u64(&mut primary[SB_ICOUNT_OFF..SB_ICOUNT_OFF + 8], new);
        }
        if ifree_delta != 0 {
            let cur = BigEndian::read_u64(&primary[SB_IFREE_OFF..SB_IFREE_OFF + 8]);
            let new = (cur as i64 + ifree_delta).max(0) as u64;
            BigEndian::write_u64(&mut primary[SB_IFREE_OFF..SB_IFREE_OFF + 8], new);
        }
        self.write_sb_primary(sb, &mut primary)
    }

    /// Adjust `sb_ifree` by `delta` in the primary superblock.
    pub(super) fn bump_sb_ifree(
        &mut self,
        sb: &super::super::sb::XfsSuperblock,
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
        self.write_sb_primary(sb, &mut primary)
    }

    /// Free a previously-allocated inode: mark its inobt slot free (set the
    /// `ir_free` bit, ++ record freecount + AGI freecount + `sb_ifree`) and zero
    /// its on-disk `di_mode` so it reads as a free inode.
    pub(super) fn free_inode(
        &mut self,
        sb: &super::super::sb::XfsSuperblock,
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
        let hdr_len = sb.sblock_hdr_len();
        for leaf_agbno in leaves {
            let fsblock = (agno << sb.agblklog) | leaf_agbno as u64;
            let mut block = vec![0u8; bs];
            self.read_fsblock(fsblock, &mut block)?;
            let numrecs = BigEndian::read_u16(&block[6..8]) as usize;
            let max_leaf = (bs - hdr_len) / INOBT_REC_SIZE;
            if numrecs > max_leaf {
                continue;
            }
            for r in 0..numrecs {
                let off = hdr_len + r * INOBT_REC_SIZE;
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
                if sb.is_v5() {
                    super::super::v5_crc::stamp_sblock_hdr_for_ag(&mut block, sb, agno, leaf_agbno);
                }
                self.write_fsblock(sb, fsblock, &block)?;

                BigEndian::write_u32(
                    &mut agi_sec[AGI_FREECOUNT_OFF..AGI_FREECOUNT_OFF + 4],
                    agi.freecount + 1,
                );
                self.write_agi_sector(sb, agi_byte, &mut agi_sec)?;
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

    /// Free the inodes on each AGI's unlinked lists (files deleted while still open at a crash),
    /// as a mount does after recovery. Their blocks are left for the free-space rebuild.
    pub(crate) fn free_unlinked_inodes(&mut self) -> Result<usize, FilesystemError> {
        const NULLAGINO: u32 = u32::MAX;
        const AGI_UNLINKED_OFF: usize = 40;
        let sb = self.superblock().clone();
        let sectsize = sb.sectsize as u64;
        let ino_shift = sb.agblklog + sb.inopblog;
        let mut freed = 0usize;
        for agno in 0..u64::from(sb.agcount) {
            let agi_byte = self.partition_offset
                + agno * u64::from(sb.agblocks) * u64::from(sb.blocksize)
                + 2 * sectsize;
            let mut agi = vec![0u8; sectsize as usize];
            read_at_aligned(&mut self.reader, agi_byte, sectsize, &mut agi)?;
            let mut chain = Vec::new();
            let mut seen = std::collections::HashSet::new();
            for bucket in 0..64 {
                let o = AGI_UNLINKED_OFF + 4 * bucket;
                let mut agino = BigEndian::read_u32(&agi[o..o + 4]);
                while agino != NULLAGINO && seen.insert(agino) {
                    let ino = (agno << ino_shift) | u64::from(agino);
                    let (_c, buf) = self.read_inode_buf(ino)?;
                    chain.push(ino);
                    agino = BigEndian::read_u32(&buf[96..100]);
                }
                BigEndian::write_u32(&mut agi[o..o + 4], NULLAGINO);
            }
            if chain.is_empty() {
                continue;
            }
            self.write_agi_sector(&sb, agi_byte, &mut agi)?;
            for ino in chain {
                let (core, mut buf) = self.read_inode_buf(ino)?;
                BigEndian::write_u32(&mut buf[96..100], NULLAGINO);
                if core.nlink > 0 {
                    // Still linked (an O_TMPFILE that was given a name): only leave the list.
                    self.write_inode_region(&sb, ino, &buf)?;
                    continue;
                }
                // A free inode owns nothing: empty extent forks, no size, no blocks.
                buf[5] = 2;
                buf[56..84].fill(0);
                buf[83] = 2;
                let fork = if sb.is_v5() { 176 } else { 100 };
                buf[fork..].fill(0);
                self.write_inode_region(&sb, ino, &buf)?;
                self.free_inode(&sb, ino)?;
                freed += 1;
            }
        }
        self.reader.flush()?;
        Ok(freed)
    }
}
