//! Directory layout planning and the block builders for every dir2 form (short-form, block,
//! leaf, node), plus the on-disk store the in-place tree editor writes through.

use super::*;

/// [`dir2_tree::Dir2Store`] over a leaf/node-form directory inode on disk:
/// reads via the extent map; `apply` allocates, stamps, writes, remaps, frees.
pub(super) struct DiskDirStore<'a, R: Read + Write + Seek + Send> {
    pub(super) fs: &'a mut XfsFilesystem<R>,
    pub(super) sb: &'a super::super::sb::XfsSuperblock,
    pub(super) ino: u64,
    pub(super) geo: dir2_tree::Geo,
    pub(super) extents: Vec<super::super::bmap::XfsBmbtIrec>,
    pub(super) mapped: std::collections::BTreeSet<u64>,
    pub(super) size: u64,
    /// Extent records the inode can address: inline, or bmbt leaves under its root.
    pub(super) extent_cap: usize,
}

impl<'a, R: Read + Write + Seek + Send> DiskDirStore<'a, R> {
    /// `None` when `ino` is not a leaf/node-form directory (no root block).
    pub(super) fn open(
        fs: &'a mut XfsFilesystem<R>,
        sb: &'a super::super::sb::XfsSuperblock,
        ino: u64,
    ) -> Result<Option<Self>, FilesystemError> {
        let (core, buf) = fs.read_inode_buf(ino)?;
        if !matches!(
            core.format,
            super::super::types::DiFormat::Extents | super::super::types::DiFormat::Btree
        ) {
            return Ok(None);
        }
        let extents = fs.decode_data_extents(&core, &buf)?;
        let geo = dir2_tree::Geo::of(sb);
        let regions = dir_regions(&extents, sb);
        let mut mapped = std::collections::BTreeSet::new();
        for (base, region) in [
            (0u64, &regions.data),
            (geo.leaf_off, &regions.leaf),
            (geo.free_off, &regions.free),
        ] {
            for (i, b) in region.iter().enumerate() {
                if b.is_some() {
                    mapped.insert(base + i as u64 * geo.bpd);
                }
            }
        }
        if !mapped.contains(&geo.leaf_off) {
            return Ok(None);
        }
        let fork_room = data_fork_end(sb, &core, buf.len()) - sb.fork_offset();
        Ok(Some(DiskDirStore {
            fs,
            sb,
            ino,
            geo,
            extents,
            mapped,
            size: core.size,
            extent_cap: bmbt_root_max(fork_room) * bmbt_leaf_max(sb),
        }))
    }
}

impl<R: Read + Write + Seek + Send> dir2_tree::Dir2Store for DiskDirStore<'_, R> {
    fn geo(&self) -> dir2_tree::Geo {
        self.geo
    }

    fn read_block(&mut self, dablk: u64) -> Result<Option<Vec<u8>>, FilesystemError> {
        if !self.mapped.contains(&dablk) {
            return Ok(None);
        }
        self.fs.read_mapped_dir_block(self.sb, &self.extents, dablk)
    }

    fn is_mapped(&self, dablk: u64) -> bool {
        self.mapped.contains(&dablk)
    }

    fn size(&self) -> u64 {
        self.size
    }

    fn apply(&mut self, txn: dir2_tree::Dir2Txn) -> Result<bool, FilesystemError> {
        let sb = self.sb;
        let bpd = self.geo.bpd;
        // Allocate every created block, next to the block before it when
        // that run is free, so the directory stays in few extents.
        let mut fresh: Vec<(u64, u64)> = Vec::new();
        for &d in &txn.created {
            let prev = fresh
                .iter()
                .find(|(fd, _)| *fd + bpd == d)
                .map(|(_, f)| *f)
                .or_else(|| {
                    d.checked_sub(bpd)
                        .and_then(|p| super::super::fb_to_disk(&self.extents, p).map(|(f, _)| f))
                });
            let mut got = None;
            if let Some(p) = prev {
                got = self.fs.alloc_blocks_at(sb, p + bpd, bpd as u32)?;
            }
            let fsb = match got {
                Some(f) => f,
                None => match self.fs.alloc_blocks_from_head(sb, bpd as u32) {
                    Ok(f) => f,
                    Err(e) => {
                        let runs: Vec<(u64, u32)> =
                            fresh.iter().map(|(_, f)| (*f, bpd as u32)).collect();
                        if !runs.is_empty() {
                            self.fs.free_blocks(sb, &runs)?;
                        }
                        return Err(e);
                    }
                },
            };
            fresh.push((d, fsb));
        }
        // The new map: every mapped fsblock minus the freed dir blocks, plus
        // the created ones, coalesced into extent records.
        let mut map: std::collections::BTreeMap<u64, u64> = std::collections::BTreeMap::new();
        for e in &self.extents {
            for i in 0..e.blockcount {
                let fb = e.startoff + i;
                if !txn.freed.contains(&(fb - fb % bpd)) {
                    map.insert(fb, e.startblock + i);
                }
            }
        }
        for &(d, fsb) in &fresh {
            for i in 0..bpd {
                map.insert(d + i, fsb + i);
            }
        }
        let mut runs: Vec<(u64, u64, u32)> = Vec::new();
        for (&fb, &fsb) in &map {
            match runs.last_mut() {
                Some((s, b, c)) if *s + u64::from(*c) == fb && *b + u64::from(*c) == fsb => *c += 1,
                _ => runs.push((fb, fsb, 1)),
            }
        }
        if runs.len() > self.extent_cap {
            let back: Vec<(u64, u32)> = fresh.iter().map(|(_, f)| (*f, bpd as u32)).collect();
            if !back.is_empty() {
                self.fs.free_blocks(sb, &back)?;
            }
            return Ok(false);
        }
        for (d, bytes) in &txn.dirty {
            let Some(&fsb) = map.get(d) else {
                return Err(FilesystemError::Parse(format!(
                    "directory block {d} written without a mapping"
                )));
            };
            let mut block = bytes.clone();
            if sb.is_v5() {
                let daddr = super::super::v5_crc::fsblock_to_daddr(fsb, sb);
                if self.geo.is_leaf_space(*d) {
                    super::super::v5_crc::stamp_da3_blkinfo(&mut block, daddr, self.ino, sb);
                } else {
                    super::super::v5_crc::stamp_dir3_blk_hdr(&mut block, daddr, self.ino, sb);
                }
            }
            self.fs.write_dir_block(sb, fsb, &block)?;
        }
        self.fs
            .write_dir_inode_multi_extent(sb, self.ino, &runs, txn.size, map.len() as u64)?;
        let mut gone: Vec<(u64, u32)> = Vec::new();
        for &d in &txn.freed {
            for i in 0..bpd {
                if let Some((f, _)) = super::super::fb_to_disk(&self.extents, d + i) {
                    gone.push((f, 1));
                }
            }
        }
        if !gone.is_empty() {
            self.fs.free_blocks(sb, &gone)?;
        }
        Ok(true)
    }
}

/// dir2 leaf-block magics (`xfs_da_blkinfo.magic`, big-endian u16 at byte 8):
/// leaf1 = the single-leaf form, leafN = a leaf of the node-form da btree.
pub(super) const XFS_DIR2_LEAF1_MAGIC: u16 = 0xD2F1;
pub(super) const XFS_DIR2_LEAFN_MAGIC: u16 = 0xD2FF;
pub(super) const XFS_DIR3_LEAF1_MAGIC: u16 = 0x3DF1;
pub(super) const XFS_DIR3_LEAFN_MAGIC: u16 = 0x3DFF;
/// da btree intermediate-node magics (v4 / v5), same byte position.
pub(super) const XFS_DA_NODE_MAGIC: u16 = 0xFEBE;
pub(super) const XFS_DA3_NODE_MAGIC: u16 = 0x3EBE;
/// dir2 freeindex block magics: `XD2F` (v4) / `XDF3` (v5), u32 at byte 0.
pub(super) const XFS_DIR2_FREE_MAGIC: u32 = 0x5844_3246;
pub(super) const XFS_DIR3_FREE_MAGIC: u32 = 0x5844_4633;
/// `NULLDATAOFF`: the freeindex best of a data block that doesn't exist.
pub(super) const NULLDATAOFF: u16 = 0xFFFF;

/// dir2 leaf address space starts at byte 32 GiB in the inode's file offset
/// stream: `XFS_DIR2_LEAF_OFFSET` = `XFS_DIR2_SPACE_SIZE` = `1 << (32 + 3)`,
/// the 3 being the data alignment log (xfs_da_format.h). In fsblocks.
pub(super) fn dir2_leaf_offset_fb(blocksize: u32) -> u64 {
    (1u64 << 35) / (blocksize as u64)
}

/// dir2 freeindex address space: `XFS_DIR2_FREE_OFFSET` = 2 x 32 GiB, in fsblocks.
pub(super) fn dir2_free_offset_fb(blocksize: u32) -> u64 {
    (1u64 << 36) / (blocksize as u64)
}

/// fsblocks per directory block (`dirblksize / blocksize`, at least 1).
pub(super) fn blocks_per_dir_block(sb: &super::super::sb::XfsSuperblock) -> u32 {
    (sb.dirblksize() as u64)
        .div_ceil(sb.blocksize as u64)
        .max(1) as u32
}

/// Header length of a data block: `xfs_dir2_data_hdr` (16) / `xfs_dir3_data_hdr` (64).
pub(super) fn dir2_data_hdr_len(is_v5: bool) -> usize {
    if is_v5 {
        super::super::dir2::XFS_DIR3_DATA_HDR_LEN
    } else {
        XFS_DIR2_DATA_HDR_LEN
    }
}

/// Header length shared by leaf1/leafN and da-node blocks: blkinfo + count +
/// stale-or-level, padded to 8 on v5 (`xfs_dir3_leaf_hdr` / `xfs_da3_node_hdr`).
pub(super) fn dir2_da_hdr_len(is_v5: bool) -> usize {
    if is_v5 {
        64
    } else {
        16
    }
}

/// Header length of a freeindex block: `xfs_dir2_free_hdr` (16) / `xfs_dir3_free_hdr` (64).
pub(super) fn dir2_free_hdr_len(is_v5: bool) -> usize {
    if is_v5 {
        64
    } else {
        16
    }
}

/// Where a directory inode's data fork ends in its buffer: the attr fork's
/// start when the inode has one, else the inode's end.
pub(super) fn data_fork_end(
    sb: &super::super::sb::XfsSuperblock,
    core: &super::super::inode::XfsDinodeCore,
    inode_len: usize,
) -> usize {
    if core.forkoff > 0 {
        (sb.fork_offset() + core.forkoff as usize * 8).min(inode_len)
    } else {
        inode_len
    }
}

/// A multi-block directory's dir blocks per dir2 address space: the first
/// fsblock of each dir block, `None` where it is a hole or not contiguous.
pub(super) struct DirRegions {
    pub(super) data: Vec<Option<u64>>,
    pub(super) leaf: Vec<Option<u64>>,
    pub(super) free: Vec<Option<u64>>,
}

/// Map `extents` onto the three dir2 address spaces (see [`DirRegions`]).
pub(super) fn dir_regions(
    extents: &[super::super::bmap::XfsBmbtIrec],
    sb: &super::super::sb::XfsSuperblock,
) -> DirRegions {
    let bpd = blocks_per_dir_block(sb) as u64;
    let leaf_off = dir2_leaf_offset_fb(sb.blocksize);
    let free_off = dir2_free_offset_fb(sb.blocksize);
    let region = |lo: u64, hi: u64| -> Vec<Option<u64>> {
        let mut end = lo;
        for e in extents {
            let e_end = e.startoff.saturating_add(e.blockcount);
            if e.startoff < hi && e_end > lo {
                end = end.max(e_end.min(hi));
            }
        }
        (0..(end - lo).div_ceil(bpd))
            .map(|i| {
                let fb = lo + i * bpd;
                let (first, _) = super::super::fb_to_disk(extents, fb)?;
                for j in 1..bpd {
                    let (disk, _) = super::super::fb_to_disk(extents, fb + j)?;
                    if disk != first + j {
                        return None;
                    }
                }
                Some(first)
            })
            .collect()
    };
    DirRegions {
        data: region(0, leaf_off),
        leaf: region(leaf_off, free_off),
        free: region(free_off, u64::MAX),
    }
}

/// A region is rewritten in place when it keeps its size and every dir block
/// is mapped contiguously.
pub(super) fn region_reusable(old: &[Option<u64>], new_len: usize) -> bool {
    old.len() == new_len && old.iter().all(|b| b.is_some())
}

/// The `(fsblock, count)` runs of `extents` inside file range `[lo, hi)`.
pub(super) fn extent_runs_in(
    extents: &[super::super::bmap::XfsBmbtIrec],
    lo: u64,
    hi: u64,
) -> Vec<(u64, u32)> {
    let mut out = Vec::new();
    for e in extents {
        let s = e.startoff.max(lo);
        let end = e.startoff.saturating_add(e.blockcount).min(hi);
        if end > s {
            out.push((e.startblock + (s - e.startoff), (end - s) as u32));
        }
    }
    out
}

/// A rebuilt directory's blocks per dir2 address space (unstamped), or `sf`
/// alone when the entries fit the inode literal area.
pub(super) struct DirLayout {
    pub(super) data: Vec<Vec<u8>>,
    pub(super) leaf: Vec<Vec<u8>>,
    pub(super) free: Vec<Vec<u8>>,
    pub(super) sf: Option<Vec<u8>>,
}

/// Plan the smallest dir2 form holding `entries`: short-form when `sf_room`
/// (literal-area bytes) allows, else block, leaf or node form (module doc).
pub(super) fn plan_dir_layout(
    entries: &[(String, u64, u8)],
    self_ino: u64,
    dotdot: u64,
    sb: &super::super::sb::XfsSuperblock,
    sf_room: Option<usize>,
) -> Result<DirLayout, FilesystemError> {
    let has_ftype = sb.has_ftype();
    let is_v5 = sb.is_v5();
    let dirblksize = sb.dirblksize() as usize;
    let mut layout = DirLayout {
        data: Vec::new(),
        leaf: Vec::new(),
        free: Vec::new(),
        sf: None,
    };
    if let Some(room) = sf_room {
        if let Some(sf) = build_sf_fork(entries, dotdot, has_ftype, is_v5) {
            if sf.len() <= room {
                layout.sf = Some(sf);
                return Ok(layout);
            }
        }
    }
    match build_block_dir(entries, self_ino, dotdot, dirblksize, has_ftype, is_v5) {
        Ok(block) => {
            layout.data.push(block);
            return Ok(layout);
        }
        Err(FilesystemError::DiskFull(_)) => {}
        Err(e) => return Err(e),
    }

    // Multi-block: entries in hash order, data blocks filled front to back.
    let mut sorted = entries.to_vec();
    sorted.sort_by_key(|(n, _, _)| dir_hashname(n.as_bytes()));
    let hdr = dir2_data_hdr_len(is_v5);
    let mut leaf_index: Vec<(u32, u32)> = Vec::new();
    let mut bests: Vec<u16> = Vec::new();
    let mut i = 0;
    while i < sorted.len() || bests.is_empty() {
        let db = bests.len();
        let mut room = dirblksize - hdr;
        if db == 0 {
            room -= dir2_data_entsize(1, has_ftype) + dir2_data_entsize(2, has_ftype);
        }
        let mut j = i;
        while j < sorted.len() {
            let ent = dir2_data_entsize(sorted[j].0.len(), has_ftype);
            if ent > room {
                break;
            }
            room -= ent;
            j += 1;
        }
        if j == i && i < sorted.len() {
            return Err(FilesystemError::DiskFull(format!(
                "'{}' does not fit a {dirblksize}-byte directory block",
                sorted[i].0
            )));
        }
        let built = build_leaf_data_block(
            &sorted[i..j],
            self_ino,
            dotdot,
            db as u64,
            dirblksize,
            has_ftype,
            db == 0,
            is_v5,
        )?;
        leaf_index.extend(built.leaf_index);
        bests.push(built.best);
        layout.data.push(built.bytes);
        i = j;
    }
    leaf_index.sort_by_key(|&(h, _)| h);

    let leaf_hdr = dir2_da_hdr_len(is_v5);
    if leaf_hdr + leaf_index.len() * 8 + bests.len() * 2 + 4 <= dirblksize {
        layout
            .leaf
            .push(build_leaf1_block(&leaf_index, &bests, dirblksize, is_v5)?);
        return Ok(layout);
    }

    // Node form. Leaves are balanced; the root is the lone leafN itself or
    // a level-1 node at XFS_DIR2_LEAF_OFFSET with the leaves after it.
    let leaf_max = (dirblksize - leaf_hdr) / 8;
    let per_leaf = leaf_index
        .len()
        .div_ceil(leaf_index.len().div_ceil(leaf_max).max(1));
    let chunks: Vec<&[(u32, u32)]> = leaf_index.chunks(per_leaf.max(1)).collect();
    let nleaves = chunks.len();
    if nleaves > (dirblksize - leaf_hdr) / 8 {
        return Err(FilesystemError::Unsupported(
            "directory too large for a one-level dir2 node tree".into(),
        ));
    }
    let bpd = blocks_per_dir_block(sb) as u64;
    let leaf_off = dir2_leaf_offset_fb(sb.blocksize);
    let first_leaf = usize::from(nleaves > 1);
    let dablk = |leaf: usize| -> u32 { (leaf_off + (first_leaf + leaf) as u64 * bpd) as u32 };
    let mut children: Vec<(u32, u32)> = Vec::with_capacity(nleaves);
    let mut leaves: Vec<Vec<u8>> = Vec::with_capacity(nleaves);
    for (idx, chunk) in chunks.iter().enumerate() {
        let back = if idx == 0 { 0 } else { dablk(idx - 1) };
        let forw = if idx + 1 == nleaves {
            0
        } else {
            dablk(idx + 1)
        };
        leaves.push(build_leafn_block(chunk, forw, back, dirblksize, is_v5));
        children.push((chunk.last().map_or(0, |e| e.0), dablk(idx)));
    }
    if nleaves > 1 {
        layout
            .leaf
            .push(build_da_node_block(&children, dirblksize, is_v5));
    }
    layout.leaf.extend(leaves);
    layout
        .free
        .push(build_free_block(&bests, dirblksize, is_v5)?);
    Ok(layout)
}

/// Short-form fork bytes for `entries` under parent `dotdot`, or `None` when
/// an inode number needs the 8-byte (`i8count`) encoding we don't write.
pub(super) fn build_sf_fork(
    entries: &[(String, u64, u8)],
    dotdot: u64,
    has_ftype: bool,
    is_v5: bool,
) -> Option<Vec<u8>> {
    if dotdot > u64::from(u32::MAX)
        || entries.len() > usize::from(u8::MAX)
        || entries.iter().any(|(_, ino, _)| *ino > u64::from(u32::MAX))
    {
        return None;
    }
    let mut fork = Vec::with_capacity(6 + entries.len() * 16);
    fork.push(entries.len() as u8);
    fork.push(0); // i8count
    fork.extend_from_slice(&(dotdot as u32).to_be_bytes());
    // Offset cookies: each entry's byte position in the notional data block,
    // after the header and the synthetic `.`/`..`.
    let mut data_off = dir2_data_hdr_len(is_v5)
        + dir2_data_entsize(1, has_ftype)
        + dir2_data_entsize(2, has_ftype);
    for (name, ino, ft) in entries {
        fork.push(name.len() as u8);
        fork.extend_from_slice(&(data_off as u16).to_be_bytes());
        fork.extend_from_slice(name.as_bytes());
        if has_ftype {
            fork.push(*ft);
        }
        fork.extend_from_slice(&(*ino as u32).to_be_bytes());
        data_off += dir2_data_entsize(name.len(), has_ftype);
    }
    Some(fork)
}

/// A built data block: bytes, the leaf index's `(hashval, dataptr)` per
/// entry (`dataptr = (db * dirblksize + off) / 8`), and `bestfree[0].length`.
pub(super) struct DataBlockWithIndex {
    pub(super) bytes: Vec<u8>,
    pub(super) leaf_index: Vec<(u32, u32)>,
    pub(super) best: u16,
}

/// Build data block `db` (XD2D / XDD3) holding `entries`, plus `.`/`..` when
/// `extra_dot_dotdot`; the tail gap is one unused record in `bestfree[0]`.
#[allow(clippy::too_many_arguments)] // 8 args: v4 builder shape + is_v5 flag
pub(super) fn build_leaf_data_block(
    entries: &[(String, u64, u8)],
    self_ino: u64,
    dotdot: u64,
    db: u64,
    dirblksize: usize,
    has_ftype: bool,
    extra_dot_dotdot: bool,
    is_v5: bool,
) -> Result<DataBlockWithIndex, FilesystemError> {
    let mut all: Vec<(Vec<u8>, u64, u8)> = Vec::with_capacity(entries.len() + 2);
    if extra_dot_dotdot {
        all.push((b".".to_vec(), self_ino, XFS_DIR3_FT_DIR));
        all.push((b"..".to_vec(), dotdot, XFS_DIR3_FT_DIR));
    }
    for (n, ino, ft) in entries {
        all.push((n.as_bytes().to_vec(), *ino, *ft));
    }

    let data_size: usize = all
        .iter()
        .map(|(n, _, _)| dir2_data_entsize(n.len(), has_ftype))
        .sum();
    let data_start = dir2_data_hdr_len(is_v5);
    let bestfree0_off = if is_v5 { 48 } else { 4 };
    if data_start + data_size > dirblksize {
        return Err(FilesystemError::DiskFull(format!(
            "leaf-form data block overflow: {} entries need {} bytes, block is {dirblksize}",
            all.len(),
            data_start + data_size
        )));
    }

    let mut buf = vec![0u8; dirblksize];
    let magic = if is_v5 {
        super::super::dir2::XFS_DIR3_DATA_MAGIC
    } else {
        super::super::dir2::XFS_DIR2_DATA_MAGIC
    };
    BigEndian::write_u32(&mut buf[0..4], magic);

    let block_base_units = (db * (dirblksize as u64 / 8)) as u32;
    let mut leaf: Vec<(u32, u32)> = Vec::with_capacity(all.len());
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
        BigEndian::write_u16(&mut buf[pos + ent - 2..pos + ent], pos as u16);
        leaf.push((dir_hashname(name), block_base_units + (pos / 8) as u32));
        pos += ent;
    }

    // Free region at the end (if any): freetag(0xFFFF) + length(2) + tag(2).
    let gap_off = pos;
    let gap_len = dirblksize - pos;
    let mut best = 0u16;
    if gap_len >= 8 {
        BigEndian::write_u16(&mut buf[gap_off..gap_off + 2], 0xFFFF);
        BigEndian::write_u16(&mut buf[gap_off + 2..gap_off + 4], gap_len as u16);
        BigEndian::write_u16(
            &mut buf[gap_off + gap_len - 2..gap_off + gap_len],
            gap_off as u16,
        );
        // bestfree[0] = (offset, length); bestfree[1..3] stay zero.
        BigEndian::write_u16(&mut buf[bestfree0_off..bestfree0_off + 2], gap_off as u16);
        BigEndian::write_u16(
            &mut buf[bestfree0_off + 2..bestfree0_off + 4],
            gap_len as u16,
        );
        best = gap_len as u16;
    }

    Ok(DataBlockWithIndex {
        bytes: buf,
        leaf_index: leaf,
        best,
    })
}

/// Fill the prefix shared by leaf1/leafN blocks: `xfs_da_blkinfo` (forw,
/// back, magic), count, stale = 0, then `entries` (already hash-sorted).
pub(super) fn write_leaf_prefix(
    buf: &mut [u8],
    magic: u16,
    entries: &[(u32, u32)],
    forw: u32,
    back: u32,
    is_v5: bool,
) {
    BigEndian::write_u32(&mut buf[0..4], forw);
    BigEndian::write_u32(&mut buf[4..8], back);
    BigEndian::write_u16(&mut buf[8..10], magic);
    let count_off = if is_v5 { 56 } else { 12 };
    BigEndian::write_u16(&mut buf[count_off..count_off + 2], entries.len() as u16);
    let hdr = dir2_da_hdr_len(is_v5);
    for (i, (h, a)) in entries.iter().enumerate() {
        let off = hdr + i * 8;
        BigEndian::write_u32(&mut buf[off..off + 4], *h);
        BigEndian::write_u32(&mut buf[off + 4..off + 8], *a);
    }
}

/// Build a leaf1 block: header + hash-sorted entries at the front, then
/// `bests[bestcount]` (u16 per data block) and the u32 `bestcount` at the end.
pub(super) fn build_leaf1_block(
    entries: &[(u32, u32)],
    bests: &[u16],
    dirblksize: usize,
    is_v5: bool,
) -> Result<Vec<u8>, FilesystemError> {
    let tail = bests.len() * 2 + 4;
    let used = dir2_da_hdr_len(is_v5) + entries.len() * 8 + tail;
    if used > dirblksize {
        return Err(FilesystemError::DiskFull(format!(
            "leaf1 block overflow: needed {used} bytes, have {dirblksize}"
        )));
    }
    let mut buf = vec![0u8; dirblksize];
    let magic = if is_v5 {
        XFS_DIR3_LEAF1_MAGIC
    } else {
        XFS_DIR2_LEAF1_MAGIC
    };
    write_leaf_prefix(&mut buf, magic, entries, 0, 0, is_v5);
    let bests_off = dirblksize - tail;
    for (i, &b) in bests.iter().enumerate() {
        BigEndian::write_u16(&mut buf[bests_off + i * 2..bests_off + i * 2 + 2], b);
    }
    BigEndian::write_u32(&mut buf[dirblksize - 4..dirblksize], bests.len() as u32);
    Ok(buf)
}

/// Build a leafN block: like leaf1 without the bests tail, linked to its
/// hash-order siblings by dablk (`forw`/`back`, 0 at either end).
pub(super) fn build_leafn_block(
    entries: &[(u32, u32)],
    forw: u32,
    back: u32,
    dirblksize: usize,
    is_v5: bool,
) -> Vec<u8> {
    let mut buf = vec![0u8; dirblksize];
    let magic = if is_v5 {
        XFS_DIR3_LEAFN_MAGIC
    } else {
        XFS_DIR2_LEAFN_MAGIC
    };
    write_leaf_prefix(&mut buf, magic, entries, 0, 0, is_v5);
    BigEndian::write_u32(&mut buf[0..4], forw);
    BigEndian::write_u32(&mut buf[4..8], back);
    buf
}

/// Build a level-1 da btree node: one `(hashval, before)` per child leafN,
/// where `hashval` is the child's highest hash and `before` its dablk.
pub(super) fn build_da_node_block(
    children: &[(u32, u32)],
    dirblksize: usize,
    is_v5: bool,
) -> Vec<u8> {
    let mut buf = vec![0u8; dirblksize];
    let magic = if is_v5 {
        XFS_DA3_NODE_MAGIC
    } else {
        XFS_DA_NODE_MAGIC
    };
    BigEndian::write_u16(&mut buf[8..10], magic);
    let count_off = if is_v5 { 56 } else { 12 };
    BigEndian::write_u16(&mut buf[count_off..count_off + 2], children.len() as u16);
    BigEndian::write_u16(&mut buf[count_off + 2..count_off + 4], 1); // level above leaves
    let hdr = dir2_da_hdr_len(is_v5);
    for (i, (h, before)) in children.iter().enumerate() {
        let off = hdr + i * 8;
        BigEndian::write_u32(&mut buf[off..off + 4], *h);
        BigEndian::write_u32(&mut buf[off + 4..off + 8], *before);
    }
    buf
}

/// Build the node form's freeindex block for data blocks `0..bests.len()`:
/// header (magic, firstdb = 0, nvalid, nused) then `bests[]`.
pub(super) fn build_free_block(
    bests: &[u16],
    dirblksize: usize,
    is_v5: bool,
) -> Result<Vec<u8>, FilesystemError> {
    let hdr = dir2_free_hdr_len(is_v5);
    if bests.len() > (dirblksize - hdr) / 2 {
        return Err(FilesystemError::Unsupported(
            "directory needs more than one dir2 freeindex block".into(),
        ));
    }
    let mut buf = vec![0u8; dirblksize];
    let magic = if is_v5 {
        XFS_DIR3_FREE_MAGIC
    } else {
        XFS_DIR2_FREE_MAGIC
    };
    BigEndian::write_u32(&mut buf[0..4], magic);
    let fields = if is_v5 { 48 } else { 4 };
    BigEndian::write_u32(&mut buf[fields..fields + 4], 0); // firstdb
    BigEndian::write_u32(&mut buf[fields + 4..fields + 8], bests.len() as u32); // nvalid
    let nused = bests.iter().filter(|&&b| b != NULLDATAOFF).count() as u32;
    BigEndian::write_u32(&mut buf[fields + 8..fields + 12], nused);
    for (i, &b) in bests.iter().enumerate() {
        BigEndian::write_u16(&mut buf[hdr + i * 2..hdr + i * 2 + 2], b);
    }
    Ok(buf)
}

/// Build a single-block (`XD2B` v4 / `XDB3` v5) directory holding `.`, `..`,
/// and `entries`. `self_ino` is the directory's own inode (its `.`),
/// `dotdot` its parent. Returns `DiskFull` if it doesn't fit one `dirblksize`
/// block.
///
/// **v5 (CRC)**: header is 64 bytes (`XFS_DIR3_DATA_HDR_LEN`) instead of 16,
/// magic is `XFS_DIR3_BLOCK_MAGIC`, and `bestfree[0]` sits at offset 48..52
/// of the block (after the 48-byte `xfs_dir3_blk_hdr`). The CRC tuple
/// (crc/blkno/lsn/uuid/owner) is left zero here — the caller stamps it via
/// [`v5_crc::stamp_dir3_blk_hdr`] once the destination fsblock is known.
pub(super) fn build_block_dir(
    entries: &[(String, u64, u8)],
    self_ino: u64,
    dotdot: u64,
    dirblksize: usize,
    has_ftype: bool,
    is_v5: bool,
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
    let data_start = if is_v5 {
        super::super::dir2::XFS_DIR3_DATA_HDR_LEN
    } else {
        XFS_DIR2_DATA_HDR_LEN
    };
    // bestfree[0].offset / .length live inside the data header. v4 packs the
    // bestfree triple right after the magic at byte 4..16; v5 packs it after
    // the 48-byte xfs_dir3_blk_hdr (bytes 48..60) with a 4-byte pad after.
    let bestfree0_off = if is_v5 { 48 } else { 4 };
    let leaf_start = dirblksize
        .checked_sub(leaf_section)
        .ok_or_else(|| FilesystemError::DiskFull("dir block too small".into()))?;
    if data_start + data_size > leaf_start {
        return Err(FilesystemError::DiskFull(
            "directory entries exceed one block (leaf/node format not implemented)".into(),
        ));
    }

    let mut buf = vec![0u8; dirblksize];
    let magic = if is_v5 {
        super::super::dir2::XFS_DIR3_BLOCK_MAGIC
    } else {
        super::super::dir2::XFS_DIR2_BLOCK_MAGIC
    };
    BigEndian::write_u32(&mut buf[0..4], magic);

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
        BigEndian::write_u16(&mut buf[bestfree0_off..bestfree0_off + 2], gap_off as u16);
        BigEndian::write_u16(
            &mut buf[bestfree0_off + 2..bestfree0_off + 4],
            gap_len as u16,
        );
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
pub(super) fn sf_entries_with_ftype(
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
    if fork.len() < 2 + ino_width {
        return Err(FilesystemError::Parse("short-form fork too small".into()));
    }
    let parent = read_ino(&fork[2..2 + ino_width]);
    let mut out = Vec::with_capacity(count.min(fork.len() / 4));
    let mut pos = 2 + ino_width;
    for _ in 0..count {
        let name_off = pos + 3;
        let Some(&namelen) = fork.get(pos) else {
            return Err(FilesystemError::Parse(
                "short-form entry count runs past the fork".into(),
            ));
        };
        let name_end = name_off + namelen as usize;
        let ino_off = name_end + usize::from(has_ftype);
        if ino_off + ino_width > fork.len() {
            return Err(FilesystemError::Parse(
                "short-form entry runs past the fork".into(),
            ));
        }
        let ft = if has_ftype { fork[name_end] } else { 0 };
        let ino = read_ino(&fork[ino_off..ino_off + ino_width]);
        out.push((strict_name(&fork[name_off..name_end])?, ino, ft));
        pos = ino_off + ino_width;
    }
    Ok((parent, out))
}

/// A directory name for a rebuild: refused unless UTF-8, as a lossy copy would be written back.
pub(super) fn strict_name(name: &[u8]) -> Result<String, FilesystemError> {
    String::from_utf8(name.to_vec()).map_err(|_| {
        FilesystemError::Unsupported(format!(
            "the directory holds a name that is not UTF-8 ({}); editing it would rewrite that name",
            String::from_utf8_lossy(name)
        ))
    })
}

/// Walk a single-block directory into `(dotdot_ino, [(name, ino, ftype)])`,
/// skipping `.`/`..`: the data entries stop where the block's leaf tail begins.
pub(super) fn block_entries_with_ftype(
    block: &[u8],
    has_ftype: bool,
) -> Result<(u64, DirEntries), FilesystemError> {
    let block_end = block.len();
    if block_end < 64 {
        return Err(FilesystemError::Parse("directory block too small".into()));
    }
    let tail = block_end - 8;
    let leaf_count = BigEndian::read_u32(&block[tail..tail + 4]) as usize;
    let data_end = block_end.saturating_sub(leaf_count.saturating_mul(8).saturating_add(8));
    walk_data_entries(block, data_end, has_ftype)
}

/// Walk a leaf/node-form data block (`XD2D`/`XDD3`): entries run to the end.
pub(super) fn data_entries_with_ftype(
    block: &[u8],
    has_ftype: bool,
) -> Result<(u64, DirEntries), FilesystemError> {
    walk_data_entries(block, block.len(), has_ftype)
}

/// Data-entry walker shared by the block and data forms: skips unused
/// records, returns `..`'s inode separately and drops `.`.
pub(super) fn walk_data_entries(
    block: &[u8],
    data_end: usize,
    has_ftype: bool,
) -> Result<(u64, DirEntries), FilesystemError> {
    let mut out = Vec::new();
    let mut dotdot = 0u64;
    // A v5 block (XDB3/XDD3) carries the 64-byte dir3 header; a v4 one 16 bytes.
    // Starting at 16 on v5 read the header as entries and found none (R-063).
    let mut pos = match BigEndian::read_u32(&block[0..4]) {
        super::super::dir2::XFS_DIR3_BLOCK_MAGIC | super::super::dir2::XFS_DIR3_DATA_MAGIC => {
            super::super::dir2::XFS_DIR3_DATA_HDR_LEN
        }
        _ => XFS_DIR2_DATA_HDR_LEN,
    };
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
        if pos + 9 > data_end {
            break;
        }
        let ino = BigEndian::read_u64(&block[pos..pos + 8]);
        let namelen = block[pos + 8] as usize;
        if pos + 9 + namelen + usize::from(has_ftype) > data_end {
            return Err(FilesystemError::Parse(format!(
                "directory entry at byte {pos} runs past the data area"
            )));
        }
        let name = &block[pos + 9..pos + 9 + namelen];
        let ft = if has_ftype {
            block[pos + 9 + namelen]
        } else {
            0
        };
        if name == b".." {
            dotdot = ino;
        } else if name != b"." {
            out.push((strict_name(name)?, ino, ft));
        }
        pos += dir2_data_entsize(namelen, has_ftype);
    }
    Ok((dotdot, out))
}
