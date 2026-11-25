//! Directory edits: short-form insert and remove, conversion to block form, and the full layout rebuild.

use super::*;

impl<R: Read + Write + Seek + Send> XfsFilesystem<R> {
    /// Initialize a freshly-allocated inode as an empty short-form directory
    /// whose parent is `parent_ino`. Preserves the on-disk inode version and
    /// bumps the generation; writes a clean v4 dinode core + the 6-byte inline
    /// fork (`count=0, i8count=0, parent`). `nlink` is 2 (the new directory's
    /// own `.` plus its entry in the parent); no subdirectories yet.
    ///
    /// `times`, when set, replaces the default `now` stamp on the three
    /// di_atime/di_mtime/di_ctime fields so a cross-fs copy preserves the
    /// source's mtime end-to-end.
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn init_empty_shortform_dir(
        &mut self,
        sb: &super::super::sb::XfsSuperblock,
        ino: u64,
        parent_ino: u64,
        mode: u16,
        uid: u32,
        gid: u32,
        times: Option<disk_core::times::UnixTimes>,
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
        BigEndian::write_u16(&mut buf[0..2], super::super::types::XFS_DINODE_MAGIC);
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
            *b = 0; // projid, pad, atime, mtime, ctime — stamped below
        }
        stamp_v4_times(&mut buf, times);
        BigEndian::write_u64(&mut buf[56..64], 6); // di_size = empty sf fork len
        for b in buf.iter_mut().take(92).skip(64) {
            *b = 0; // nblocks, extsize, nextents, anextents, forkoff, aformat, dm*, flags
        }
        buf[83] = 2; // di_aformat = extents (no attr fork)
        BigEndian::write_u32(&mut buf[92..96], gen);
        BigEndian::write_u32(&mut buf[96..100], NULLAGINO); // di_next_unlinked

        // Inline fork: count=0, i8count=0, parent (4 bytes).
        let fs = sb.fork_offset();
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
        sb: &super::super::sb::XfsSuperblock,
        parent_ino: u64,
        name: &str,
        child_ino: u64,
        is_dir: bool,
    ) -> Result<(), FilesystemError> {
        let has_ftype = sb.has_ftype();
        let (core, mut buf) = self.read_inode_buf(parent_ino)?;
        if !core.is_dir() || core.format != super::super::types::DiFormat::Local {
            return Err(FilesystemError::Unsupported(
                "parent is not a short-form directory".into(),
            ));
        }
        let namelen = name.len();
        if namelen == 0 || namelen > 255 {
            return Err(FilesystemError::InvalidData("bad entry name length".into()));
        }

        let fs = sb.fork_offset();
        let fork_len = core.size as usize;
        let fork_end = fs + fork_len;
        if fork_end > buf.len() {
            return Err(FilesystemError::Parse(
                "short-form fork overflows inode".into(),
            ));
        }

        // Reject a duplicate name, then pick the append offset cookie from the
        // raw fork (so removal gaps are respected).
        let (_parent, entries) =
            super::super::dir2::parse_shortform(&buf[fs..fork_end], has_ftype)?;
        if entries.iter().any(|e| e.name == name) {
            return Err(FilesystemError::AlreadyExists(name.to_string()));
        }
        let new_off = self.sf_next_offset(&buf[fs..fork_end], has_ftype, sb.is_v5())?;

        // Entries take the width the header's i8count gives them all (XFS-010).
        let wide = fork_len >= 2 && buf[fs + 1] > 0;
        if !wide && child_ino > u64::from(u32::MAX) {
            return Err(FilesystemError::Unsupported(
                "an 8-byte inode number in a 4-byte short-form directory needs a conversion".into(),
            ));
        }
        let ino_width = if wide { 8 } else { 4 };
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
        if wide {
            BigEndian::write_u64(&mut buf[p..p + 8], child_ino);
            if child_ino > u64::from(u32::MAX) {
                buf[fs + 1] = buf[fs + 1].saturating_add(1);
            }
        } else {
            BigEndian::write_u32(&mut buf[p..p + 4], child_ino as u32);
        }

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
    pub(super) fn sf_next_offset(
        &self,
        fork: &[u8],
        has_ftype: bool,
        is_v5: bool,
    ) -> Result<u32, FilesystemError> {
        // The cookies mirror a block-form layout: past the dir2 / dir3 data
        // header and the `.` / `..` entries (xfs_repair checks the order).
        let hdr = if is_v5 {
            super::super::dir2::XFS_DIR3_DATA_HDR_LEN
        } else {
            XFS_DIR2_DATA_HDR_LEN
        };
        let mut next = (hdr + data_entsize(1, has_ftype) + data_entsize(2, has_ftype)) as u32;
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

    /// Remove `name` from short-form directory `parent_ino`, rebuilding the
    /// inline fork without it (count--, di_size), and decrementing the parent
    /// link count when the removed child was a subdirectory. Errors `NotFound`
    /// if the name isn't present.
    pub(super) fn sf_remove_entry(
        &mut self,
        sb: &super::super::sb::XfsSuperblock,
        parent_ino: u64,
        name: &str,
        child_was_dir: bool,
    ) -> Result<(), FilesystemError> {
        let has_ftype = sb.has_ftype();
        let (core, mut buf) = self.read_inode_buf(parent_ino)?;
        if !core.is_dir() || core.format != super::super::types::DiFormat::Local {
            return Err(FilesystemError::Unsupported(
                "parent is not a short-form directory".into(),
            ));
        }
        let fs = sb.fork_offset();
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
            if !removed && &buf[name_off..name_off + namelen] == name.as_bytes() {
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

    /// Check, without writing, that `name` can be inserted into directory
    /// `parent_ino` (unique name; parent is short-form or single-block; the
    /// result fits — converting short-form→block if the inline fork overflows).
    /// Errors `AlreadyExists` / `DiskFull` / `Unsupported` accordingly.
    pub(crate) fn dir_can_insert(
        &mut self,
        sb: &super::super::sb::XfsSuperblock,
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
        // Leaf/node form: a hash lookup for the duplicate check, and room
        // for the couple of dir blocks an insert can add.
        if matches!(
            core.format,
            super::super::types::DiFormat::Extents | super::super::types::DiFormat::Btree
        ) {
            let dup = match DiskDirStore::open(self, sb, parent_ino)? {
                Some(mut store) => {
                    Some(dir2_tree::Dir2Op::new(&mut store).contains(name.as_bytes())?)
                }
                None => None,
            };
            if let Some(dup) = dup {
                if dup {
                    return Err(FilesystemError::AlreadyExists(name.into()));
                }
                let bpd = blocks_per_dir_block(sb);
                if !self.can_alloc_run(sb, bpd)? {
                    return Err(FilesystemError::DiskFull(format!(
                        "no free run of {bpd} blocks for a directory block"
                    )));
                }
                return Ok(());
            }
        }
        let (dotdot, mut entries) = self.gather_dir_entries(sb, &core, &buf, parent_ino)?;
        if entries.iter().any(|(n, _, _)| n == name) {
            return Err(FilesystemError::AlreadyExists(name.into()));
        }
        match core.format {
            super::super::types::DiFormat::Local => {
                // Inline fit?
                let fs = sb.fork_offset();
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
                    sb.is_v5(),
                )
                .map(|_| ())
            }
            super::super::types::DiFormat::Extents => {
                let ft = if new_is_dir {
                    XFS_DIR3_FT_DIR
                } else {
                    XFS_DIR3_FT_REG_FILE
                };
                entries.push((name.to_string(), 0, ft));
                let layout = plan_dir_layout(&entries, parent_ino, dotdot, sb, None)?;
                // Probe for the largest fresh run the rebuild would need, so a
                // DiskFull can't strand the inode the caller is about to allocate.
                let extents = self.decode_data_extents(&core, &buf)?;
                let regions = dir_regions(&extents, sb);
                let bpd = blocks_per_dir_block(sb);
                let mut largest = 0u32;
                for (old, new_len) in [
                    (&regions.data, layout.data.len()),
                    (&regions.leaf, layout.leaf.len()),
                    (&regions.free, layout.free.len()),
                ] {
                    if new_len > 0 && !region_reusable(old, new_len) {
                        largest = largest.max(new_len as u32 * bpd);
                    }
                }
                if largest > 0 && !self.can_alloc_run(sb, largest)? {
                    return Err(FilesystemError::DiskFull(format!(
                        "no free run of {largest} blocks for the directory rebuild"
                    )));
                }
                Ok(())
            }
            _ => Err(FilesystemError::Unsupported(
                "parent directory format not editable (btree)".into(),
            )),
        }
    }

    /// Insert `(name -> child_ino)` into directory `parent_ino` (short-form
    /// inline, else a rebuild); bumps the parent link count for a subdirectory.
    pub(crate) fn dir_insert_entry(
        &mut self,
        sb: &super::super::sb::XfsSuperblock,
        parent_ino: u64,
        name: &str,
        child_ino: u64,
        is_dir: bool,
    ) -> Result<(), FilesystemError> {
        let (core, _buf) = self.read_inode_buf(parent_ino)?;
        match core.format {
            super::super::types::DiFormat::Local => {
                match self.sf_insert_entry(sb, parent_ino, name, child_ino, is_dir) {
                    Err(FilesystemError::DiskFull(_)) => {
                        self.convert_sf_dir_to_block(sb, parent_ino, name, child_ino, is_dir)
                    }
                    other => other,
                }
            }
            super::super::types::DiFormat::Extents | super::super::types::DiFormat::Btree => {
                self.extents_insert_entry(sb, parent_ino, name, child_ino, is_dir)
            }
            _ => Err(FilesystemError::Unsupported(
                "parent directory format not editable (btree)".into(),
            )),
        }
    }

    /// Convert an overflowing short-form directory to a single-block directory,
    /// including the new `(name -> child_ino)` entry. Allocates one directory
    /// block, writes it, and rewrites the inode to extents format.
    pub(super) fn convert_sf_dir_to_block(
        &mut self,
        sb: &super::super::sb::XfsSuperblock,
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
        let mut block = build_block_dir(
            &entries,
            parent_ino,
            dotdot,
            dirblksize,
            has_ftype,
            sb.is_v5(),
        )?;
        let blocks_per_dir = (dirblksize as u64).div_ceil(sb.blocksize as u64) as u32;
        let fsblock = self.alloc_blocks(sb, blocks_per_dir)?;
        if sb.is_v5() {
            // Stamp xfs_dir3_blk_hdr (crc/blkno/lsn/uuid/owner) over the
            // fresh `XDB3` block before it goes to disk.
            let blkno = super::super::v5_crc::fsblock_to_daddr(fsblock, sb);
            super::super::v5_crc::stamp_dir3_blk_hdr(&mut block, blkno, parent_ino, sb);
        }
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

    /// Insert `(name -> child_ino)` into a block, leaf or node-form directory
    /// by rebuilding it from its complete entry list ([`plan_dir_layout`]).
    pub(super) fn extents_insert_entry(
        &mut self,
        sb: &super::super::sb::XfsSuperblock,
        parent_ino: u64,
        name: &str,
        child_ino: u64,
        is_dir: bool,
    ) -> Result<(), FilesystemError> {
        let ft = if is_dir {
            XFS_DIR3_FT_DIR
        } else {
            XFS_DIR3_FT_REG_FILE
        };
        let handled = match DiskDirStore::open(self, sb, parent_ino)? {
            Some(mut store) => {
                let mut op = dir2_tree::Dir2Op::new(&mut store);
                match op.insert(name.as_bytes(), child_ino, ft)? {
                    dir2_tree::Outcome::Done { .. } => op.commit()?,
                    dir2_tree::Outcome::Fallback => false,
                }
            }
            None => false,
        };
        if !handled {
            let (core, buf) = self.read_inode_buf(parent_ino)?;
            let extents = self.decode_data_extents(&core, &buf)?;
            let (dotdot, mut entries) = self.read_multi_dir_entries(sb, &extents)?;
            if entries.iter().any(|(n, _, _)| n == name) {
                return Err(FilesystemError::AlreadyExists(name.into()));
            }
            entries.push((name.to_string(), child_ino, ft));
            let layout = plan_dir_layout(&entries, parent_ino, dotdot, sb, None)?;
            self.write_dir_layout(sb, parent_ino, &layout, &extents)?;
        }
        if is_dir {
            self.bump_dir_nlink(sb, parent_ino, 1)?;
        }
        Ok(())
    }

    /// Remove `name` from a block/leaf/node-form directory by rebuilding it
    /// without the entry, back to short-form when the survivors fit the inode.
    pub(super) fn extents_remove_entry(
        &mut self,
        sb: &super::super::sb::XfsSuperblock,
        parent_ino: u64,
        name: &str,
        child_was_dir: bool,
    ) -> Result<(), FilesystemError> {
        let handled = match DiskDirStore::open(self, sb, parent_ino)? {
            Some(mut store) => {
                let mut op = dir2_tree::Dir2Op::new(&mut store);
                match op.remove(name.as_bytes())? {
                    dir2_tree::Outcome::Done { shrink } => op.commit()?.then_some(shrink),
                    dir2_tree::Outcome::Fallback => None,
                }
            }
            None => None,
        };
        match handled {
            Some(true) => self.rebuild_dir_without(sb, parent_ino, None)?,
            Some(false) => {}
            None => self.rebuild_dir_without(sb, parent_ino, Some(name))?,
        }
        if child_was_dir {
            self.bump_dir_nlink(sb, parent_ino, -1)?;
        }
        Ok(())
    }

    /// Rebuild a multi-block directory from its entries, minus `drop` when
    /// given, into the smallest form that fits (down to short-form).
    pub(super) fn rebuild_dir_without(
        &mut self,
        sb: &super::super::sb::XfsSuperblock,
        ino: u64,
        drop: Option<&str>,
    ) -> Result<(), FilesystemError> {
        let (core, buf) = self.read_inode_buf(ino)?;
        let extents = self.decode_data_extents(&core, &buf)?;
        let (dotdot, mut entries) = self.read_multi_dir_entries(sb, &extents)?;
        if let Some(name) = drop {
            let before = entries.len();
            entries.retain(|(n, _, _)| n != name);
            if entries.len() == before {
                return Err(FilesystemError::NotFound(name.into()));
            }
        }
        let sf_room = data_fork_end(sb, &core, buf.len()) - sb.fork_offset();
        let layout = plan_dir_layout(&entries, ino, dotdot, sb, Some(sf_room))?;
        self.write_dir_layout(sb, ino, &layout, &extents)
    }

    /// Every entry of a multi-block directory as `(dotdot, entries)`, read
    /// from each mapped dir block of the data address space (block or data magic).
    pub(super) fn read_multi_dir_entries(
        &mut self,
        sb: &super::super::sb::XfsSuperblock,
        extents: &[super::super::bmap::XfsBmbtIrec],
    ) -> Result<(u64, DirEntries), FilesystemError> {
        let has_ftype = sb.has_ftype();
        let bpd = blocks_per_dir_block(sb) as u64;
        let ndata = dir_regions(extents, sb).data.len() as u64;
        let mut dotdot = 0u64;
        let mut out = Vec::new();
        for db in 0..ndata {
            let Some(block) = self.read_mapped_dir_block(sb, extents, db * bpd)? else {
                continue;
            };
            let (dd, entries) = match BigEndian::read_u32(&block[0..4]) {
                super::super::dir2::XFS_DIR2_BLOCK_MAGIC
                | super::super::dir2::XFS_DIR3_BLOCK_MAGIC => {
                    block_entries_with_ftype(&block, has_ftype)?
                }
                super::super::dir2::XFS_DIR2_DATA_MAGIC
                | super::super::dir2::XFS_DIR3_DATA_MAGIC => {
                    data_entries_with_ftype(&block, has_ftype)?
                }
                _ => continue,
            };
            if dd != 0 {
                dotdot = dd;
            }
            out.extend(entries);
        }
        Ok((dotdot, out))
    }

    /// Read the dir block at file offset `fb` (fsblocks) through the extent
    /// map, one fsblock at a time; `None` when any part of it is a hole.
    pub(super) fn read_mapped_dir_block(
        &mut self,
        sb: &super::super::sb::XfsSuperblock,
        extents: &[super::super::bmap::XfsBmbtIrec],
        fb: u64,
    ) -> Result<Option<Vec<u8>>, FilesystemError> {
        let bs = sb.blocksize as u64;
        let bpd = blocks_per_dir_block(sb) as u64;
        let mut block = vec![0u8; (bpd * bs) as usize];
        for i in 0..bpd {
            let Some((disk_fb, _unwritten)) = super::super::fb_to_disk(extents, fb + i) else {
                return Ok(None);
            };
            let part = fsblock_to_partition_byte(disk_fb, sb.agblocks, sb.agblklog, sb.blocksize);
            read_at_aligned(
                &mut self.reader,
                self.partition_offset + part,
                bs,
                &mut block[(i * bs) as usize..((i + 1) * bs) as usize],
            )?;
        }
        Ok(Some(block))
    }

    /// Lay a planned directory down over `extents`, its current map: a region
    /// keeps its blocks when its size is unchanged, else gets a fresh run.
    pub(super) fn write_dir_layout(
        &mut self,
        sb: &super::super::sb::XfsSuperblock,
        ino: u64,
        layout: &DirLayout,
        extents: &[super::super::bmap::XfsBmbtIrec],
    ) -> Result<(), FilesystemError> {
        if let Some(sf) = &layout.sf {
            self.write_sf_dir_inode(sb, ino, sf)?;
            let all = extent_runs_in(extents, 0, u64::MAX);
            if !all.is_empty() {
                self.free_blocks(sb, &all)?;
            }
            return Ok(());
        }
        let bpd = blocks_per_dir_block(sb);
        let leaf_off = dir2_leaf_offset_fb(sb.blocksize);
        let free_off = dir2_free_offset_fb(sb.blocksize);
        let regions = dir_regions(extents, sb);
        // Per region: (current blocks, new blocks, file range, da-style header).
        let plan = [
            (&regions.data, &layout.data, 0u64, leaf_off, false),
            (&regions.leaf, &layout.leaf, leaf_off, free_off, true),
            (&regions.free, &layout.free, free_off, u64::MAX, false),
        ];
        // Resolve every region's fsblocks before touching the disk.
        let mut placed: Vec<Vec<u64>> = Vec::with_capacity(plan.len());
        let mut fresh: Vec<(u64, u32)> = Vec::new();
        let mut to_free: Vec<(u64, u32)> = Vec::new();
        for (old, new, lo, hi, _) in &plan {
            if region_reusable(old, new.len()) {
                placed.push(old.iter().map(|b| b.unwrap_or(0)).collect());
                continue;
            }
            to_free.extend(extent_runs_in(extents, *lo, *hi));
            if new.is_empty() {
                placed.push(Vec::new());
                continue;
            }
            let n = new.len() as u32 * bpd;
            let start = match self.alloc_blocks(sb, n) {
                Ok(s) => s,
                Err(e) => {
                    if !fresh.is_empty() {
                        self.free_blocks(sb, &fresh)?;
                    }
                    return Err(e);
                }
            };
            fresh.push((start, n));
            placed.push(
                (0..new.len() as u64)
                    .map(|i| start + i * bpd as u64)
                    .collect(),
            );
        }
        // Inode extents: one record per contiguous stretch of each region.
        let mut new_extents: Vec<(u64, u64, u32)> = Vec::new();
        for ((_, _, lo, _, _), blocks) in plan.iter().zip(&placed) {
            for (i, &fsb) in blocks.iter().enumerate() {
                let fb = lo + i as u64 * bpd as u64;
                match new_extents.last_mut() {
                    Some((s, b, c)) if *s + *c as u64 == fb && *b + *c as u64 == fsb => *c += bpd,
                    _ => new_extents.push((fb, fsb, bpd)),
                }
            }
        }
        let (core, ibuf) = self.read_inode_buf(ino)?;
        let fork_room = data_fork_end(sb, &core, ibuf.len()) - sb.fork_offset();
        if new_extents.len() > bmbt_root_max(fork_room) * bmbt_leaf_max(sb) {
            if !fresh.is_empty() {
                self.free_blocks(sb, &fresh)?;
            }
            return Err(FilesystemError::Unsupported(
                "directory extent list needs a two-level bmap btree".into(),
            ));
        }
        // Stamp (v5) and write every block now that its fsblock is known.
        for ((_, new, _, _, da), blocks) in plan.iter().zip(&placed) {
            for (bytes, &fsb) in new.iter().zip(blocks) {
                let mut block = bytes.clone();
                if sb.is_v5() {
                    let daddr = super::super::v5_crc::fsblock_to_daddr(fsb, sb);
                    if *da {
                        super::super::v5_crc::stamp_da3_blkinfo(&mut block, daddr, ino, sb);
                    } else {
                        super::super::v5_crc::stamp_dir3_blk_hdr(&mut block, daddr, ino, sb);
                    }
                }
                self.write_dir_block(sb, fsb, &block)?;
            }
        }
        let nblocks = placed.iter().map(|b| b.len() as u64).sum::<u64>() * bpd as u64;
        self.write_dir_inode_multi_extent(
            sb,
            ino,
            &new_extents,
            layout.data.len() as u64 * sb.dirblksize() as u64,
            nblocks,
        )?;
        if !to_free.is_empty() {
            self.free_blocks(sb, &to_free)?;
        }
        Ok(())
    }

    /// Rewrite `ino` as a short-form directory holding `sf_fork`, zeroing the
    /// rest of the data fork's literal area first.
    pub(super) fn write_sf_dir_inode(
        &mut self,
        sb: &super::super::sb::XfsSuperblock,
        ino: u64,
        sf_fork: &[u8],
    ) -> Result<(), FilesystemError> {
        let (core, mut ibuf) = self.read_inode_buf(ino)?;
        let old_leaves = self.dir_bmbt_leaves(sb, &core, &ibuf)?;
        let fs = sb.fork_offset();
        let end = data_fork_end(sb, &core, ibuf.len());
        ibuf[5] = 1; // di_format = local
        BigEndian::write_u64(&mut ibuf[56..64], sf_fork.len() as u64); // di_size
        BigEndian::write_u64(&mut ibuf[64..72], 0); // di_nblocks
        BigEndian::write_u32(&mut ibuf[76..80], 0); // di_nextents
        for b in ibuf.iter_mut().take(end).skip(fs) {
            *b = 0;
        }
        ibuf[fs..fs + sf_fork.len()].copy_from_slice(sf_fork);
        self.write_inode_region(sb, ino, &ibuf)?;
        if !old_leaves.is_empty() {
            let runs: Vec<(u64, u32)> = old_leaves.iter().map(|&l| (l, 1)).collect();
            self.free_blocks(sb, &runs)?;
        }
        Ok(())
    }

    /// Rewrite a directory inode with N inline `(startoff, fsblock, blocks)`
    /// extents; the caller checks they fit the data fork's literal area.
    pub(super) fn write_dir_inode_multi_extent(
        &mut self,
        sb: &super::super::sb::XfsSuperblock,
        ino: u64,
        extents: &[(u64, u64, u32)],
        size: u64,
        nblocks: u64,
    ) -> Result<(), FilesystemError> {
        let (core, mut buf) = self.read_inode_buf(ino)?;
        let fs = sb.fork_offset();
        let isz = data_fork_end(sb, &core, buf.len());
        let mut leaves = self.dir_bmbt_leaves(sb, &core, &buf)?;
        // Past the literal area the records go to bmap-btree leaves under an
        // in-inode root, reusing the directory's own (`xfs_bmap_extents_to_btree`).
        let per_leaf = bmbt_leaf_max(sb);
        let needed = if fs + extents.len() * BMBT_REC_SIZE <= isz {
            0
        } else {
            extents.len().div_ceil(per_leaf)
        };
        if needed > bmbt_root_max(isz - fs) {
            return Err(FilesystemError::Unsupported(
                "directory extent list needs a two-level bmap btree".into(),
            ));
        }
        while leaves.len() < needed {
            leaves.push(self.alloc_blocks(sb, 1)?);
        }
        let surplus = leaves.split_off(needed);
        buf[5] = if needed > 0 { 3 } else { 2 }; // di_format
        BigEndian::write_u64(&mut buf[56..64], size); // di_size
        BigEndian::write_u64(&mut buf[64..72], nblocks + needed as u64); // di_nblocks
        BigEndian::write_u32(&mut buf[76..80], extents.len() as u32); // di_nextents
        for b in buf.iter_mut().take(isz).skip(fs) {
            *b = 0;
        }
        if needed > 0 {
            let mut root: Vec<(u64, u64)> = Vec::with_capacity(needed);
            for (i, chunk) in extents.chunks(per_leaf).enumerate() {
                let mut leaf_bytes = build_bmbt_leaf(chunk, sb);
                let left = if i == 0 { NULLFSBLOCK } else { leaves[i - 1] };
                let right = leaves.get(i + 1).copied().unwrap_or(NULLFSBLOCK);
                BigEndian::write_u64(&mut leaf_bytes[8..16], left);
                BigEndian::write_u64(&mut leaf_bytes[16..24], right);
                if sb.is_v5() {
                    let blkno = super::super::v5_crc::fsblock_to_daddr(leaves[i], sb);
                    super::super::v5_crc::stamp_lblock_crc_header(&mut leaf_bytes, blkno, ino, sb);
                }
                self.write_fsblock(sb, leaves[i], &leaf_bytes)?;
                root.push((chunk[0].0, leaves[i]));
            }
            write_bmbt_root(&mut buf[fs..isz], &root);
        } else {
            for (i, &(startoff, fsblock, count)) in extents.iter().enumerate() {
                let rec = encode_extent(false, startoff, fsblock, count as u64);
                buf[fs + i * 16..fs + (i + 1) * 16].copy_from_slice(&rec);
            }
        }
        self.write_inode_region(sb, ino, &buf)?;
        if !surplus.is_empty() {
            let runs: Vec<(u64, u32)> = surplus.iter().map(|&l| (l, 1)).collect();
            self.free_blocks(sb, &runs)?;
        }
        Ok(())
    }

    /// The bmap-btree leaves of a btree-format directory fork, left to right;
    /// a tree deeper than root-over-leaves is refused.
    pub(super) fn dir_bmbt_leaves(
        &mut self,
        sb: &super::super::sb::XfsSuperblock,
        core: &super::super::inode::XfsDinodeCore,
        buf: &[u8],
    ) -> Result<Vec<u64>, FilesystemError> {
        if core.format != super::super::types::DiFormat::Btree {
            return Ok(Vec::new());
        }
        let fork = &buf[sb.fork_offset()..data_fork_end(sb, core, buf.len())];
        let (level, first) = super::super::parse_bmbt_root(fork, fork.len())?;
        if level != 1 {
            return Err(FilesystemError::Unsupported(
                "directory with a two-level bmap btree is not editable".into(),
            ));
        }
        let mut leaves = vec![first];
        let mut block = vec![0u8; sb.blocksize as usize];
        loop {
            self.read_fsblock(*leaves.last().unwrap_or(&first), &mut block)?;
            let right = BigEndian::read_u64(&block[16..24]);
            if right == NULLFSBLOCK || leaves.len() > 64 {
                return Ok(leaves);
            }
            leaves.push(right);
        }
    }

    /// Remove `name` from directory `parent_ino`, dispatching by format:
    /// short-form (inline rewrite) or block/leaf/node form (rebuilt without
    /// the entry, back to short-form when the survivors fit the literal area).
    pub(super) fn dir_remove_entry(
        &mut self,
        sb: &super::super::sb::XfsSuperblock,
        parent_ino: u64,
        name: &str,
        child_was_dir: bool,
    ) -> Result<(), FilesystemError> {
        let (core, _buf) = self.read_inode_buf(parent_ino)?;
        match core.format {
            super::super::types::DiFormat::Local => {
                self.sf_remove_entry(sb, parent_ino, name, child_was_dir)
            }
            super::super::types::DiFormat::Extents | super::super::types::DiFormat::Btree => {
                self.extents_remove_entry(sb, parent_ino, name, child_was_dir)
            }
            _ => Err(FilesystemError::Unsupported(
                "parent directory format not editable (btree)".into(),
            )),
        }
    }

    /// Collect a directory's entries `(name, ino, ftype)` (excluding `.`/`..`)
    /// plus the `..` inode, from a short-form or multi-block directory.
    pub(super) fn gather_dir_entries(
        &mut self,
        sb: &super::super::sb::XfsSuperblock,
        core: &super::super::inode::XfsDinodeCore,
        buf: &[u8],
        parent_ino: u64,
    ) -> Result<(u64, DirEntries), FilesystemError> {
        let has_ftype = sb.has_ftype();
        match core.format {
            super::super::types::DiFormat::Local => {
                let fs = sb.fork_offset();
                let fork = &buf[fs..fs + core.size as usize];
                Ok(sf_entries_with_ftype(fork, has_ftype)?)
            }
            super::super::types::DiFormat::Extents | super::super::types::DiFormat::Btree => {
                let extents = self.decode_data_extents(core, buf)?;
                self.read_multi_dir_entries(sb, &extents)
            }
            _ => {
                let _ = parent_ino;
                Err(FilesystemError::Unsupported(
                    "parent directory format not editable".into(),
                ))
            }
        }
    }

    /// Write `block` bytes across the directory block's fsblock(s).
    pub(super) fn write_dir_block(
        &mut self,
        sb: &super::super::sb::XfsSuperblock,
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
    pub(super) fn set_dir_inode_single_extent(
        &mut self,
        sb: &super::super::sb::XfsSuperblock,
        ino: u64,
        fsblock: u64,
        blocks: u32,
        dirblksize: usize,
        add_subdir: bool,
    ) -> Result<(), FilesystemError> {
        let (core, mut buf) = self.read_inode_buf(ino)?;
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
        let fs = sb.fork_offset();
        let end = data_fork_end(sb, &core, buf.len());
        for b in buf.iter_mut().take(end).skip(fs) {
            *b = 0;
        }
        let rec = encode_extent(false, 0, fsblock, blocks as u64);
        buf[fs..fs + 16].copy_from_slice(&rec);
        self.write_inode_region(sb, ino, &buf)
    }

    /// Bump a directory inode's link count by `delta` (version-correct field).
    pub(super) fn bump_dir_nlink(
        &mut self,
        sb: &super::super::sb::XfsSuperblock,
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
}
