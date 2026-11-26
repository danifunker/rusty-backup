//! Orphan reconnection into `lost+found` (repair R7).

use super::*;
// mrustc's macro matcher cannot read `super::super` inside a `matches!` pattern.
use super::super::types::DiFormat;

impl<R: Read + Write + Seek + Send> XfsFilesystem<R> {
    /// R7 (reconnection half): link every allocated-but-unreachable inode into
    /// `/lost+found` (created under the root if absent), naming each by its
    /// inode number. A reconnected subdirectory has its `..` repointed at
    /// `lost+found`; a detached *subtree* reconnects only at its top (children
    /// that are themselves reachable from an orphan are left in place). Link
    /// counts are not adjusted here — the R7 nlink pass runs afterwards and
    /// recomputes them from the now-connected tree.
    pub(crate) fn run_orphan_reconnect(&mut self) -> Result<RepairReport, FilesystemError> {
        let mut report = RepairReport {
            fixes_applied: Vec::new(),
            fixes_failed: Vec::new(),
            unrepairable_count: 0,
        };
        let sb = self.superblock().clone();
        // §2.1 (E.5d): v5 lifted. Reconnection creates a `/lost+found`
        // directory entry via the same `dir_insert_entry` /
        // `init_empty_shortform_dir` pipeline `do_create_directory` uses;
        // both now stamp dir3 / v3 inode core on v5.
        let root = match self.root() {
            Ok(r) => r,
            Err(_) => return Ok(report),
        };

        // Reachability BFS from the root + superblock-internal inodes. If any
        // directory can't be listed the reachable set is incomplete, so abort
        // (reconnecting then would wrongly relink genuinely-reachable inodes).
        use std::collections::HashSet;
        let mut reachable: HashSet<u64> = self.internal_inodes().into_iter().collect();
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
    pub(super) fn ensure_lost_found(
        &mut self,
        sb: &super::super::sb::XfsSuperblock,
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
        self.init_empty_shortform_dir(sb, ino, root_ino, 0o040755, 0, 0, None)?;
        self.dir_insert_entry(sb, root_ino, "lost+found", ino, true)?;
        Ok(ino)
    }

    /// Repoint a directory's `..` at `new_parent` (short-form header parent, or
    /// the `..` data entry of a single-block directory).
    pub(super) fn set_dotdot(
        &mut self,
        sb: &super::super::sb::XfsSuperblock,
        ino: u64,
        new_parent: u64,
    ) -> Result<(), FilesystemError> {
        let (core, mut buf) = self.read_inode_buf(ino)?;
        match core.format {
            DiFormat::Local => {
                let fs = sb.fork_offset();
                if buf[fs + 1] > 0 {
                    BigEndian::write_u64(&mut buf[fs + 2..fs + 10], new_parent);
                } else {
                    BigEndian::write_u32(&mut buf[fs + 2..fs + 6], new_parent as u32);
                }
                self.write_inode_region(sb, ino, &buf)
            }
            DiFormat::Extents => {
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
                let mut pos = match BigEndian::read_u32(&block[0..4]) {
                    super::super::dir2::XFS_DIR3_BLOCK_MAGIC => {
                        super::super::dir2::XFS_DIR3_DATA_HDR_LEN
                    }
                    _ => XFS_DIR2_DATA_HDR_LEN,
                };
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
    pub(super) fn enumerate_allocated_inodes(
        &mut self,
        sb: &super::super::sb::XfsSuperblock,
    ) -> Vec<u64> {
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
            let hdr_len = sb.sblock_hdr_len();
            for leaf_agbno in leaves {
                let fsblock = (agno << sb.agblklog) | leaf_agbno as u64;
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
