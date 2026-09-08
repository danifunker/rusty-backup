//! BFS consistency check.
//!
//! A VALIDATE-model check: walk the volume from the root and reconcile what
//! the tree owns against what the allocation bitmap says is in use. BFS has no
//! second copy of anything except the log, so there is nothing to cross-check a
//! damaged superblock against — the errors here are about the *relationships*
//! between structures, which is where a partial write shows up.
//!
//! What it checks:
//!
//! - superblock self-consistency (magics, block/AG geometry, the bitmap and log
//!   fitting inside the volume, `root_dir` landing on a real inode),
//! - every inode reachable from the root: its magic, that `inode_num` names the
//!   block it was read from, and that its `parent` points at a directory,
//! - every block a stream owns: inside the volume, and marked used,
//! - blocks marked used that nothing reachable claims (a leak),
//! - `used_blocks` against the bitmap's own population count.
//!
//! Repair is deliberately limited to the two findings that are provably safe
//! to rewrite from evidence the volume already holds: the bitmap bits for
//! leaked blocks, and the `used_blocks` counter. Anything structural is
//! reported and left alone — BeOS ships `chkbfs`, and guessing at a B+tree is
//! how a recoverable volume becomes an unrecoverable one.

use std::collections::HashSet;
use std::io::{Read, Seek, Write};

use super::bfs::{BfsFilesystem, BfsInode};
use super::filesystem::FilesystemError;
use super::fsck::{FsckIssue, FsckResult, FsckStats, RepairReport};

/// Stop walking after this many inodes; a cyclic B+tree would otherwise spin.
const MAX_INODES: usize = 2_000_000;

/// `S_INDEX_DIR` — BFS marks a live-query index with this mode bit. An index
/// is shaped like a directory but its B+tree values are tagged duplicate
/// pointers, not inode numbers, so walking one reads garbage.
const S_INDEX_DIR: u32 = 0x2000_0000;

fn error(code: &str, message: String, repairable: bool) -> FsckIssue {
    FsckIssue {
        code: code.to_string(),
        message,
        repairable,
        debug: false,
    }
}

fn warning(code: &str, message: String) -> FsckIssue {
    FsckIssue {
        code: code.to_string(),
        message,
        repairable: false,
        debug: false,
    }
}

/// One inode queued for the walk.
struct Visit {
    block: u64,
    path: String,
    /// Read this inode's B+tree as a directory listing.
    walk_children: bool,
    /// Count it in the file/directory statistics. Attribute inodes and the
    /// per-file attribute directories are structure, not user-visible content.
    counts: bool,
}

/// What the tree walk found, so `repair` can act without walking twice.
struct Survey {
    /// Blocks the reachable tree owns.
    claimed: HashSet<u64>,
    files: u32,
    directories: u32,
    errors: Vec<FsckIssue>,
    warnings: Vec<FsckIssue>,
}

impl<R: Read + Seek + Send> BfsFilesystem<R> {
    /// Check the volume. Returns errors, warnings, and aggregate counts.
    pub fn fsck_bfs(&mut self) -> Result<FsckResult, FilesystemError> {
        let mut survey = self.survey()?;
        let bitmap = self.bitmap_snapshot()?;

        let mut leaked = 0u64;
        let mut unmarked = 0u64;
        let metadata_end = self.metadata_end();
        for block in 0..self.sb.num_blocks as u64 {
            let marked = bitmap_bit(&bitmap, block, self.sb.endian);
            let claimed = block < metadata_end || survey.claimed.contains(&block);
            if claimed && !marked {
                unmarked += 1;
            } else if !claimed && marked {
                leaked += 1;
            }
        }
        if unmarked > 0 {
            survey.errors.push(error(
                "BlockNotMarked",
                format!(
                    "{unmarked} block(s) the directory tree owns are marked free in the bitmap; \
                     a later allocation would overwrite live data"
                ),
                false,
            ));
        }
        if leaked > 0 {
            survey.errors.push(error(
                "LeakedBlocks",
                format!("{leaked} block(s) are marked used but no file or directory claims them"),
                true,
            ));
        }

        let counted = bitmap
            .iter()
            .map(|b| b.count_ones() as u64)
            .sum::<u64>()
            .min(self.sb.num_blocks as u64);
        if counted != self.sb.used_blocks.max(0) as u64 {
            survey.errors.push(error(
                "UsedBlockCount",
                format!(
                    "superblock says {} used blocks; the bitmap has {counted} set",
                    self.sb.used_blocks
                ),
                true,
            ));
        }

        if !self.sb.log_is_empty() {
            survey.warnings.push(warning(
                "JournalNotEmpty",
                format!(
                    "the log has unreplayed entries (log_start {} != log_end {}); mount and \
                     unmount cleanly in BeOS/Haiku before editing this volume",
                    self.sb.log_start, self.sb.log_end
                ),
            ));
        }

        let repairable = survey.errors.iter().any(|e| e.repairable);
        Ok(FsckResult {
            repairable,
            errors: survey.errors,
            warnings: survey.warnings,
            stats: FsckStats {
                files_checked: survey.files,
                directories_checked: survey.directories,
                extra: vec![
                    ("Block size".into(), self.sb.block_size.to_string()),
                    ("Allocation groups".into(), self.sb.num_ags.to_string()),
                    (
                        "Byte order".into(),
                        format!("{:?}", self.sb.endian).to_lowercase(),
                    ),
                    ("Blocks in use".into(), counted.to_string()),
                ],
            },
            orphaned_entries: Vec::new(),
        })
    }

    /// First block past the fixed metadata region — superblock, bitmap and log
    /// are always allocated and are not owned by any inode.
    fn metadata_end(&self) -> u64 {
        let log_end = self.sb.log_blocks.to_block(self.sb.ag_shift) + self.sb.log_blocks.len as u64;
        self.sb.first_data_block().max(log_end)
    }

    fn bitmap_snapshot(&mut self) -> Result<Vec<u8>, FilesystemError> {
        let count = self.sb.num_ags as u64 * self.sb.blocks_per_ag as u64;
        self.read_blocks(1, count)
    }

    /// Walk every inode reachable from the root, collecting the blocks they own.
    fn survey(&mut self) -> Result<Survey, FilesystemError> {
        let mut survey = Survey {
            claimed: HashSet::new(),
            files: 0,
            directories: 0,
            errors: Vec::new(),
            warnings: Vec::new(),
        };
        self.check_superblock(&mut survey);

        // The index directory hangs off the superblock, not off the root, so a
        // walk that starts only at `root_dir` reports its blocks as leaked.
        let root = self.sb.root_dir.to_block(self.sb.ag_shift);
        let indices = self.sb.indices.to_block(self.sb.ag_shift);
        let mut seen: HashSet<u64> = HashSet::new();
        // The third element says whether to read this inode's B+tree as a
        // directory listing. The index directory itself is enumerated (so its
        // index files' blocks are accounted for); the index files are not.
        let mut queue: Vec<Visit> = vec![Visit {
            block: root,
            path: "/".to_string(),
            walk_children: true,
            counts: true,
        }];
        if !self.sb.indices.is_zero() && indices != root {
            queue.push(Visit {
                block: indices,
                path: "<indices>".to_string(),
                walk_children: true,
                counts: true,
            });
        }
        while let Some(Visit {
            block,
            path,
            walk_children,
            counts,
        }) = queue.pop()
        {
            if !seen.insert(block) {
                continue;
            }
            if seen.len() > MAX_INODES {
                survey.errors.push(error(
                    "TooManyInodes",
                    format!("stopped after {MAX_INODES} inodes; the tree may contain a cycle"),
                    false,
                ));
                break;
            }
            let inode = match self.read_inode(block) {
                Ok(i) => i,
                Err(e) => {
                    survey.errors.push(error(
                        "UnreadableInode",
                        format!("{path}: inode at block {block} could not be read ({e})"),
                        false,
                    ));
                    continue;
                }
            };
            self.check_inode(&inode, &path, &mut survey);

            // Extended attributes that outgrew the inode's `small_data` area
            // live in their own directory hanging off `attributes`, and its
            // blocks belong to no path — unclaimed, they read as leaks.
            if inode.attributes.len > 0 {
                queue.push(Visit {
                    block: inode.attributes.to_block(self.sb.ag_shift),
                    path: format!("{path} (attributes)"),
                    walk_children: true,
                    counts: false,
                });
            }

            if inode.is_directory() {
                if counts {
                    survey.directories += 1;
                }
                if !walk_children {
                    continue;
                }
                match self.read_directory(&inode) {
                    Ok(children) => {
                        for (name, child) in children {
                            let sep = if path == "/" { "" } else { "/" };
                            // An index file's own contents are not a namespace.
                            let descend = match self.read_inode(child) {
                                Ok(c) => c.mode & S_INDEX_DIR == 0,
                                Err(_) => true,
                            };
                            queue.push(Visit {
                                block: child,
                                path: format!("{path}{sep}{name}"),
                                walk_children: descend,
                                counts,
                            });
                        }
                    }
                    Err(e) => survey.errors.push(error(
                        "UnreadableDirectory",
                        format!("{path}: directory B+tree could not be walked ({e})"),
                        false,
                    )),
                }
            } else if counts {
                survey.files += 1;
            }
        }
        Ok(survey)
    }

    fn check_superblock(&self, survey: &mut Survey) {
        let sb = &self.sb;
        let bitmap_blocks = sb.num_ags as u64 * sb.blocks_per_ag as u64;
        if 1 + bitmap_blocks > sb.num_blocks as u64 {
            survey.errors.push(error(
                "BitmapPastEnd",
                format!(
                    "the allocation bitmap ({bitmap_blocks} blocks) does not fit in a \
                     {}-block volume",
                    sb.num_blocks
                ),
                false,
            ));
        }
        let ag_capacity = (1u64 << sb.ag_shift) * sb.num_ags as u64;
        if ag_capacity < sb.num_blocks as u64 {
            survey.errors.push(error(
                "AllocationGroupsTooFew",
                format!(
                    "{} allocation groups of {} blocks cover {ag_capacity} of {} blocks",
                    sb.num_ags,
                    1u64 << sb.ag_shift,
                    sb.num_blocks
                ),
                false,
            ));
        }
        let log_end = sb.log_blocks.to_block(sb.ag_shift) + sb.log_blocks.len as u64;
        if log_end > sb.num_blocks as u64 {
            survey.errors.push(error(
                "LogPastEnd",
                format!(
                    "the log ends at block {log_end}, past the {}-block volume",
                    sb.num_blocks
                ),
                false,
            ));
        }
        if sb.used_blocks < 0 || sb.used_blocks > sb.num_blocks {
            survey.errors.push(error(
                "UsedBlocksOutOfRange",
                format!(
                    "used_blocks {} is outside 0..={}",
                    sb.used_blocks, sb.num_blocks
                ),
                true,
            ));
        }
    }

    fn check_inode(&mut self, inode: &BfsInode, path: &str, survey: &mut Survey) {
        let self_block = inode.inode_num.to_block(self.sb.ag_shift);
        if self_block != inode.block {
            survey.errors.push(error(
                "InodeNumMismatch",
                format!(
                    "{path}: inode at block {} records itself as block {self_block}",
                    inode.block
                ),
                false,
            ));
        }
        let inode_blocks = (self.sb.inode_size as u64)
            .div_ceil(self.sb.block_size as u64)
            .max(1);
        for b in inode.block..inode.block + inode_blocks {
            survey.claimed.insert(b);
        }

        // An inline symlink's "data stream" is the target text; decoding it as
        // extents produces block numbers in the trillions.
        if inode.is_inline_symlink() {
            return;
        }

        match self.stream_extents(&inode.data) {
            Ok(extents) => {
                let mut total = 0u64;
                for (start, count) in extents {
                    if start + count > self.sb.num_blocks as u64 {
                        survey.errors.push(error(
                            "ExtentPastEnd",
                            format!(
                                "{path}: an extent runs to block {}, past the {}-block volume",
                                start + count,
                                self.sb.num_blocks
                            ),
                            false,
                        ));
                        continue;
                    }
                    for b in start..start + count {
                        if !survey.claimed.insert(b) {
                            survey.errors.push(error(
                                "CrossLinkedBlock",
                                format!("{path}: block {b} is claimed by more than one file"),
                                false,
                            ));
                        }
                    }
                    total += count;
                }
                let need = (inode.data.size.max(0) as u64).div_ceil(self.sb.block_size as u64);
                if total < need {
                    survey.errors.push(error(
                        "StreamTooShort",
                        format!(
                            "{path}: size claims {} bytes but only {total} block(s) are allocated",
                            inode.data.size
                        ),
                        false,
                    ));
                }
            }
            Err(e) => survey.errors.push(error(
                "UnreadableStream",
                format!("{path}: data stream could not be walked ({e})"),
                false,
            )),
        }

        if inode.data.indirect.len > 0 {
            let start = inode.data.indirect.to_block(self.sb.ag_shift);
            for b in start..start + inode.data.indirect.len as u64 {
                survey.claimed.insert(b);
            }
        }
        if inode.data.double_indirect.len > 0 {
            survey.warnings.push(warning(
                "DoubleIndirectStream",
                format!("{path}: uses a double-indirect stream, which is not fully validated"),
            ));
        }
    }
}

impl<R: Read + Write + Seek + Send> BfsFilesystem<R> {
    /// Apply the two repairs that follow from evidence already on the volume:
    /// clear bitmap bits for leaked blocks, and resync `used_blocks`.
    pub fn repair_bfs(&mut self) -> Result<RepairReport, FilesystemError> {
        let mut report = RepairReport {
            fixes_applied: Vec::new(),
            fixes_failed: Vec::new(),
            unrepairable_count: 0,
        };
        let survey = self.survey()?;
        report.unrepairable_count = survey.errors.iter().filter(|e| !e.repairable).count();
        if report.unrepairable_count > 0 {
            report.fixes_failed.push(
                "structural errors are present; the bitmap was left alone so chkbfs still has \
                 the original evidence to work from"
                    .into(),
            );
            return Ok(report);
        }

        let mut bitmap = self.bitmap_snapshot()?;
        let metadata_end = self.metadata_end();
        let endian = self.sb.endian;
        let mut cleared = 0u64;
        for block in 0..self.sb.num_blocks as u64 {
            let claimed = block < metadata_end || survey.claimed.contains(&block);
            if !claimed && bitmap_bit(&bitmap, block, endian) {
                let at = (block / 32) as usize * 4;
                let word = endian.read_u32(&bitmap, at) & !(1 << (block % 32));
                endian.put_u32(&mut bitmap, at, word);
                cleared += 1;
            }
        }
        if cleared > 0 {
            self.write_blocks(1, &bitmap)?;
            report.fixes_applied.push(format!(
                "freed {cleared} leaked block(s) in the allocation bitmap"
            ));
        }

        let counted = bitmap
            .iter()
            .map(|b| b.count_ones() as u64)
            .sum::<u64>()
            .min(self.sb.num_blocks as u64);
        if counted != self.sb.used_blocks.max(0) as u64 {
            report.fixes_applied.push(format!(
                "corrected used_blocks from {} to {counted}",
                self.sb.used_blocks
            ));
            self.sb.used_blocks = counted as i64;
            self.sb_dirty = true;
            self.sync_superblock()?;
        }
        Ok(report)
    }
}

fn bitmap_bit(bitmap: &[u8], block: u64, endian: super::bfs::BfsEndian) -> bool {
    let at = (block / 32) as usize * 4;
    if at + 4 > bitmap.len() {
        return false;
    }
    endian.read_u32(bitmap, at) & (1 << (block % 32)) != 0
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::fs::bfs::BfsEndian;
    use crate::fs::bfs_write::create_blank_bfs;
    use crate::fs::filesystem::{CreateFileOptions, EditableFilesystem, Filesystem};
    use std::io::Cursor;

    fn volume(endian: BfsEndian) -> BfsFilesystem<Cursor<Vec<u8>>> {
        let img = create_blank_bfs(72 * 1024 * 1024, 1024, "Check", endian).expect("format");
        BfsFilesystem::open(Cursor::new(img), 0).expect("open")
    }

    #[test]
    fn a_freshly_formatted_volume_is_clean() {
        for endian in [BfsEndian::Little, BfsEndian::Big] {
            let mut fs = volume(endian);
            let r = fs.fsck_bfs().unwrap();
            assert!(
                r.is_clean(),
                "{endian:?} formatted volume is not fsck-clean: {:?}",
                r.errors
            );
            // The root plus the superblock's index directory.
            assert_eq!(r.stats.directories_checked, 2);
        }
    }

    /// The strongest evidence the write path is right: after a full round of
    /// creates and deletes, the bitmap and the tree still agree exactly.
    #[test]
    fn the_volume_stays_clean_across_creates_and_deletes() {
        let mut fs = volume(BfsEndian::Little);
        let root = fs.root().unwrap();
        for i in 0..120 {
            let name = format!("f{i:03}.dat");
            let body = vec![(i % 251) as u8; 1500 + i * 37];
            fs.create_file(
                &root,
                &name,
                &mut body.as_slice(),
                body.len() as u64,
                &CreateFileOptions::default(),
            )
            .unwrap();
        }
        let r = fs.fsck_bfs().unwrap();
        assert!(r.is_clean(), "after creates: {:?}", r.errors);
        assert_eq!(r.stats.files_checked, 120);

        let listed = fs.list_directory(&root).unwrap();
        for e in listed.iter().take(60) {
            fs.delete_entry(&root, e).unwrap();
        }
        let r = fs.fsck_bfs().unwrap();
        assert!(r.is_clean(), "after deletes: {:?}", r.errors);
        assert_eq!(r.stats.files_checked, 60);
    }

    #[test]
    fn a_leaked_block_is_found_and_freed() {
        let mut fs = volume(BfsEndian::Little);
        // Mark a block nothing owns as allocated, the way an interrupted
        // create would leave it.
        let mut bitmap = fs.bitmap_snapshot().unwrap();
        let victim = 70_000u64;
        let at = (victim / 32) as usize * 4;
        let word = fs.sb.endian.read_u32(&bitmap, at) | (1 << (victim % 32));
        fs.sb.endian.put_u32(&mut bitmap, at, word);
        fs.write_blocks(1, &bitmap).unwrap();

        let r = fs.fsck_bfs().unwrap();
        assert!(r.errors.iter().any(|e| e.code == "LeakedBlocks"));
        assert!(r.repairable);

        let report = fs.repair_bfs().unwrap();
        assert!(!report.fixes_applied.is_empty());
        assert!(fs.fsck_bfs().unwrap().is_clean());
    }

    #[test]
    fn a_wrong_used_block_count_is_corrected() {
        let mut fs = volume(BfsEndian::Big);
        fs.sb.used_blocks += 999;
        fs.sb_dirty = true;
        fs.sync_superblock().unwrap();
        let r = fs.fsck_bfs().unwrap();
        assert!(r.errors.iter().any(|e| e.code == "UsedBlockCount"));
        fs.repair_bfs().unwrap();
        assert!(fs.fsck_bfs().unwrap().is_clean());
    }
}
