//! BeOS OFS consistency check.
//!
//! The same VALIDATE model as the other bitmap filesystems here: walk the
//! directory tree, collect every sector it owns, and reconcile that against
//! the allocation bitmap and the table of contents' `used_sectors`.
//!
//! OFS keeps no redundancy at all — one table of contents, one bitmap, one
//! copy of each directory chain — so a structural error has nothing to be
//! repaired *from*. Repair is therefore limited to the two findings the volume
//! itself proves: bits set for sectors nothing claims, and a `used_sectors`
//! that disagrees with the bitmap's own population count.

use std::collections::HashSet;
use std::io::{Read, Seek, Write};

use super::filesystem::FilesystemError;
use super::fsck::{FsckIssue, FsckResult, FsckStats, RepairReport};
use super::ofs::{OfsFilesystem, SECTOR};

/// Stop after this many entries; a cyclic `next_block` would otherwise spin.
const MAX_ENTRIES: usize = 1_000_000;

fn issue(code: &str, message: String, repairable: bool) -> FsckIssue {
    FsckIssue {
        code: code.to_string(),
        message,
        repairable,
        debug: false,
    }
}

/// What the walk found.
struct Survey {
    claimed: HashSet<u64>,
    files: u32,
    directories: u32,
    errors: Vec<FsckIssue>,
    warnings: Vec<FsckIssue>,
}

impl<R: Read + Seek + Send> OfsFilesystem<R> {
    pub fn fsck_ofs(&mut self) -> Result<FsckResult, FilesystemError> {
        let mut survey = self.survey()?;
        let bitmap = self.bitmap_snapshot()?;

        let mut leaked = 0u64;
        let mut unmarked = 0u64;
        let reserved = self.reserved_sectors();
        for sector in 0..self.toc.total_sectors as u64 {
            let marked = bit_is_set(&bitmap, sector);
            let claimed = sector < reserved || survey.claimed.contains(&sector);
            if claimed && !marked {
                unmarked += 1;
            } else if !claimed && marked {
                leaked += 1;
            }
        }
        if unmarked > 0 {
            survey.errors.push(issue(
                "SectorNotMarked",
                format!(
                    "{unmarked} sector(s) the directory tree owns are marked free; the next \
                     file written would overwrite live data"
                ),
                false,
            ));
        }
        if leaked > 0 {
            survey.errors.push(issue(
                "LeakedSectors",
                format!("{leaked} sector(s) are marked used but no entry claims them"),
                true,
            ));
        }

        let counted = self.bitmap_population(&bitmap);
        if counted != self.toc.used_sectors as u64 {
            survey.errors.push(issue(
                "UsedSectorCount",
                format!(
                    "the table of contents says {} used sectors; the bitmap has {counted} set",
                    self.toc.used_sectors
                ),
                true,
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
                    (
                        "OFS version".into(),
                        format!("{}.{}", self.toc.major, self.toc.minor),
                    ),
                    ("Total sectors".into(), self.toc.total_sectors.to_string()),
                    ("Sectors in use".into(), counted.to_string()),
                ],
            },
            orphaned_entries: Vec::new(),
        })
    }

    /// Sectors the table of contents and the bitmap always own.
    fn reserved_sectors(&self) -> u64 {
        self.toc.bitmap_start as u64 + self.toc.bitmap_sectors as u64
    }

    fn bitmap_snapshot(&mut self) -> Result<Vec<u8>, FilesystemError> {
        let (start, count) = (self.toc.bitmap_start as u64, self.toc.bitmap_sectors as u64);
        self.read_sectors(start, count)
    }

    /// Population count, capped at the volume's sector count so trailing bits
    /// in the last bitmap byte cannot inflate it.
    fn bitmap_population(&self, bitmap: &[u8]) -> u64 {
        (0..self.toc.total_sectors as u64)
            .filter(|s| bit_is_set(bitmap, *s))
            .count() as u64
    }

    fn survey(&mut self) -> Result<Survey, FilesystemError> {
        let mut survey = Survey {
            claimed: HashSet::new(),
            files: 0,
            directories: 0,
            errors: Vec::new(),
            warnings: Vec::new(),
        };

        let block_sectors = self.toc.block_sectors();
        let total = self.toc.total_sectors as u64;
        let mut queue: Vec<(u64, String)> = vec![(self.toc.first_dir_sector as u64, "/".into())];
        let mut seen_blocks: HashSet<u64> = HashSet::new();
        let mut entries = 0usize;

        while let Some((start, path)) = queue.pop() {
            survey.directories += 1;
            let mut block = start;
            while block != 0 {
                if !seen_blocks.insert(block) {
                    survey.errors.push(issue(
                        "DirectoryChainLoop",
                        format!("{path}: block {block} appears twice in the directory chain"),
                        false,
                    ));
                    break;
                }
                if block + block_sectors > total {
                    survey.errors.push(issue(
                        "DirectoryBlockPastEnd",
                        format!("{path}: directory block {block} runs past the volume"),
                        false,
                    ));
                    break;
                }
                for s in block..block + block_sectors {
                    if !survey.claimed.insert(s) {
                        survey.errors.push(issue(
                            "CrossLinkedSector",
                            format!("{path}: sector {s} is claimed twice"),
                            false,
                        ));
                    }
                }
                let raw = self.read_sectors(block, block_sectors)?;
                let next = u32::from_be_bytes([raw[0], raw[1], raw[2], raw[3]]) as u64;

                for e in self.entries_in_block(&raw, block) {
                    entries += 1;
                    if entries > MAX_ENTRIES {
                        survey.errors.push(issue(
                            "TooManyEntries",
                            format!("stopped after {MAX_ENTRIES} entries; the tree may loop"),
                            false,
                        ));
                        return Ok(survey);
                    }
                    let sep = if path == "/" { "" } else { "/" };
                    let child_path = format!("{path}{sep}{}", e.name);
                    if e.attrs.is_directory() {
                        queue.push((e.attrs.first_alloc_list as u64, child_path));
                        continue;
                    }
                    survey.files += 1;
                    self.claim_file(&e.attrs, &child_path, &mut survey);
                }
                block = next;
            }
        }
        Ok(survey)
    }

    /// Decode the live entries in one already-read directory block.
    fn entries_in_block(&self, raw: &[u8], block: u64) -> Vec<super::ofs::OfsEntry> {
        (0..super::ofs::ENTRIES_PER_BLOCK)
            .filter_map(|i| self.entry_in_block(raw, block, i))
            .collect()
    }

    fn claim_file(&mut self, attrs: &super::ofs::OfsAttrs, path: &str, survey: &mut Survey) {
        let total = self.toc.total_sectors as u64;
        if !attrs.is_contiguous() && attrs.first_alloc_list != 0 {
            // The extent-list sector is allocated in its own right.
            let list = attrs.first_alloc_list as u64;
            if list >= total {
                survey.errors.push(issue(
                    "ExtentListPastEnd",
                    format!("{path}: extent list at sector {list} is past the volume"),
                    false,
                ));
                return;
            }
            survey.claimed.insert(list);
        }
        let extents = match self.file_extents(attrs) {
            Ok(e) => e,
            Err(err) => {
                survey.errors.push(issue(
                    "UnreadableExtents",
                    format!("{path}: extent list could not be read ({err})"),
                    false,
                ));
                return;
            }
        };
        let mut owned = 0u64;
        for (start, count) in extents {
            if start + count > total {
                survey.errors.push(issue(
                    "ExtentPastEnd",
                    format!(
                        "{path}: an extent runs to sector {}, past the volume",
                        start + count
                    ),
                    false,
                ));
                continue;
            }
            for s in start..start + count {
                if !survey.claimed.insert(s) {
                    survey.errors.push(issue(
                        "CrossLinkedSector",
                        format!("{path}: sector {s} is claimed twice"),
                        false,
                    ));
                }
            }
            owned += count;
        }
        let need = (attrs.logical_size as u64).div_ceil(SECTOR);
        if owned < need {
            survey.errors.push(issue(
                "FileTooShort",
                format!(
                    "{path}: size claims {} bytes but only {owned} sector(s) are allocated",
                    attrs.logical_size
                ),
                false,
            ));
        }
    }
}

impl<R: Read + Write + Seek + Send> OfsFilesystem<R> {
    /// Clear bits for sectors nothing claims, then resync `used_sectors`.
    pub fn repair_ofs(&mut self) -> Result<RepairReport, FilesystemError> {
        let mut report = RepairReport {
            fixes_applied: Vec::new(),
            fixes_failed: Vec::new(),
            unrepairable_count: 0,
        };
        let survey = self.survey()?;
        report.unrepairable_count = survey.errors.iter().filter(|e| !e.repairable).count();
        if report.unrepairable_count > 0 {
            report.fixes_failed.push(
                "structural errors are present; the bitmap was left untouched so the original \
                 evidence survives"
                    .into(),
            );
            return Ok(report);
        }

        let mut bitmap = self.bitmap_snapshot()?;
        let reserved = self.reserved_sectors();
        let mut cleared = 0u64;
        for sector in 0..self.toc.total_sectors as u64 {
            let claimed = sector < reserved || survey.claimed.contains(&sector);
            if !claimed && bit_is_set(&bitmap, sector) {
                bitmap[(sector / 8) as usize] &= !(1 << (sector % 8));
                cleared += 1;
            }
        }
        if cleared > 0 {
            let start = self.toc.bitmap_start as u64;
            self.write_bitmap_at(start, &bitmap)?;
            report
                .fixes_applied
                .push(format!("freed {cleared} leaked sector(s) in the bitmap"));
        }

        let counted = self.bitmap_population(&bitmap);
        if counted != self.toc.used_sectors as u64 {
            report.fixes_applied.push(format!(
                "corrected used_sectors from {} to {counted}",
                self.toc.used_sectors
            ));
            self.toc.used_sectors = counted as u32;
            self.toc_dirty = true;
            self.sync_toc()?;
        }
        Ok(report)
    }
}

fn bit_is_set(bitmap: &[u8], sector: u64) -> bool {
    let byte = (sector / 8) as usize;
    byte < bitmap.len() && bitmap[byte] & (1 << (sector % 8)) != 0
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::fs::filesystem::{CreateFileOptions, EditableFilesystem, Filesystem};
    use crate::fs::ofs_write::create_blank_ofs;
    use std::io::Cursor;

    fn volume() -> OfsFilesystem<Cursor<Vec<u8>>> {
        OfsFilesystem::open(
            Cursor::new(create_blank_ofs(4 * 1024 * 1024, "check").expect("format")),
            0,
        )
        .expect("open")
    }

    #[test]
    fn a_freshly_formatted_volume_is_clean() {
        let mut fs = volume();
        let r = fs.fsck_ofs().unwrap();
        assert!(r.is_clean(), "{:?}", r.errors);
        assert_eq!(r.stats.directories_checked, 1);
        assert_eq!(r.stats.files_checked, 0);
    }

    #[test]
    fn the_volume_stays_clean_across_creates_and_deletes() {
        let mut fs = volume();
        let root = fs.root().unwrap();
        for i in 0..80 {
            let name = format!("f{i:03}.dat");
            let body = vec![(i % 251) as u8; 700 + i * 29];
            fs.create_file(
                &root,
                &name,
                &mut body.as_slice(),
                body.len() as u64,
                &CreateFileOptions::default(),
            )
            .unwrap();
        }
        let r = fs.fsck_ofs().unwrap();
        assert!(r.is_clean(), "after creates: {:?}", r.errors);
        assert_eq!(r.stats.files_checked, 80);

        let listed = fs.list_directory(&root).unwrap();
        for e in listed.iter().take(40) {
            fs.delete_entry(&root, e).unwrap();
        }
        let r = fs.fsck_ofs().unwrap();
        assert!(r.is_clean(), "after deletes: {:?}", r.errors);
        assert_eq!(r.stats.files_checked, 40);
    }

    #[test]
    fn a_leaked_sector_is_found_and_freed() {
        let mut fs = volume();
        let mut bitmap = fs.bitmap_snapshot().unwrap();
        let victim = 5000u64;
        bitmap[(victim / 8) as usize] |= 1 << (victim % 8);
        let start = fs.toc.bitmap_start as u64;
        fs.write_bitmap_at(start, &bitmap).unwrap();

        let r = fs.fsck_ofs().unwrap();
        assert!(r.errors.iter().any(|e| e.code == "LeakedSectors"));
        let report = fs.repair_ofs().unwrap();
        assert!(!report.fixes_applied.is_empty());
        assert!(fs.fsck_ofs().unwrap().is_clean());
    }

    #[test]
    fn a_wrong_used_sector_count_is_corrected() {
        let mut fs = volume();
        fs.toc.used_sectors += 77;
        fs.toc_dirty = true;
        fs.sync_toc().unwrap();
        let r = fs.fsck_ofs().unwrap();
        assert!(r.errors.iter().any(|e| e.code == "UsedSectorCount"));
        fs.repair_ofs().unwrap();
        assert!(fs.fsck_ofs().unwrap().is_clean());
    }

    /// Subdirectories are chains of their own; their blocks have to be claimed
    /// or every `mkdir` would read as a leak.
    #[test]
    fn nested_directories_are_accounted_for() {
        let mut fs = volume();
        let root = fs.root().unwrap();
        let a = fs
            .create_directory(&root, "a", &Default::default())
            .unwrap();
        let b = fs.create_directory(&a, "b", &Default::default()).unwrap();
        fs.create_file(
            &b,
            "deep",
            &mut b"payload".as_slice(),
            7,
            &CreateFileOptions::default(),
        )
        .unwrap();
        let r = fs.fsck_ofs().unwrap();
        assert!(r.is_clean(), "{:?}", r.errors);
        assert_eq!(r.stats.directories_checked, 3);
        assert_eq!(r.stats.files_checked, 1);
    }
}
