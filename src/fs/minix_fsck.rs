//! Minix filesystem consistency check + repair (fsck).
//!
//! Follows the CBM/EFS "VALIDATE" model: walk the directory tree from the root
//! inode, recompute what the inode + zone bitmaps *should* say, and diff that
//! against what's on disk. The recomputable structures (both bitmaps, link
//! counts) are repaired; orphaned inodes are adopted into `/lost+found`.
//! Structural damage that isn't safely recomputable (dangling directory
//! entries pointing at freed inodes) is surfaced read-only.
//!
//! Every repair is verified against the real `fsck.minix` in the tests.

use std::collections::{HashMap, HashSet};
use std::io::{Read, Seek, Write};

use super::filesystem::{CreateDirectoryOptions, EditableFilesystem, Filesystem, FilesystemError};
use super::fsck::{FsckIssue, FsckResult, FsckStats, OrphanedEntry, RepairReport};
use super::minix::{MinixFilesystem, MinixInode};

const ROOT_INO: u32 = 1;

/// What a walk of the directory tree from root tells us.
struct TreeWalk {
    /// Inodes reachable from the root.
    reachable: HashSet<u32>,
    /// Name references (dirents excluding `.`/`..`) per inode.
    ref_count: HashMap<u32, u32>,
    /// Subdirectory count per directory inode.
    subdir_count: HashMap<u32, u32>,
    /// Data + indirect zones referenced by reachable inodes.
    zones_used: HashSet<u32>,
    /// Dirents pointing at an inode whose mode is 0 (freed) — corruption.
    dangling: Vec<(u32, String)>,
    files: u32,
    dirs: u32,
}

fn walk_tree<R: Read + Seek + Send>(
    fs: &mut MinixFilesystem<R>,
) -> Result<TreeWalk, FilesystemError> {
    let stride = fs.superblock().dir_entry_size;
    let ino_field = fs.ino_field();
    let ninodes = fs.superblock().ninodes;

    let mut w = TreeWalk {
        reachable: HashSet::new(),
        ref_count: HashMap::new(),
        subdir_count: HashMap::new(),
        zones_used: HashSet::new(),
        dangling: Vec::new(),
        files: 0,
        dirs: 0,
    };

    let root = fs.read_inode(ROOT_INO)?;
    if !root.is_dir() {
        return Err(FilesystemError::InvalidData(
            "Minix root inode is not a directory".into(),
        ));
    }
    w.reachable.insert(ROOT_INO);
    w.dirs += 1;
    for z in fs.inode_all_zones(&root)? {
        w.zones_used.insert(z);
    }

    let mut stack = vec![ROOT_INO];
    while let Some(dir_ino) = stack.pop() {
        let inode = fs.read_inode(dir_ino)?;
        let data = fs.read_inode_data(&inode, inode.size as usize)?;
        for chunk in data.chunks(stride) {
            if chunk.len() < stride {
                break;
            }
            let child = if ino_field == 4 {
                u32::from_le_bytes([chunk[0], chunk[1], chunk[2], chunk[3]])
            } else {
                u16::from_le_bytes([chunk[0], chunk[1]]) as u32
            };
            if child == 0 || child > ninodes {
                continue;
            }
            let nb = &chunk[ino_field..stride];
            let end = nb.iter().position(|&b| b == 0).unwrap_or(nb.len());
            let name = String::from_utf8_lossy(&nb[..end]).into_owned();
            if name == "." || name == ".." || name.is_empty() {
                continue;
            }
            *w.ref_count.entry(child).or_default() += 1;

            let child_inode = fs.read_inode(child)?;
            if child_inode.mode == 0 {
                w.dangling.push((child, name));
                continue;
            }
            if child_inode.is_dir() {
                *w.subdir_count.entry(dir_ino).or_default() += 1;
            }
            if w.reachable.insert(child) {
                for z in fs.inode_all_zones(&child_inode)? {
                    w.zones_used.insert(z);
                }
                if child_inode.is_dir() {
                    w.dirs += 1;
                    stack.push(child);
                } else {
                    w.files += 1;
                }
            }
        }
    }
    Ok(w)
}

/// Expected on-disk link count for a reachable inode.
fn expected_nlink(inode: &MinixInode, ino: u32, w: &TreeWalk) -> u16 {
    if inode.is_dir() {
        // "." + the entry in the parent + one per subdirectory's "..".
        2 + *w.subdir_count.get(&ino).unwrap_or(&0) as u16
    } else {
        *w.ref_count.get(&ino).unwrap_or(&0) as u16
    }
}

/// Allocated-but-unreachable inodes with real content (mode != 0).
fn find_orphans<R: Read + Seek + Send>(
    fs: &mut MinixFilesystem<R>,
    w: &TreeWalk,
) -> Result<Vec<u32>, FilesystemError> {
    let ninodes = fs.superblock().ninodes;
    let imap = fs.read_inode_bitmap()?;
    let mut orphans = Vec::new();
    for ino in 1..=ninodes {
        if w.reachable.contains(&ino) {
            continue;
        }
        let allocated = bit_at(&imap, ino as usize);
        let inode = fs.read_inode(ino)?;
        if allocated && inode.mode != 0 {
            orphans.push(ino);
        }
    }
    Ok(orphans)
}

fn bit_at(bitmap: &[u8], bit: usize) -> bool {
    let byte = bit / 8;
    byte < bitmap.len() && (bitmap[byte] >> (bit % 8)) & 1 == 1
}

pub fn fsck_minix<R: Read + Seek + Send>(
    fs: &mut MinixFilesystem<R>,
) -> Result<FsckResult, FilesystemError> {
    let sb = fs.superblock();
    let (ninodes, zones, firstdatazone) = (sb.ninodes, sb.zones, sb.firstdatazone);

    let walk = walk_tree(fs)?;
    let imap = fs.read_inode_bitmap()?;
    let zmap = fs.read_zone_bitmap()?;

    let mut errors: Vec<FsckIssue> = Vec::new();
    let mut warnings: Vec<FsckIssue> = Vec::new();
    let mut orphaned_entries: Vec<OrphanedEntry> = Vec::new();

    // Include orphan inodes' zones in the "used" set so bitmap reconciliation
    // doesn't flag them as leaked before adoption.
    let orphans = find_orphans(fs, &walk)?;
    let mut zones_used = walk.zones_used.clone();
    for &ino in &orphans {
        let inode = fs.read_inode(ino)?;
        for z in fs.inode_all_zones(&inode)? {
            zones_used.insert(z);
        }
        orphaned_entries.push(OrphanedEntry {
            id: ino as u64,
            name: format!("inode_{ino}"),
            is_directory: inode.is_dir(),
            missing_parent_id: 0,
        });
        warnings.push(FsckIssue {
            code: "OrphanInode".into(),
            message: format!(
                "inode {ino} is allocated but unreachable from the root (adopt into /lost+found)"
            ),
            repairable: true,
            debug: false,
        });
    }

    // Dangling directory entries (point at a freed inode) — not safely
    // recomputable, surfaced read-only.
    for (ino, name) in &walk.dangling {
        errors.push(FsckIssue {
            code: "DanglingDirent".into(),
            message: format!("directory entry '{name}' points at freed inode {ino}"),
            repairable: false,
            debug: false,
        });
    }

    // Inode bitmap reconciliation.
    for ino in 1..=ninodes {
        let allocated = bit_at(&imap, ino as usize);
        let reachable = walk.reachable.contains(&ino);
        if reachable && !allocated {
            errors.push(FsckIssue {
                code: "InodeMarkedFree".into(),
                message: format!("inode {ino} is in use but marked free in the inode bitmap"),
                repairable: true,
                debug: false,
            });
        } else if allocated && !reachable && !orphans.contains(&ino) {
            // Allocated, unreachable, and empty (mode 0) — a leaked bit.
            warnings.push(FsckIssue {
                code: "InodeLeaked".into(),
                message: format!("inode {ino} is marked used but empty and unreachable"),
                repairable: true,
                debug: false,
            });
        }
    }

    // Link-count check for reachable inodes.
    for &ino in &walk.reachable {
        let inode = fs.read_inode(ino)?;
        let expected = expected_nlink(&inode, ino, &walk);
        if inode.nlinks != expected {
            errors.push(FsckIssue {
                code: "LinkCount".into(),
                message: format!(
                    "inode {ino} link count is {} but should be {expected}",
                    inode.nlinks
                ),
                repairable: true,
                debug: false,
            });
        }
    }

    // Zone bitmap reconciliation over the real data-zone range.
    for zone in firstdatazone..zones {
        let bit = (zone - firstdatazone + 1) as usize;
        let marked = bit_at(&zmap, bit);
        let referenced = zones_used.contains(&zone);
        if referenced && !marked {
            errors.push(FsckIssue {
                code: "ZoneMarkedFree".into(),
                message: format!("zone {zone} is in use but marked free in the zone bitmap"),
                repairable: true,
                debug: false,
            });
        } else if marked && !referenced {
            warnings.push(FsckIssue {
                code: "ZoneLeaked".into(),
                message: format!("zone {zone} is marked used but referenced by no inode"),
                repairable: true,
                debug: false,
            });
        }
    }

    let repairable = errors.iter().any(|e| e.repairable) || !warnings.is_empty();
    Ok(FsckResult {
        stats: FsckStats {
            files_checked: walk.files,
            directories_checked: walk.dirs,
            extra: vec![
                ("inodes".into(), ninodes.to_string()),
                ("zones".into(), zones.to_string()),
            ],
        },
        repairable,
        errors,
        warnings,
        orphaned_entries,
    })
}

pub fn repair_minix<R: Read + Write + Seek + Send>(
    fs: &mut MinixFilesystem<R>,
) -> Result<RepairReport, FilesystemError> {
    let mut report = RepairReport {
        fixes_applied: Vec::new(),
        fixes_failed: Vec::new(),
        unrepairable_count: 0,
    };

    // 1. Adopt orphans into /lost+found so the later re-walk sees them.
    let walk = walk_tree(fs)?;
    report.unrepairable_count += walk.dangling.len();
    let orphans = find_orphans(fs, &walk)?;
    if !orphans.is_empty() {
        let root_entry = fs.root()?;
        let root_inode = fs.read_inode(ROOT_INO)?;
        let lf_ino = match fs.dir_find(&root_inode, b"lost+found")? {
            Some(i) => i,
            None => {
                let e = fs.create_directory(
                    &root_entry,
                    "lost+found",
                    &CreateDirectoryOptions::default(),
                )?;
                report.fixes_applied.push("created /lost+found".to_string());
                e.location as u32
            }
        };
        for ino in orphans {
            let name = format!("inode_{ino}");
            let mut lf_inode = fs.read_inode(lf_ino)?;
            fs.dir_add(&mut lf_inode, name.as_bytes(), ino)?;
            fs.write_inode(&lf_inode)?;

            let orphan = fs.read_inode(ino)?;
            if orphan.is_dir() {
                set_dotdot(fs, &orphan, lf_ino)?;
                let mut lf2 = fs.read_inode(lf_ino)?;
                lf2.nlinks = lf2.nlinks.saturating_add(1);
                fs.write_inode(&lf2)?;
            }
            report.fixes_applied.push(format!(
                "adopted orphan inode {ino} into /lost+found/{name}"
            ));
        }
    }

    // 2. Re-walk (orphans now reachable) and rebuild the recomputable state.
    let walk = walk_tree(fs)?;
    let sb = fs.superblock();
    let (ninodes, zones, firstdatazone, block_size, imap_blocks, zmap_blocks) = (
        sb.ninodes,
        sb.zones,
        sb.firstdatazone,
        sb.block_size as u64,
        sb.imap_blocks as u64,
        sb.zmap_blocks as u64,
    );
    let bpb = block_size * 8;

    // Rebuild the inode bitmap from actual inode occupancy (mode != 0).
    let mut new_imap = vec![0u8; (imap_blocks * block_size) as usize];
    new_imap[0] |= 1; // sentinel
    let mut inode_fixes = 0u32;
    for ino in 1..=ninodes {
        if fs.read_inode(ino)?.mode != 0 {
            new_imap[ino as usize / 8] |= 1 << (ino as usize % 8);
        }
    }
    set_padding(
        &mut new_imap,
        (ninodes + 1) as usize,
        (imap_blocks * bpb) as usize,
    );
    let old_imap = fs.read_inode_bitmap()?;
    if old_imap != new_imap {
        let start = fs.imap_start();
        fs.write_at(start, &new_imap)?;
        inode_fixes += 1;
    }

    // Rebuild the zone bitmap from referenced zones.
    let mut new_zmap = vec![0u8; (zmap_blocks * block_size) as usize];
    new_zmap[0] |= 1; // sentinel
    for &zone in &walk.zones_used {
        if zone >= firstdatazone && zone < zones {
            let bit = (zone - firstdatazone + 1) as usize;
            new_zmap[bit / 8] |= 1 << (bit % 8);
        }
    }
    set_padding(
        &mut new_zmap,
        (zones - firstdatazone + 1) as usize,
        (zmap_blocks * bpb) as usize,
    );
    let old_zmap = fs.read_zone_bitmap()?;
    if old_zmap != new_zmap {
        let start = fs.zmap_start();
        fs.write_at(start, &new_zmap)?;
        report
            .fixes_applied
            .push("rebuilt the zone bitmap".to_string());
    }
    if inode_fixes > 0 {
        report
            .fixes_applied
            .push("rebuilt the inode bitmap".to_string());
    }

    // Fix link counts.
    let reachable: Vec<u32> = walk.reachable.iter().copied().collect();
    for ino in reachable {
        let mut inode = fs.read_inode(ino)?;
        let expected = expected_nlink(&inode, ino, &walk);
        if inode.nlinks != expected {
            inode.nlinks = expected;
            fs.write_inode(&inode)?;
            report
                .fixes_applied
                .push(format!("set inode {ino} link count to {expected}"));
        }
    }

    fs.sync_metadata()?;
    Ok(report)
}

/// Set bitmap padding bits `[first, end)` (inodes/zones that don't exist).
fn set_padding(bitmap: &mut [u8], first: usize, end: usize) {
    for bit in first..end {
        bitmap[bit / 8] |= 1 << (bit % 8);
    }
}

/// Rewrite a directory's `..` entry to point at `new_parent`.
fn set_dotdot<R: Read + Write + Seek + Send>(
    fs: &mut MinixFilesystem<R>,
    dir: &MinixInode,
    new_parent: u32,
) -> Result<(), FilesystemError> {
    let stride = fs.superblock().dir_entry_size;
    let ino_field = fs.ino_field();
    let zs = fs.superblock().zone_size() as usize;
    let data = fs.read_inode_data(dir, dir.size as usize)?;
    let mut off = 0;
    while off + stride <= data.len() {
        let nb = &data[off + ino_field..off + stride];
        let end = nb.iter().position(|&b| b == 0).unwrap_or(nb.len());
        if &nb[..end] == b".." {
            let phys = dir.zones[off / zs];
            let mut field = vec![0u8; ino_field];
            if ino_field == 4 {
                field.copy_from_slice(&new_parent.to_le_bytes());
            } else {
                field.copy_from_slice(&(new_parent as u16).to_le_bytes());
            }
            fs.write_at(phys as u64 * zs as u64 + (off % zs) as u64, &field)?;
            return Ok(());
        }
        off += stride;
    }
    Ok(())
}
