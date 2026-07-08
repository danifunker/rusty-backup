//! FAT12/16/32 integrity check (fsck) and repair.
//!
//! Promotes the old header-only `validate_fat_integrity` gate to a real
//! allocation-level check + repair, mirroring the shape of `ufs_fsck` /
//! `efs_fsck`: a single [`analyze`] pass produces the [`FsckResult`] the
//! read-only [`fsck_fat`] reports and the concrete fix list the writable
//! [`repair_fat`] applies.
//!
//! ## What it checks
//!
//! The FAT is walked in memory (one read of the primary copy) after the
//! directory tree is enumerated through the public [`Filesystem`] interface,
//! so every file and directory contributes its cluster chain to an ownership
//! map. From that we surface:
//!
//! - **FAT[0] identifier** disagreeing with the BPB media byte (warning).
//! - **FAT[1] "dirty"/hard-error flags** on FAT16/32 (warning — the volume was
//!   not cleanly unmounted; not corruption).
//! - **Chain faults** — loops, links into a free / bad / reserved / out-of-range
//!   cluster, and a cluster count longer than the directory entry's size.
//! - **Cross-linked clusters** — one cluster claimed by two chains.
//! - **Size vs. chain** disagreements a chain edit can't safely resolve.
//! - **Lost cluster chains** — allocated clusters no chain references.
//! - **FAT mirror mismatch** — a secondary FAT differing from the primary.
//!
//! ## What it repairs (FAT-only, non-destructive)
//!
//! Repairs touch only the FAT table, never directory entries, so a chain is
//! only ever shortened or reclaimed — file data already past the truncation
//! point is what the size field already said was not there:
//!
//! - truncate a chain at the last good cluster (loop / bad link / over-long);
//! - free lost cluster chains;
//! - rewrite a wrong FAT[0] identifier;
//! - resynchronise mirror FATs from the primary.
//!
//! Faults that need a directory-entry rewrite to fix safely (cross-links, a
//! size that exceeds the chain, a first-cluster pointer into free/bad space)
//! are surfaced for diagnosis but left for the editor — the same policy
//! `ufs_fsck` uses for double-allocations.

use std::collections::{HashMap, HashSet, VecDeque};
use std::io::{Read, Seek, Write};

use super::entry::FileEntry;
use super::fat::{
    end_of_chain_marker, is_bad_cluster, is_end_of_chain, read_fat_entry, FatFilesystem, FatGeom,
    FatType,
};
use super::filesystem::{Filesystem, FilesystemError};
use super::fsck::{FsckIssue, FsckResult, FsckStats};

/// Interpretation of a raw FAT entry value for a given geometry.
enum Class {
    /// Zero — the cluster is not allocated.
    Free,
    /// A valid forward pointer to another data cluster.
    Next(u32),
    /// A value in the reserved band just below the end-of-chain markers.
    Reserved,
    /// The bad-cluster marker.
    Bad,
    /// An end-of-chain marker.
    Eoc,
    /// A forward pointer past the last valid data cluster.
    OutOfRange,
}

fn classify(v: u32, g: &FatGeom) -> Class {
    if v == 0 {
        return Class::Free;
    }
    if is_end_of_chain(v, g.fat_type) {
        return Class::Eoc;
    }
    if is_bad_cluster(v, g.fat_type) {
        return Class::Bad;
    }
    // The reserved band (0x?FF0..0x?FF6) sits between the largest usable cluster
    // number and the bad/EOC markers; the FAT spec forbids using it as a cluster
    // number, and the driver's own `next_cluster` treats it as a terminator.
    let reserved_lo = match g.fat_type {
        FatType::Fat12 => 0x0FF0,
        FatType::Fat16 => 0xFFF0,
        FatType::Fat32 => 0x0FFF_FFF0,
    };
    if v >= reserved_lo {
        return Class::Reserved;
    }
    if (2..=g.max_data_cluster).contains(&v) {
        Class::Next(v)
    } else {
        Class::OutOfRange
    }
}

/// A concrete repair operation. Only FAT-table edits appear here.
enum Fix {
    /// Set `cluster`'s FAT entry to the end-of-chain marker (truncate here).
    SetEoc { cluster: u32 },
    /// Set an explicit FAT entry value (FAT[0] identifier rewrite).
    SetEntry { cluster: u32, value: u32 },
    /// Free every listed cluster (set its FAT entry to 0).
    FreeClusters { clusters: Vec<u32> },
    /// Copy the primary FAT over every mirror copy.
    MirrorResync,
}

/// Result of the shared analysis pass consumed by both `fsck_fat` and
/// `repair_fat`.
struct Analysis {
    geom: FatGeom,
    errors: Vec<FsckIssue>,
    warnings: Vec<FsckIssue>,
    /// `(human description, operation)` pairs, applied in order by repair.
    fixes: Vec<(String, Fix)>,
    /// Count of surfaced errors that repair cannot safely fix on its own.
    unrepairable: usize,
    files_checked: u32,
    dirs_checked: u32,
    clusters_free: u64,
    clusters_used: u64,
    clusters_bad: u64,
    lost_clusters: u64,
    lost_chains: u64,
}

impl Analysis {
    fn err(&mut self, code: &str, message: String, repairable: bool) {
        if !repairable {
            self.unrepairable += 1;
        }
        self.errors.push(FsckIssue {
            code: code.into(),
            message,
            repairable,
            debug: false,
        });
    }

    fn warn(&mut self, code: &str, message: String) {
        self.warnings.push(FsckIssue {
            code: code.into(),
            message,
            repairable: false,
            debug: false,
        });
    }

    /// Record a repairable error together with the fix that resolves it.
    fn repairable(&mut self, code: &str, message: String, fix: Fix) {
        self.errors.push(FsckIssue {
            code: code.into(),
            message: message.clone(),
            repairable: true,
            debug: false,
        });
        self.fixes.push((format!("{code}: {message}"), fix));
    }
}

/// A file or directory to trace, discovered during the directory walk.
struct EntryRef {
    label: String,
    first: u32,
    size: u64,
    is_dir: bool,
}

/// Enumerate every file/directory in the tree through the public
/// [`Filesystem`] interface. Returns the traceable entries plus file/dir
/// counts. Directory-read failures are recorded as warnings and skipped so a
/// single corrupt directory can't abort the whole check.
fn enumerate_entries<R: Read + Seek + Send>(
    fs: &mut FatFilesystem<R>,
    g: &FatGeom,
    a: &mut Analysis,
) -> Vec<EntryRef> {
    let mut entries: Vec<EntryRef> = Vec::new();

    // On FAT32 the root directory is itself a cluster chain (no fixed region),
    // so it owns clusters and must be traced like any other directory.
    if g.fat_type == FatType::Fat32 {
        entries.push(EntryRef {
            label: "/".into(),
            first: g.root_cluster,
            size: 0,
            is_dir: true,
        });
    }

    let root = match fs.root() {
        Ok(r) => r,
        Err(e) => {
            a.warn("RootReadFailed", format!("cannot read root directory: {e}"));
            return entries;
        }
    };

    let mut queue: VecDeque<FileEntry> = VecDeque::new();
    queue.push_back(root);
    // Track visited directory start clusters to bound recursion and avoid
    // double-claiming a directory reachable by more than one entry.
    let mut visited_dirs: HashSet<u64> = HashSet::new();

    while let Some(dir) = queue.pop_front() {
        a.dirs_checked += 1;
        let children = match fs.list_directory(&dir) {
            Ok(c) => c,
            Err(e) => {
                a.warn(
                    "DirReadFailed",
                    format!("cannot read directory {}: {e}", dir.path),
                );
                continue;
            }
        };
        for ch in children {
            if ch.is_directory() {
                let loc = ch.location;
                if loc < 2 {
                    a.err(
                        "InvalidDirCluster",
                        format!("directory {} has invalid start cluster {loc}", ch.path),
                        false,
                    );
                    continue;
                }
                // Only trace / recurse a directory cluster once.
                if visited_dirs.insert(loc) {
                    entries.push(EntryRef {
                        label: ch.path.clone(),
                        first: loc as u32,
                        size: 0,
                        is_dir: true,
                    });
                    queue.push_back(ch);
                }
            } else {
                a.files_checked += 1;
                entries.push(EntryRef {
                    label: ch.path.clone(),
                    first: ch.location as u32,
                    size: ch.size,
                    is_dir: false,
                });
            }
        }
    }

    entries
}

/// Trace one entry's cluster chain against the in-memory FAT, claiming each
/// cluster into `owner`. Records chain faults into `a` and returns the ordered
/// list of clusters the chain owns (only the clean-claimed ones).
fn trace_chain(
    e: &EntryRef,
    idx: usize,
    fat: &[u8],
    g: &FatGeom,
    owner: &mut HashMap<u32, usize>,
    entries: &[EntryRef],
    a: &mut Analysis,
) -> Vec<u32> {
    let ft = g.fat_type;
    let mut chain: Vec<u32> = Vec::new();

    // An empty file/dir with no first cluster.
    if e.first == 0 {
        if !e.is_dir && e.size > 0 {
            a.err(
                "ZeroClusterNonEmpty",
                format!(
                    "{}: size is {} bytes but the entry has no start cluster",
                    e.label, e.size
                ),
                false,
            );
        }
        return chain;
    }

    let mut cluster = e.first;
    let mut prev: Option<u32> = None;
    // Hard cap: no valid chain can be longer than the cluster count.
    let cap = g.total_clusters as usize + 2;

    loop {
        // Position validity.
        if !(2..=g.max_data_cluster).contains(&cluster) {
            match prev {
                None => a.err(
                    "InvalidStartCluster",
                    format!("{}: start cluster {cluster} is out of range", e.label),
                    false,
                ),
                Some(p) => a.repairable(
                    "ChainPointerOutOfRange",
                    format!(
                        "{}: cluster {p} links to out-of-range cluster {cluster}",
                        e.label
                    ),
                    Fix::SetEoc { cluster: p },
                ),
            }
            break;
        }

        // Cross-link / loop: someone already owns this cluster.
        if let Some(&j) = owner.get(&cluster) {
            if j == idx {
                // Back-edge into our own chain: a loop. Break it at prev.
                if let Some(p) = prev {
                    a.repairable(
                        "ChainLoop",
                        format!("{}: cluster {p} links back into its own chain", e.label),
                        Fix::SetEoc { cluster: p },
                    );
                }
            } else {
                let other = entries.get(j).map(|o| o.label.as_str()).unwrap_or("?");
                a.err(
                    "CrossLinkedCluster",
                    format!("{}: cluster {cluster} is also claimed by {other}", e.label),
                    false,
                );
            }
            break;
        }

        // The cluster's own allocation status (its FAT entry doubles as the
        // forward link).
        let fc = read_fat_entry(fat, cluster, ft);
        if fc == 0 {
            match prev {
                None => a.err(
                    "StartClusterFree",
                    format!(
                        "{}: start cluster {cluster} is marked free in the FAT",
                        e.label
                    ),
                    false,
                ),
                Some(p) => a.repairable(
                    "ChainIntoFreeCluster",
                    format!("{}: cluster {p} links to free cluster {cluster}", e.label),
                    Fix::SetEoc { cluster: p },
                ),
            }
            break;
        }
        if is_bad_cluster(fc, ft) {
            match prev {
                None => a.err(
                    "StartClusterBad",
                    format!(
                        "{}: start cluster {cluster} is marked bad in the FAT",
                        e.label
                    ),
                    false,
                ),
                Some(p) => a.repairable(
                    "ChainIntoBadCluster",
                    format!("{}: cluster {p} links to bad cluster {cluster}", e.label),
                    Fix::SetEoc { cluster: p },
                ),
            }
            break;
        }

        // Claim it.
        owner.insert(cluster, idx);
        chain.push(cluster);

        // Follow the forward link.
        match classify(fc, g) {
            Class::Eoc => break,
            Class::Next(n) => {
                prev = Some(cluster);
                cluster = n;
            }
            Class::Reserved | Class::OutOfRange => {
                a.repairable(
                    "ChainPointerInvalid",
                    format!(
                        "{}: cluster {cluster} has an invalid forward link (0x{fc:X})",
                        e.label
                    ),
                    Fix::SetEoc { cluster },
                );
                break;
            }
            // Free/Bad handled above before the claim.
            Class::Free | Class::Bad => break,
        }

        if chain.len() > cap {
            // Runaway (should be unreachable given the loop guard); stop safely.
            break;
        }
    }

    // File size vs. chain length.
    if !e.is_dir {
        let expected = e.size.div_ceil(g.cluster_size) as usize;
        let actual = chain.len();
        if actual > expected {
            if expected == 0 {
                a.err(
                    "NonEmptyChainZeroSize",
                    format!(
                        "{}: size is 0 but {actual} cluster(s) are allocated",
                        e.label
                    ),
                    false,
                );
            } else {
                let free: Vec<u32> = chain[expected..].to_vec();
                a.repairable(
                    "ChainLongerThanSize",
                    format!(
                        "{}: {actual} clusters allocated for a {}-byte file ({expected} needed)",
                        e.label, e.size
                    ),
                    Fix::SetEoc {
                        cluster: chain[expected - 1],
                    },
                );
                a.fixes.push((
                    format!(
                        "ChainLongerThanSize: {} freeing {} tail cluster(s)",
                        e.label,
                        free.len()
                    ),
                    Fix::FreeClusters { clusters: free },
                ));
            }
        } else if actual < expected {
            a.err(
                "SizeExceedsChain",
                format!(
                    "{}: size claims {expected} clusters but only {actual} are allocated",
                    e.label
                ),
                false,
            );
        }
    }

    chain
}

/// Check the FAT[0] media identifier and the FAT[1] clean/hard-error flags.
fn check_fat_header(fat: &[u8], media: u8, g: &FatGeom, a: &mut Analysis) {
    let ft = g.fat_type;
    let fat0 = read_fat_entry(fat, 0, ft);
    let expected0 = match ft {
        FatType::Fat12 => 0x0F00 | media as u32,
        FatType::Fat16 => 0xFF00 | media as u32,
        FatType::Fat32 => 0x0FFF_FF00 | media as u32,
    };
    if fat0 != expected0 {
        a.repairable(
            "FatIdMismatch",
            format!(
                "FAT[0] identifier is 0x{fat0:X}, expected 0x{expected0:X} for media 0x{media:02X}"
            ),
            Fix::SetEntry {
                cluster: 0,
                value: expected0,
            },
        );
    }

    // FAT[1] carries volume-dirty / hard-error flags on FAT16/32 only. These
    // are informational (the volume was not cleanly unmounted), never a
    // structural error, so they surface as warnings and are left untouched.
    let fat1 = read_fat_entry(fat, 1, ft);
    match ft {
        FatType::Fat16 => {
            if fat1 & 0x8000 == 0 {
                a.warn(
                    "VolumeDirty",
                    "FAT[1] clean-shutdown bit is clear (volume was not cleanly unmounted)".into(),
                );
            }
            if fat1 & 0x4000 == 0 {
                a.warn(
                    "VolumeIoError",
                    "FAT[1] hard-error bit is clear (volume had I/O errors)".into(),
                );
            }
        }
        FatType::Fat32 => {
            if fat1 & 0x0800_0000 == 0 {
                a.warn(
                    "VolumeDirty",
                    "FAT[1] clean-shutdown bit is clear (volume was not cleanly unmounted)".into(),
                );
            }
            if fat1 & 0x0400_0000 == 0 {
                a.warn(
                    "VolumeIoError",
                    "FAT[1] hard-error bit is clear (volume had I/O errors)".into(),
                );
            }
        }
        FatType::Fat12 => {}
    }
}

/// Scan every data cluster for allocation status and flag lost chains
/// (allocated but referenced by no directory entry). Populates the free/used/
/// bad/lost counters.
fn check_lost_clusters(fat: &[u8], g: &FatGeom, referenced: &HashSet<u32>, a: &mut Analysis) {
    let ft = g.fat_type;
    let mut lost_set: HashSet<u32> = HashSet::new();
    for c in 2..=g.max_data_cluster {
        let v = read_fat_entry(fat, c, ft);
        match classify(v, g) {
            Class::Free => a.clusters_free += 1,
            Class::Bad => a.clusters_bad += 1,
            // Everything else (Next / Eoc / Reserved / OutOfRange) marks the
            // cluster as allocated.
            _ => {
                a.clusters_used += 1;
                if !referenced.contains(&c) {
                    lost_set.insert(c);
                }
            }
        }
    }

    if lost_set.is_empty() {
        return;
    }
    a.lost_clusters = lost_set.len() as u64;

    // Group the lost clusters into chains for a useful message; free them all.
    // A "head" is a lost cluster not pointed at by another lost cluster.
    let pointed_at: HashSet<u32> = lost_set
        .iter()
        .filter_map(|&c| match classify(read_fat_entry(fat, c, ft), g) {
            Class::Next(n) if lost_set.contains(&n) => Some(n),
            _ => None,
        })
        .collect();

    let mut all_lost: Vec<u32> = Vec::with_capacity(lost_set.len());
    let mut chains = 0u64;
    for &head in lost_set.iter() {
        if pointed_at.contains(&head) {
            continue;
        }
        // Walk this lost chain (bounded by the lost set) for its length.
        chains += 1;
        let mut len = 0u64;
        let mut c = head;
        let mut guard = 0usize;
        loop {
            if !lost_set.contains(&c) || guard > lost_set.len() {
                break;
            }
            all_lost.push(c);
            len += 1;
            guard += 1;
            match classify(read_fat_entry(fat, c, ft), g) {
                Class::Next(n) => c = n,
                _ => break,
            }
        }
        a.errors.push(FsckIssue {
            code: "LostClusterChain".into(),
            message: format!("lost cluster chain of {len} cluster(s) starting at cluster {head}"),
            repairable: true,
            debug: false,
        });
    }
    a.lost_chains = chains;

    // Any lost cluster not reached from a head (e.g. part of a lost loop) is
    // still freed so the reclaim is complete.
    for &c in lost_set.iter() {
        if !all_lost.contains(&c) {
            all_lost.push(c);
        }
    }
    a.fixes.push((
        format!(
            "LostClusterChain: freeing {} lost cluster(s)",
            all_lost.len()
        ),
        Fix::FreeClusters { clusters: all_lost },
    ));
}

/// Compare mirror FAT copies against the primary over the meaningful entry
/// range (ignoring any trailing padding past the last cluster's entry).
fn check_fat_mirrors<R: Read + Seek + Send>(
    fs: &mut FatFilesystem<R>,
    primary: &[u8],
    g: &FatGeom,
    a: &mut Analysis,
) {
    if g.num_fats < 2 {
        return;
    }
    // Bytes spanning entries 0..=max_data_cluster+1.
    let entries = g.total_clusters + 2;
    let used = match g.fat_type {
        FatType::Fat12 => (entries * 3).div_ceil(2),
        FatType::Fat16 => entries * 2,
        FatType::Fat32 => entries * 4,
    } as usize;
    let cmp = used.min(primary.len());

    let mut any_mismatch = false;
    for idx in 1..g.num_fats {
        match fs.read_fat_copy(idx) {
            Ok(copy) => {
                let clen = cmp.min(copy.len());
                if clen < cmp || copy[..clen] != primary[..clen] {
                    any_mismatch = true;
                    a.errors.push(FsckIssue {
                        code: "FatMirrorMismatch".into(),
                        message: format!("FAT copy {idx} differs from the primary FAT"),
                        repairable: true,
                        debug: false,
                    });
                }
            }
            Err(e) => a.warn(
                "FatCopyReadFailed",
                format!("cannot read FAT copy {idx}: {e}"),
            ),
        }
    }
    if any_mismatch {
        a.fixes.push((
            "FatMirrorMismatch: resynchronising mirror FAT(s) from the primary".into(),
            Fix::MirrorResync,
        ));
    }
}

/// The shared analysis pass. Read-only; produces both the report and the fix
/// list.
fn analyze<R: Read + Seek + Send>(fs: &mut FatFilesystem<R>) -> Result<Analysis, FilesystemError> {
    let g = fs.fsck_geometry();
    let mut a = Analysis {
        geom: g,
        errors: Vec::new(),
        warnings: Vec::new(),
        fixes: Vec::new(),
        unrepairable: 0,
        files_checked: 0,
        dirs_checked: 0,
        clusters_free: 0,
        clusters_used: 0,
        clusters_bad: 0,
        lost_clusters: 0,
        lost_chains: 0,
    };

    let media = fs.read_bpb()?[21];
    let fat = fs.read_fat_copy(0)?;

    // 0. Geometry: the FAT must be large enough to hold an entry for every
    //    data cluster (entries 0..=max_data_cluster). A FAT sized for fewer
    //    entries than the cluster count leaves the tail clusters unaddressable
    //    — real drivers and `fsck_msdos` both reject this. Not auto-repairable
    //    (it needs a reformat / resize, not a FAT edit).
    let fat_capacity = match g.fat_type {
        FatType::Fat12 => fat.len() as u64 * 2 / 3,
        FatType::Fat16 => fat.len() as u64 / 2,
        FatType::Fat32 => fat.len() as u64 / 4,
    };
    let entries_needed = g.total_clusters + 2;
    if fat_capacity < entries_needed {
        a.err(
            "FatTooSmallForClusters",
            format!(
                "the FAT holds {fat_capacity} entries but the volume declares {} data \
                 clusters ({entries_needed} entries needed) — the FAT is undersized",
                g.total_clusters
            ),
            false,
        );
    }

    // 1. FAT[0]/FAT[1] header.
    check_fat_header(&fat, media, &g, &mut a);

    // 2. Directory tree -> traceable entries.
    let entries = enumerate_entries(fs, &g, &mut a);

    // 3. Chain ownership + per-chain faults.
    let mut owner: HashMap<u32, usize> = HashMap::new();
    for (idx, e) in entries.iter().enumerate() {
        trace_chain(e, idx, &fat, &g, &mut owner, &entries, &mut a);
    }
    let referenced: HashSet<u32> = owner.keys().copied().collect();

    // 4. Lost clusters.
    check_lost_clusters(&fat, &g, &referenced, &mut a);

    // 5. FAT mirror consistency (last, so a repair resync runs after per-cluster
    //    fixes have updated the primary).
    check_fat_mirrors(fs, &fat, &g, &mut a);

    Ok(a)
}

/// Build the aggregate statistics block shared by the report.
fn stats(a: &Analysis) -> FsckStats {
    FsckStats {
        files_checked: a.files_checked,
        directories_checked: a.dirs_checked,
        extra: vec![
            ("fat_type".into(), a.geom.fat_type.name().to_string()),
            ("clusters_total".into(), a.geom.total_clusters.to_string()),
            ("clusters_used".into(), a.clusters_used.to_string()),
            ("clusters_free".into(), a.clusters_free.to_string()),
            ("clusters_bad".into(), a.clusters_bad.to_string()),
            ("lost_clusters".into(), a.lost_clusters.to_string()),
            ("lost_chains".into(), a.lost_chains.to_string()),
        ],
    }
}

/// Run the FAT integrity check (read-only).
pub fn fsck_fat<R: Read + Seek + Send>(
    fs: &mut FatFilesystem<R>,
) -> Result<FsckResult, FilesystemError> {
    let a = analyze(fs)?;
    let repairable = a.errors.iter().any(|e| e.repairable);
    let stats = stats(&a);
    Ok(FsckResult {
        errors: a.errors,
        warnings: a.warnings,
        stats,
        repairable,
        orphaned_entries: Vec::new(),
    })
}

/// Repair the FAT-only faults `analyze` found and re-check nothing (the caller
/// re-runs fsck to confirm). Only mutates the FAT table.
pub fn repair_fat<R: Read + Write + Seek + Send>(
    fs: &mut FatFilesystem<R>,
) -> Result<super::fsck::RepairReport, FilesystemError> {
    let a = analyze(fs)?;
    let eoc = end_of_chain_marker(a.geom.fat_type);
    let num_fats = a.geom.num_fats;

    let mut fixes_applied: Vec<String> = Vec::new();
    let mut fixes_failed: Vec<String> = Vec::new();

    for (desc, fix) in &a.fixes {
        let result = match fix {
            Fix::SetEoc { cluster } => fs.write_fat_entry_disk(*cluster, eoc),
            Fix::SetEntry { cluster, value } => fs.write_fat_entry_disk(*cluster, *value),
            Fix::FreeClusters { clusters } => (|| {
                for &c in clusters {
                    fs.write_fat_entry_disk(c, 0)?;
                }
                Ok(())
            })(),
            Fix::MirrorResync => (|| {
                let primary = fs.read_fat_copy(0)?;
                for k in 1..num_fats {
                    fs.write_fat_copy(k, &primary)?;
                }
                Ok(())
            })(),
        };
        match result {
            Ok(()) => fixes_applied.push(desc.clone()),
            Err(e) => fixes_failed.push(format!("{desc}: {e}")),
        }
    }

    if !fixes_applied.is_empty() {
        fs.flush_fat()?;
    }

    Ok(super::fsck::RepairReport {
        fixes_applied,
        fixes_failed,
        unrepairable_count: a.unrepairable,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::fs::fat::{create_blank_fat, FatFilesystem};
    use crate::fs::filesystem::{CreateFileOptions, EditableFilesystem};
    use std::io::Cursor;

    type Img = Cursor<Vec<u8>>;

    /// Decompress a committed real-world FAT fixture into a writable image.
    /// These are genuine mkfs.fat images (verified `fsck_msdos`-clean), so they
    /// give a strong, independent no-false-positive base for the checker
    /// (generated by a different formatter than our own `create_blank_fat`).
    fn load(name: &str) -> Img {
        let path = format!("tests/fixtures/{name}");
        let compressed = std::fs::read(&path).unwrap_or_else(|e| panic!("read {path}: {e}"));
        let mut dec = zstd::stream::read::Decoder::new(Cursor::new(compressed)).expect("zstd");
        let mut out = Vec::new();
        dec.read_to_end(&mut out).expect("decompress");
        Cursor::new(out)
    }

    fn fat12() -> Img {
        load("test_fat12.img.zst")
    }
    fn fat16() -> Img {
        load("test_fat16.img.zst")
    }

    fn open(img: &mut Img) -> FatFilesystem<&mut Img> {
        FatFilesystem::open(img, 0).expect("open FAT")
    }

    /// Add a file of `size` bytes filled with 0xAB to the root directory.
    fn add_file(img: &mut Img, name: &str, size: usize) {
        let mut fs = open(img);
        let root = fs.root().unwrap();
        let data = vec![0xABu8; size];
        let mut r = &data[..];
        fs.create_file(
            &root,
            name,
            &mut r,
            size as u64,
            &CreateFileOptions::default(),
        )
        .unwrap_or_else(|e| panic!("create {name}: {e}"));
        fs.sync_metadata().unwrap();
    }

    /// Walk a named root file's cluster chain.
    fn chain_of(img: &mut Img, name: &str) -> Vec<u32> {
        let mut fs = open(img);
        let root = fs.root().unwrap();
        let e = fs
            .list_directory(&root)
            .unwrap()
            .into_iter()
            .find(|e| e.name == name)
            .unwrap_or_else(|| panic!("{name} not found"));
        let mut out = Vec::new();
        let mut c = e.location as u32;
        while c >= 2 {
            out.push(c);
            match fs.next_cluster(c).unwrap() {
                Some(n) => c = n,
                None => break,
            }
        }
        out
    }

    fn run_fsck(img: &mut Img) -> FsckResult {
        let mut fs = open(img);
        fsck_fat(&mut fs).expect("fsck runs")
    }

    fn run_repair(img: &mut Img) -> super::super::fsck::RepairReport {
        let mut fs = open(img);
        repair_fat(&mut fs).expect("repair runs")
    }

    fn codes(r: &FsckResult) -> Vec<String> {
        r.errors.iter().map(|e| e.code.clone()).collect()
    }

    fn cluster_size(img: &mut Img) -> usize {
        let fs = open(img);
        fs.fsck_geometry().cluster_size as usize
    }

    /// Return the `n` lowest free data clusters. Call after adding fixture
    /// files so the returned clusters don't overlap anything referenced.
    fn free_clusters(img: &mut Img, n: usize) -> Vec<u32> {
        let mut fs = open(img);
        let g = fs.fsck_geometry();
        let fat = fs.read_fat_copy(0).unwrap();
        let mut out = Vec::new();
        for c in 2..=g.max_data_cluster {
            if read_fat_entry(&fat, c, g.fat_type) == 0 {
                out.push(c);
                if out.len() == n {
                    break;
                }
            }
        }
        assert_eq!(out.len(), n, "fixture ran out of free clusters");
        out
    }

    #[test]
    fn clean_real_fixtures_report_no_errors_all_types() {
        for name in [
            "test_fat12.img.zst",
            "test_fat16.img.zst",
            "test_fat32.img.zst",
        ] {
            let mut img = load(name);
            let r = run_fsck(&mut img);
            assert!(
                r.is_clean(),
                "{name}: real fsck_msdos-clean fixture must report no errors, got {:?}",
                r.errors
            );
            assert_eq!(
                r.stats
                    .extra
                    .iter()
                    .find(|(k, _)| k == "lost_clusters")
                    .map(|(_, v)| v.as_str()),
                Some("0"),
                "{name}: a clean fixture has no lost clusters"
            );
        }
    }

    #[test]
    fn detects_and_frees_lost_cluster_chain() {
        let mut img = fat16();
        add_file(&mut img, "KEEP2.BIN", 4096);
        // Fabricate a three-cluster lost chain from clusters no entry references.
        let lost = free_clusters(&mut img, 3);
        {
            let mut fs = open(&mut img);
            let eoc = end_of_chain_marker(fs.fsck_geometry().fat_type);
            fs.write_fat_entry_disk(lost[0], lost[1]).unwrap();
            fs.write_fat_entry_disk(lost[1], lost[2]).unwrap();
            fs.write_fat_entry_disk(lost[2], eoc).unwrap();
            fs.flush_fat().unwrap();
        }

        let r = run_fsck(&mut img);
        assert!(codes(&r).contains(&"LostClusterChain".to_string()));
        assert!(r.repairable);

        let rep = run_repair(&mut img);
        assert!(rep.fixes_failed.is_empty(), "{:?}", rep.fixes_failed);
        assert!(rep
            .fixes_applied
            .iter()
            .any(|f| f.contains("LostClusterChain")));

        let r2 = run_fsck(&mut img);
        assert!(r2.is_clean(), "post-repair errors: {:?}", r2.errors);
    }

    #[test]
    fn detects_and_repairs_chain_loop() {
        // FAT12 base: exercises the 12-bit (half-byte) FAT packing in the walk.
        let mut img = fat12();
        let cs = cluster_size(&mut img);
        add_file(&mut img, "LOOPF.BIN", cs * 4); // multi-cluster
        let chain = chain_of(&mut img, "LOOPF.BIN");
        assert!(chain.len() >= 2, "need a multi-cluster file");
        // Point the last cluster back at the first: a loop.
        {
            let mut fs = open(&mut img);
            fs.write_fat_entry_disk(*chain.last().unwrap(), chain[0])
                .unwrap();
            fs.flush_fat().unwrap();
        }

        let r = run_fsck(&mut img);
        assert!(
            codes(&r).contains(&"ChainLoop".to_string()),
            "{:?}",
            r.errors
        );

        run_repair(&mut img);
        let r2 = run_fsck(&mut img);
        assert!(r2.is_clean(), "post-repair errors: {:?}", r2.errors);
    }

    #[test]
    fn detects_and_truncates_chain_longer_than_size() {
        let mut img = fat16();
        let cs = cluster_size(&mut img);
        add_file(&mut img, "OVERF.BIN", cs * 4); // exactly 4 clusters
        let chain = chain_of(&mut img, "OVERF.BIN");
        // Append an extra (free) cluster past the size-implied end.
        let extra = free_clusters(&mut img, 1)[0];
        {
            let mut fs = open(&mut img);
            let eoc = end_of_chain_marker(fs.fsck_geometry().fat_type);
            fs.write_fat_entry_disk(*chain.last().unwrap(), extra)
                .unwrap();
            fs.write_fat_entry_disk(extra, eoc).unwrap();
            fs.flush_fat().unwrap();
        }

        let r = run_fsck(&mut img);
        assert!(
            codes(&r).contains(&"ChainLongerThanSize".to_string()),
            "{:?}",
            r.errors
        );

        run_repair(&mut img);
        let r2 = run_fsck(&mut img);
        assert!(r2.is_clean(), "post-repair errors: {:?}", r2.errors);
        // The extra cluster is now free again.
        let mut fs = open(&mut img);
        let g = fs.fsck_geometry();
        let fat = fs.read_fat_copy(0).unwrap();
        assert_eq!(read_fat_entry(&fat, extra, g.fat_type), 0, "tail freed");
    }

    #[test]
    fn detects_and_repairs_fat_mirror_mismatch() {
        let mut img = fat16();
        add_file(&mut img, "MIRR.BIN", 4096);
        // Corrupt a byte in FAT copy #1 (the mirror) only.
        {
            let mut fs = open(&mut img);
            assert!(fs.fsck_geometry().num_fats >= 2, "need a mirrored FAT");
            let mut copy = fs.read_fat_copy(1).unwrap();
            copy[64] ^= 0xFF;
            fs.write_fat_copy(1, &copy).unwrap();
            fs.flush_fat().unwrap();
        }

        let r = run_fsck(&mut img);
        assert!(
            codes(&r).contains(&"FatMirrorMismatch".to_string()),
            "{:?}",
            r.errors
        );

        run_repair(&mut img);
        let r2 = run_fsck(&mut img);
        assert!(r2.is_clean(), "post-repair errors: {:?}", r2.errors);
    }

    #[test]
    fn cross_link_is_surfaced_but_not_auto_repaired() {
        let mut img = fat16();
        let cs = cluster_size(&mut img);
        // "XLA" sorts before "XLB", so A is traced first and claims the shared
        // cluster; B then reports the cross-link.
        add_file(&mut img, "XLA.BIN", cs * 3); // [A0, A1, A2]
        add_file(&mut img, "XLB.BIN", cs); // [B0]
        let a = chain_of(&mut img, "XLA.BIN");
        let b = chain_of(&mut img, "XLB.BIN");
        // Redirect A's middle link into B's cluster: A0 -> A1 -> B0, sharing B0.
        // A's length stays 3 (== its size), so only the cross-link stands out.
        {
            let mut fs = open(&mut img);
            fs.write_fat_entry_disk(a[1], b[0]).unwrap();
            fs.flush_fat().unwrap();
        }

        let r = run_fsck(&mut img);
        assert!(
            codes(&r).contains(&"CrossLinkedCluster".to_string()),
            "{:?}",
            r.errors
        );
        // Cross-links need an editor to resolve; repair must not claim to fix it.
        let rep = run_repair(&mut img);
        assert!(rep.unrepairable_count >= 1);
    }

    /// Regression guard for the `create_blank_fat` FAT12/16 sizing bug: the
    /// formatter used to label ~4-32 MiB volumes FAT12 while their cluster count
    /// implied FAT16, sizing the FAT for 12-bit entries — undersized once `open`
    /// re-derived FAT16. Every size below must now format to a self-consistent,
    /// fsck-clean volume (in particular, no `FatTooSmallForClusters`).
    #[test]
    fn create_blank_fat_is_geometrically_valid_across_sizes() {
        for size in [
            1024 * 1024u64,    // FAT12
            2 * 1024 * 1024,   // FAT12 near the boundary
            4 * 1024 * 1024,   // was mislabeled FAT12
            8 * 1024 * 1024,   // the reported case
            16 * 1024 * 1024,  // was mislabeled FAT12
            32 * 1024 * 1024,  // was mislabeled FAT12
            64 * 1024 * 1024,  // FAT16
            128 * 1024 * 1024, // FAT16
        ] {
            let mut img = Cursor::new(create_blank_fat(size, Some("VOL")).expect("format"));
            add_file(&mut img, "F.BIN", 8192);
            let r = run_fsck(&mut img);
            assert!(
                !codes(&r).contains(&"FatTooSmallForClusters".to_string()),
                "size {size}: FAT is undersized for the cluster count (the bug)"
            );
            assert!(
                r.is_clean(),
                "size {size}: create_blank_fat produced a volume our fsck flags: {:?}",
                r.errors
            );
        }
    }

    // ---- Oracle cross-check against the system `fsck_msdos` (macOS/BSD). ----
    // Auto-skips where the tool is absent (e.g. Linux CI), so it stays hermetic
    // there while giving a strong "an independent checker agrees" signal on dev.

    use std::process::Command;
    use std::sync::atomic::{AtomicU32, Ordering};

    static NONCE: AtomicU32 = AtomicU32::new(0);

    fn fsck_msdos_available() -> bool {
        match Command::new("fsck_msdos").arg("-q").output() {
            Ok(_) => true,
            Err(e) => e.kind() != std::io::ErrorKind::NotFound,
        }
    }

    /// Write `bytes` to a unique temp file and return its path.
    fn dump(bytes: &[u8], tag: &str) -> std::path::PathBuf {
        let n = NONCE.fetch_add(1, Ordering::Relaxed);
        let p = std::env::temp_dir().join(format!(
            "rb_fat_fsck_{}_{}_{}.img",
            std::process::id(),
            tag,
            n
        ));
        std::fs::write(&p, bytes).expect("write temp image");
        p
    }

    /// True when `fsck_msdos -n` considers the image clean (exit success).
    fn oracle_clean(path: &std::path::Path) -> bool {
        Command::new("fsck_msdos")
            .arg("-n")
            .arg(path)
            .output()
            .map(|o| o.status.success())
            .unwrap_or(false)
    }

    #[test]
    fn oracle_fsck_msdos_agrees_clean_and_after_repair() {
        if !fsck_msdos_available() {
            eprintln!("skipping: fsck_msdos not available");
            return;
        }

        // (1) A real fixture with our own added files is clean per the oracle.
        let mut img = fat16();
        add_file(&mut img, "ORCLK.BIN", 4096);
        add_file(&mut img, "ORCLB.BIN", 30_000);
        let clean_path = dump(img.get_ref(), "clean");
        assert!(
            oracle_clean(&clean_path),
            "fsck_msdos should find the populated fixture clean"
        );

        // (2) Introduce a lost chain, repair with our code, and confirm the
        //     oracle now considers the repaired image clean.
        let lost = free_clusters(&mut img, 3);
        {
            let mut fs = open(&mut img);
            let eoc = end_of_chain_marker(fs.fsck_geometry().fat_type);
            fs.write_fat_entry_disk(lost[0], lost[1]).unwrap();
            fs.write_fat_entry_disk(lost[1], lost[2]).unwrap();
            fs.write_fat_entry_disk(lost[2], eoc).unwrap();
            fs.flush_fat().unwrap();
        }
        // Our checker sees it...
        assert!(codes(&run_fsck(&mut img)).contains(&"LostClusterChain".to_string()));
        // ...the oracle sees it too (lost chains are a classic dosfsck finding)...
        let dirty_path = dump(img.get_ref(), "dirty");
        assert!(
            !oracle_clean(&dirty_path),
            "fsck_msdos should flag the fabricated lost chain"
        );
        // ...and after our repair, the oracle agrees it's clean.
        run_repair(&mut img);
        let repaired_path = dump(img.get_ref(), "repaired");
        assert!(
            oracle_clean(&repaired_path),
            "fsck_msdos should find our repaired volume clean"
        );

        let _ = std::fs::remove_file(clean_path);
        let _ = std::fs::remove_file(dirty_path);
        let _ = std::fs::remove_file(repaired_path);
    }

    /// Independent confirmation of the `create_blank_fat` sizing fix: the system
    /// `fsck_msdos` must accept a freshly formatted volume at each size that used
    /// to be mislabeled. (Before the fix it reported "FAT size too small".)
    #[test]
    fn oracle_create_blank_fat_is_fsck_msdos_clean_across_sizes() {
        if !fsck_msdos_available() {
            eprintln!("skipping: fsck_msdos not available");
            return;
        }
        for size in [
            2 * 1024 * 1024u64,
            4 * 1024 * 1024,
            8 * 1024 * 1024,
            16 * 1024 * 1024,
            32 * 1024 * 1024,
            64 * 1024 * 1024,
        ] {
            let mut img = Cursor::new(create_blank_fat(size, Some("VOL")).expect("format"));
            add_file(&mut img, "F.BIN", 8192);
            let path = dump(img.get_ref(), &format!("blank{size}"));
            assert!(
                oracle_clean(&path),
                "size {size}: fsck_msdos rejects our freshly formatted volume"
            );
            let _ = std::fs::remove_file(path);
        }
    }
}
