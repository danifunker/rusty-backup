//! exFAT integrity check (fsck) and repair.
//!
//! Promotes the old boot-checksum-only `validate_exfat_integrity` gate to a
//! real allocation-level check + repair, in the same shape as `fat_fsck` /
//! `ufs_fsck`: a single [`analyze`] pass produces both the [`FsckResult`] the
//! read-only [`fsck_exfat`] reports and the concrete fixes the writable
//! [`repair_exfat`] applies.
//!
//! ## What it checks
//!
//! - **Main boot checksum** vs the stored value in boot sector 11.
//! - **Backup boot region** (sectors 12-23) vs the main region.
//! - **VolumeDirty flag** in the VBR (volume was not cleanly unmounted).
//! - **Allocation bitmap** reconciled against the actual allocation: the
//!   directory tree is walked (each file/dir traced contiguously or through the
//!   FAT per its NoFatChain flag), plus the root chain, the bitmap file, and the
//!   up-case table. Clusters the bitmap marks free but a chain uses
//!   (`BitmapUsedButFree`) and clusters it marks allocated that nothing
//!   references (`BitmapLeaked`) are both flagged.
//! - **Cross-linked / out-of-range cluster references** (surfaced).
//! - **Up-case table checksum** vs its directory entry (surfaced).
//!
//! ## What it repairs
//!
//! - **Rebuild the allocation bitmap** from the traced allocation (the exFAT
//!   analogue of CBM VALIDATE's BAM rewrite / FAT's lost-chain reclaim).
//! - **Recompute PercentInUse**, clear VolumeDirty, recompute the main boot
//!   checksum, and resynchronise the backup boot region from the main — so the
//!   volume is left fully self-consistent.
//!
//! Cross-links, out-of-range references, and up-case damage are surfaced for
//! diagnosis but not auto-repaired (they need directory-entry rewrites or a
//! table regenerate), matching the `ufs_fsck` / `fat_fsck` policy.

use std::collections::{HashMap, HashSet, VecDeque};
use std::io::{Read, Seek, Write};

use super::exfat::{
    build_checksum_sector, compute_exfat_boot_checksum, exfat_table_checksum, ExfatFilesystem,
    ExfatGeom, ENTRY_TYPE_ALLOCATION_BITMAP, ENTRY_TYPE_FILE, ENTRY_TYPE_FILE_NAME,
    ENTRY_TYPE_STREAM_EXT, ENTRY_TYPE_UPCASE_TABLE,
};
use super::filesystem::FilesystemError;
use super::fsck::{FsckIssue, FsckResult, FsckStats};

const DIR_ENTRY_SIZE: usize = 32;
const ATTR_DIRECTORY: u16 = 0x10;
/// VolumeDirty is bit 1 of the VBR VolumeFlags field at offset 0x6A.
const VOLUME_FLAGS_OFFSET: usize = 0x6A;
const VOLUME_DIRTY_BIT: u8 = 0x02;
const PERCENT_IN_USE_OFFSET: usize = 0x70;

/// A file or directory discovered in the tree, with the fields needed to trace
/// its cluster allocation.
struct EntryRef {
    label: String,
    first_cluster: u32,
    data_length: u64,
    is_dir: bool,
    no_fat_chain: bool,
}

/// The special metadata entries found in the root directory.
#[derive(Default)]
struct Specials {
    /// (first cluster, byte length) of the allocation bitmap.
    bitmap: Option<(u32, u64)>,
    /// (first cluster, byte length, stored checksum) of the up-case table.
    upcase: Option<(u32, u64, u32)>,
}

struct Analysis {
    geom: ExfatGeom,
    errors: Vec<FsckIssue>,
    warnings: Vec<FsckIssue>,
    unrepairable: usize,

    /// The correct bitmap bytes, present only when the on-disk bitmap differs.
    bitmap_rebuild: Option<Vec<u8>>,
    /// True when the main boot checksum did not match.
    boot_checksum_bad: bool,
    /// True when the backup boot region diverged from the main region.
    backup_bad: bool,
    /// True when the VolumeDirty flag was set.
    volume_dirty: bool,
    /// Cluster count in the traced allocation (for PercentInUse).
    used_clusters: u64,

    files_checked: u32,
    dirs_checked: u32,
    bitmap_used_but_free: u64,
    bitmap_leaked: u64,
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
}

/// True when `cluster` is a valid data-cluster number.
fn in_range(cluster: u32, g: &ExfatGeom) -> bool {
    cluster >= 2 && cluster < g.cluster_count + 2
}

/// Read a directory's raw entry bytes, honouring its NoFatChain flag.
fn read_dir_data<R: Read + Seek + Send>(
    fs: &mut ExfatFilesystem<R>,
    first_cluster: u32,
    data_length: u64,
    no_fat_chain: bool,
) -> Result<Vec<u8>, FilesystemError> {
    if no_fat_chain && data_length > 0 {
        fs.read_contiguous_clusters(first_cluster, data_length, Some(data_length))
    } else {
        fs.read_cluster_chain(first_cluster, None)
    }
}

/// Parse one directory's 32-byte entries into (file/dir entries, specials).
fn parse_entries(data: &[u8], parent_path: &str) -> (Vec<EntryRef>, Specials) {
    let mut entries = Vec::new();
    let mut specials = Specials::default();
    let mut pos = 0;
    while pos + DIR_ENTRY_SIZE <= data.len() {
        let etype = data[pos];
        if etype == 0x00 {
            break; // end of directory
        }
        match etype {
            ENTRY_TYPE_ALLOCATION_BITMAP => {
                let cl = u32::from_le_bytes([
                    data[pos + 20],
                    data[pos + 21],
                    data[pos + 22],
                    data[pos + 23],
                ]);
                let len = u64::from_le_bytes([
                    data[pos + 24],
                    data[pos + 25],
                    data[pos + 26],
                    data[pos + 27],
                    data[pos + 28],
                    data[pos + 29],
                    data[pos + 30],
                    data[pos + 31],
                ]);
                specials.bitmap = Some((cl, len));
                pos += DIR_ENTRY_SIZE;
            }
            ENTRY_TYPE_UPCASE_TABLE => {
                let checksum = u32::from_le_bytes([
                    data[pos + 4],
                    data[pos + 5],
                    data[pos + 6],
                    data[pos + 7],
                ]);
                let cl = u32::from_le_bytes([
                    data[pos + 20],
                    data[pos + 21],
                    data[pos + 22],
                    data[pos + 23],
                ]);
                let len = u64::from_le_bytes([
                    data[pos + 24],
                    data[pos + 25],
                    data[pos + 26],
                    data[pos + 27],
                    data[pos + 28],
                    data[pos + 29],
                    data[pos + 30],
                    data[pos + 31],
                ]);
                specials.upcase = Some((cl, len, checksum));
                pos += DIR_ENTRY_SIZE;
            }
            ENTRY_TYPE_FILE => {
                let secondary_count = data[pos + 1] as usize;
                let attrs = u16::from_le_bytes([data[pos + 4], data[pos + 5]]);
                let stream_pos = pos + DIR_ENTRY_SIZE;
                if stream_pos + DIR_ENTRY_SIZE > data.len()
                    || data[stream_pos] != ENTRY_TYPE_STREAM_EXT
                {
                    pos += DIR_ENTRY_SIZE;
                    continue;
                }
                let no_fat_chain = (data[stream_pos + 1] & 0x02) != 0;
                let name_length = data[stream_pos + 3] as usize;
                let first_cluster = u32::from_le_bytes([
                    data[stream_pos + 20],
                    data[stream_pos + 21],
                    data[stream_pos + 22],
                    data[stream_pos + 23],
                ]);
                let data_length = u64::from_le_bytes([
                    data[stream_pos + 24],
                    data[stream_pos + 25],
                    data[stream_pos + 26],
                    data[stream_pos + 27],
                    data[stream_pos + 28],
                    data[stream_pos + 29],
                    data[stream_pos + 30],
                    data[stream_pos + 31],
                ]);

                let mut name_chars = Vec::new();
                for i in 0..secondary_count.saturating_sub(1) {
                    let fn_pos = pos + 64 + i * DIR_ENTRY_SIZE;
                    if fn_pos + DIR_ENTRY_SIZE > data.len() || data[fn_pos] != ENTRY_TYPE_FILE_NAME
                    {
                        break;
                    }
                    for j in 0..15 {
                        if name_chars.len() >= name_length {
                            break;
                        }
                        let off = fn_pos + 2 + j * 2;
                        if off + 1 < data.len() {
                            name_chars.push(u16::from_le_bytes([data[off], data[off + 1]]));
                        }
                    }
                }
                name_chars.truncate(name_length);
                let name = String::from_utf16_lossy(&name_chars);

                pos += DIR_ENTRY_SIZE * (1 + secondary_count);

                if name == "." || name == ".." || name.is_empty() {
                    continue;
                }
                let is_dir = attrs & ATTR_DIRECTORY != 0;
                let label = if parent_path == "/" {
                    format!("/{name}")
                } else {
                    format!("{parent_path}/{name}")
                };
                entries.push(EntryRef {
                    label,
                    first_cluster,
                    data_length,
                    is_dir,
                    no_fat_chain,
                });
            }
            _ => pos += DIR_ENTRY_SIZE,
        }
    }
    (entries, specials)
}

/// Claim `cluster` for entry `idx`, flagging cross-links / out-of-range refs.
/// Returns true when the cluster was newly and validly claimed.
fn claim(
    cluster: u32,
    idx: usize,
    label: &str,
    g: &ExfatGeom,
    owner: &mut HashMap<u32, usize>,
    entries: &[(usize, String)],
    a: &mut Analysis,
) -> bool {
    if !in_range(cluster, g) {
        a.err(
            "OutOfRangeCluster",
            format!("{label}: references cluster {cluster} outside the volume"),
            false,
        );
        return false;
    }
    if let Some(&j) = owner.get(&cluster) {
        let other = entries
            .iter()
            .find(|(i, _)| *i == j)
            .map(|(_, l)| l.as_str())
            .unwrap_or("?");
        a.err(
            "CrossLinkedCluster",
            format!("{label}: cluster {cluster} is also claimed by {other}"),
            false,
        );
        return false;
    }
    owner.insert(cluster, idx);
    true
}

/// Trace a contiguous run of clusters into the ownership map.
#[allow(clippy::too_many_arguments)]
fn trace_contiguous(
    first: u32,
    data_length: u64,
    idx: usize,
    label: &str,
    g: &ExfatGeom,
    owner: &mut HashMap<u32, usize>,
    entries: &[(usize, String)],
    a: &mut Analysis,
) {
    if first == 0 || data_length == 0 {
        return;
    }
    let n = data_length.div_ceil(g.cluster_size) as u32;
    for c in first..first.saturating_add(n) {
        if !claim(c, idx, label, g, owner, entries, a) {
            break;
        }
    }
}

/// Trace a FAT cluster chain into the ownership map.
#[allow(clippy::too_many_arguments)]
fn trace_chain<R: Read + Seek + Send>(
    fs: &mut ExfatFilesystem<R>,
    first: u32,
    idx: usize,
    label: &str,
    g: &ExfatGeom,
    owner: &mut HashMap<u32, usize>,
    entries: &[(usize, String)],
    a: &mut Analysis,
) {
    if first == 0 {
        return;
    }
    let mut cluster = first;
    let mut seen: HashSet<u32> = HashSet::new();
    let cap = g.cluster_count as usize + 2;
    loop {
        if seen.contains(&cluster) {
            a.err(
                "ChainLoop",
                format!("{label}: cluster chain loops at {cluster}"),
                false,
            );
            break;
        }
        if !claim(cluster, idx, label, g, owner, entries, a) {
            break;
        }
        seen.insert(cluster);
        match fs.next_cluster(cluster) {
            Ok(Some(next)) => cluster = next,
            Ok(None) => break,
            Err(_) => break,
        }
        if seen.len() > cap {
            break;
        }
    }
}

/// Read the main + backup boot regions and check checksums / consistency.
fn check_boot<R: Read + Seek + Send>(fs: &mut ExfatFilesystem<R>, g: &ExfatGeom, a: &mut Analysis) {
    let bps = g.bytes_per_sector;
    let region = match fs.read_raw(g.partition_offset, 24 * bps as usize) {
        Ok(r) => r,
        Err(e) => {
            a.warn("BootReadFailed", format!("cannot read boot regions: {e}"));
            return;
        }
    };
    let main = &region[..12 * bps as usize];
    let backup = &region[12 * bps as usize..];

    // Main checksum vs stored (sector 11).
    let computed = compute_exfat_boot_checksum(main, bps);
    let cs_off = 11 * bps as usize;
    let stored = u32::from_le_bytes([
        main[cs_off],
        main[cs_off + 1],
        main[cs_off + 2],
        main[cs_off + 3],
    ]);
    if computed != stored {
        a.boot_checksum_bad = true;
        a.errors.push(FsckIssue {
            code: "BootChecksumMismatch".into(),
            message: format!("main boot checksum is {stored:#010x}, computed {computed:#010x}"),
            repairable: true,
            debug: false,
        });
    }

    // Backup VBR (sector 12) must match the main VBR (sector 0), and the backup
    // checksum must match too. Compare the whole 11-sector checksum region.
    let check_len = 11 * bps as usize;
    if backup.len() >= 12 * bps as usize && backup[..check_len] != main[..check_len] {
        a.backup_bad = true;
        a.errors.push(FsckIssue {
            code: "BackupBootMismatch".into(),
            message: "backup boot region differs from the main boot region".into(),
            repairable: true,
            debug: false,
        });
    }

    // VolumeDirty flag.
    if main[VOLUME_FLAGS_OFFSET] & VOLUME_DIRTY_BIT != 0 {
        a.volume_dirty = true;
        a.warn(
            "VolumeDirty",
            "VolumeDirty flag is set (volume was not cleanly unmounted)".into(),
        );
    }
}

/// The shared analysis pass. Read-only.
fn analyze<R: Read + Seek + Send>(
    fs: &mut ExfatFilesystem<R>,
) -> Result<Analysis, FilesystemError> {
    let g = fs.fsck_geometry();
    let mut a = Analysis {
        geom: g,
        errors: Vec::new(),
        warnings: Vec::new(),
        unrepairable: 0,
        bitmap_rebuild: None,
        boot_checksum_bad: false,
        backup_bad: false,
        volume_dirty: false,
        used_clusters: 0,
        files_checked: 0,
        dirs_checked: 0,
        bitmap_used_but_free: 0,
        bitmap_leaked: 0,
    };

    check_boot(fs, &g, &mut a);

    if g.bitmap_start_cluster < 2 {
        a.err(
            "MissingAllocationBitmap",
            "no allocation bitmap entry in the root directory".into(),
            false,
        );
        return Ok(a);
    }

    // Enumerate the tree. Each traceable unit gets an index into `labels` so the
    // ownership map can name the other party on a cross-link.
    let mut labels: Vec<(usize, String)> = Vec::new();
    let mut owner: HashMap<u32, usize> = HashMap::new();

    // Pseudo-entries: root chain, bitmap, up-case table.
    let root_idx = labels.len();
    labels.push((root_idx, "<root directory>".into()));
    trace_chain(
        fs,
        g.root_cluster,
        root_idx,
        "<root directory>",
        &g,
        &mut owner,
        &labels,
        &mut a,
    );

    // Walk directories, collecting entries + specials (specials only in root).
    let mut queue: VecDeque<(u32, u64, bool, String)> = VecDeque::new();
    queue.push_back((g.root_cluster, 0, false, "/".into()));
    let mut visited_dirs: HashSet<u32> = HashSet::new();
    visited_dirs.insert(g.root_cluster);
    let mut all_entries: Vec<EntryRef> = Vec::new();
    let mut upcase: Option<(u32, u64, u32)> = None;

    while let Some((cl, len, nfc, path)) = queue.pop_front() {
        a.dirs_checked += 1;
        let data = match read_dir_data(fs, cl, len, nfc) {
            Ok(d) => d,
            Err(e) => {
                a.warn(
                    "DirReadFailed",
                    format!("cannot read directory {path}: {e}"),
                );
                continue;
            }
        };
        let (entries, specials) = parse_entries(&data, &path);
        if path == "/" {
            if let Some((bc, bl)) = specials.bitmap {
                let idx = labels.len();
                labels.push((idx, "<allocation bitmap>".into()));
                trace_contiguous(
                    bc,
                    bl,
                    idx,
                    "<allocation bitmap>",
                    &g,
                    &mut owner,
                    &labels,
                    &mut a,
                );
            }
            if let Some((uc, ul, uchk)) = specials.upcase {
                upcase = Some((uc, ul, uchk));
                let idx = labels.len();
                labels.push((idx, "<up-case table>".into()));
                trace_contiguous(
                    uc,
                    ul,
                    idx,
                    "<up-case table>",
                    &g,
                    &mut owner,
                    &labels,
                    &mut a,
                );
            }
        }
        for e in entries {
            if e.is_dir {
                if e.first_cluster >= 2 && visited_dirs.insert(e.first_cluster) {
                    queue.push_back((
                        e.first_cluster,
                        e.data_length,
                        e.no_fat_chain,
                        e.label.clone(),
                    ));
                }
            } else {
                a.files_checked += 1;
            }
            all_entries.push(e);
        }
    }

    // Trace every file/dir's allocation.
    for e in &all_entries {
        let idx = labels.len();
        labels.push((idx, e.label.clone()));
        if e.no_fat_chain {
            trace_contiguous(
                e.first_cluster,
                e.data_length,
                idx,
                &e.label,
                &g,
                &mut owner,
                &labels,
                &mut a,
            );
        } else {
            trace_chain(
                fs,
                e.first_cluster,
                idx,
                &e.label,
                &g,
                &mut owner,
                &labels,
                &mut a,
            );
        }
    }

    a.used_clusters = owner.len() as u64;

    // Up-case table checksum.
    if let Some((uc, ul, stored)) = upcase {
        if in_range(uc, &g) && ul > 0 {
            if let Ok(bytes) = fs.read_contiguous_clusters(uc, ul, Some(ul)) {
                let computed = exfat_table_checksum(&bytes);
                if computed != stored {
                    a.err(
                        "UpcaseChecksumMismatch",
                        format!(
                            "up-case table checksum is {stored:#010x}, computed {computed:#010x}"
                        ),
                        false,
                    );
                }
            }
        }
    }

    // Reconcile the on-disk bitmap against the traced allocation.
    let on_disk =
        fs.read_contiguous_clusters(g.bitmap_start_cluster, g.bitmap_size, Some(g.bitmap_size))?;
    let mut correct = vec![0u8; g.bitmap_size as usize];
    for &c in owner.keys() {
        let bit = (c - 2) as usize;
        let (byte, off) = (bit / 8, bit % 8);
        if byte < correct.len() {
            correct[byte] |= 1 << off;
        }
    }
    let mut used_but_free = 0u64;
    let mut leaked = 0u64;
    for c in 2..g.cluster_count + 2 {
        let bit = (c - 2) as usize;
        let (byte, off) = (bit / 8, bit % 8);
        let disk = byte < on_disk.len() && on_disk[byte] & (1 << off) != 0;
        let should = owner.contains_key(&c);
        match (disk, should) {
            (false, true) => used_but_free += 1,
            (true, false) => leaked += 1,
            _ => {}
        }
    }
    a.bitmap_used_but_free = used_but_free;
    a.bitmap_leaked = leaked;
    if used_but_free > 0 || leaked > 0 {
        // Preserve any trailing (beyond-cluster_count) bytes verbatim so we only
        // touch the meaningful range.
        let mut rebuilt = on_disk.clone();
        let valid_bytes = (g.cluster_count as usize).div_ceil(8);
        for (i, b) in correct
            .iter()
            .enumerate()
            .take(valid_bytes.min(rebuilt.len()))
        {
            rebuilt[i] = *b;
        }
        a.bitmap_rebuild = Some(rebuilt);
        if used_but_free > 0 {
            a.errors.push(FsckIssue {
                code: "BitmapUsedButFree".into(),
                message: format!(
                    "{used_but_free} cluster(s) are referenced by a chain but marked free in the bitmap"
                ),
                repairable: true,
                debug: false,
            });
        }
        if leaked > 0 {
            a.errors.push(FsckIssue {
                code: "BitmapLeaked".into(),
                message: format!(
                    "{leaked} cluster(s) are marked allocated in the bitmap but referenced by nothing"
                ),
                repairable: true,
                debug: false,
            });
        }
    }

    Ok(a)
}

fn stats(a: &Analysis) -> FsckStats {
    FsckStats {
        files_checked: a.files_checked,
        directories_checked: a.dirs_checked,
        extra: vec![
            ("cluster_count".into(), a.geom.cluster_count.to_string()),
            ("clusters_used".into(), a.used_clusters.to_string()),
            (
                "bitmap_used_but_free".into(),
                a.bitmap_used_but_free.to_string(),
            ),
            ("bitmap_leaked".into(), a.bitmap_leaked.to_string()),
        ],
    }
}

/// Run the exFAT integrity check (read-only).
pub fn fsck_exfat<R: Read + Seek + Send>(
    fs: &mut ExfatFilesystem<R>,
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

/// Repair the exFAT faults `analyze` found: rebuild the bitmap, then leave the
/// boot regions fully consistent (PercentInUse, VolumeDirty, checksum, backup).
pub fn repair_exfat<R: Read + Write + Seek + Send>(
    fs: &mut ExfatFilesystem<R>,
) -> Result<super::fsck::RepairReport, FilesystemError> {
    let a = analyze(fs)?;
    let g = a.geom;
    let bps = g.bytes_per_sector;
    let mut fixes_applied: Vec<String> = Vec::new();
    let mut fixes_failed: Vec<String> = Vec::new();

    let needs_boot_pass =
        a.bitmap_rebuild.is_some() || a.boot_checksum_bad || a.backup_bad || a.volume_dirty;

    if let Some(bitmap) = &a.bitmap_rebuild {
        match fs.write_bitmap(bitmap) {
            Ok(()) => fixes_applied.push(format!(
                "rebuilt allocation bitmap ({} used-but-free, {} leaked)",
                a.bitmap_used_but_free, a.bitmap_leaked
            )),
            Err(e) => fixes_failed.push(format!("bitmap rebuild: {e}")),
        }
    }

    if needs_boot_pass {
        match (|| -> Result<(), FilesystemError> {
            // Read the main boot region, patch the VBR, recompute its checksum,
            // and write it back to both the main and backup regions.
            let mut main = fs.read_raw(g.partition_offset, 12 * bps as usize)?;

            // PercentInUse from the (repaired) allocation.
            let pct = if g.cluster_count == 0 {
                0
            } else {
                ((a.used_clusters * 100) / g.cluster_count as u64).min(100) as u8
            };
            main[PERCENT_IN_USE_OFFSET] = pct;
            // Clear VolumeDirty.
            main[VOLUME_FLAGS_OFFSET] &= !VOLUME_DIRTY_BIT;

            // Recompute + place the checksum sector (sector 11).
            let checksum = compute_exfat_boot_checksum(&main, bps);
            let cs_sector = build_checksum_sector(checksum, bps);
            let cs_off = 11 * bps as usize;
            main[cs_off..cs_off + bps as usize].copy_from_slice(&cs_sector);

            fs.write_raw(g.partition_offset, &main)?;
            fs.write_raw(g.partition_offset + 12 * bps, &main)?;
            Ok(())
        })() {
            Ok(()) => {
                if a.volume_dirty {
                    fixes_applied.push("cleared VolumeDirty flag".into());
                }
                if a.boot_checksum_bad {
                    fixes_applied.push("recomputed main boot checksum".into());
                }
                if a.backup_bad {
                    fixes_applied.push("resynchronised backup boot region".into());
                }
                if !a.boot_checksum_bad && !a.backup_bad && !a.volume_dirty {
                    fixes_applied
                        .push("updated PercentInUse + boot checksum after bitmap rebuild".into());
                }
            }
            Err(e) => fixes_failed.push(format!("boot region repair: {e}")),
        }
    }

    if !fixes_applied.is_empty() {
        fs.flush_writer()?;
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
    use crate::fs::exfat::ExfatFilesystem;
    use std::io::Cursor;

    type Img = Cursor<Vec<u8>>;

    /// Decompress the committed real exFAT fixture (verified `fsck_exfat`-clean
    /// via a loopback device) into a writable image.
    fn load() -> Img {
        let path = "tests/fixtures/test_exfat.img.zst";
        let compressed = std::fs::read(path).unwrap_or_else(|e| panic!("read {path}: {e}"));
        let mut dec = zstd::stream::read::Decoder::new(Cursor::new(compressed)).expect("zstd");
        let mut out = Vec::new();
        dec.read_to_end(&mut out).expect("decompress");
        Cursor::new(out)
    }

    fn open(img: &mut Img) -> ExfatFilesystem<&mut Img> {
        ExfatFilesystem::open(img, 0).expect("open exFAT")
    }

    fn run_fsck(img: &mut Img) -> FsckResult {
        let mut fs = open(img);
        fsck_exfat(&mut fs).expect("fsck runs")
    }

    fn run_repair(img: &mut Img) -> super::super::fsck::RepairReport {
        let mut fs = open(img);
        repair_exfat(&mut fs).expect("repair runs")
    }

    fn codes(r: &FsckResult) -> Vec<String> {
        r.errors.iter().map(|e| e.code.clone()).collect()
    }

    fn bitmap(img: &mut Img) -> (ExfatGeom, Vec<u8>) {
        let mut fs = open(img);
        let g = fs.fsck_geometry();
        let bm = fs
            .read_contiguous_clusters(g.bitmap_start_cluster, g.bitmap_size, Some(g.bitmap_size))
            .unwrap();
        (g, bm)
    }

    fn write_bitmap(img: &mut Img, bm: &[u8]) {
        let mut fs = open(img);
        fs.write_bitmap(bm).unwrap();
        fs.flush_writer().unwrap();
    }

    #[test]
    fn clean_fixture_reports_no_errors() {
        let mut img = load();
        let r = run_fsck(&mut img);
        assert!(
            r.is_clean(),
            "clean fixture reported errors: {:?}",
            r.errors
        );
    }

    #[test]
    fn detects_and_repairs_bitmap_leak() {
        let mut img = load();
        // Mark the lowest free cluster allocated: a leak (nothing references it).
        let (g, mut bm) = bitmap(&mut img);
        let leaked = (2..g.cluster_count + 2)
            .find(|&c| {
                let bit = (c - 2) as usize;
                bm[bit / 8] & (1 << (bit % 8)) == 0
            })
            .expect("a free cluster");
        let bit = (leaked - 2) as usize;
        bm[bit / 8] |= 1 << (bit % 8);
        write_bitmap(&mut img, &bm);

        let r = run_fsck(&mut img);
        assert!(
            codes(&r).contains(&"BitmapLeaked".to_string()),
            "{:?}",
            r.errors
        );
        assert!(r.repairable);

        let rep = run_repair(&mut img);
        assert!(rep.fixes_failed.is_empty(), "{:?}", rep.fixes_failed);
        assert!(run_fsck(&mut img).is_clean());
    }

    #[test]
    fn detects_and_repairs_bitmap_used_but_free() {
        let mut img = load();
        // Clear the bit for the bitmap's own cluster: used but marked free.
        let (g, mut bm) = bitmap(&mut img);
        let bit = (g.bitmap_start_cluster - 2) as usize;
        bm[bit / 8] &= !(1 << (bit % 8));
        write_bitmap(&mut img, &bm);

        let r = run_fsck(&mut img);
        assert!(
            codes(&r).contains(&"BitmapUsedButFree".to_string()),
            "{:?}",
            r.errors
        );

        run_repair(&mut img);
        assert!(run_fsck(&mut img).is_clean());
    }

    #[test]
    fn detects_and_repairs_boot_checksum() {
        let mut img = load();
        // Corrupt the stored main boot checksum (sector 11).
        {
            let mut fs = open(&mut img);
            let g = fs.fsck_geometry();
            let off = g.partition_offset + 11 * g.bytes_per_sector;
            let mut b = fs.read_raw(off, 4).unwrap();
            b[0] ^= 0xFF;
            fs.write_raw(off, &b).unwrap();
            fs.flush_writer().unwrap();
        }

        let r = run_fsck(&mut img);
        assert!(
            codes(&r).contains(&"BootChecksumMismatch".to_string()),
            "{:?}",
            r.errors
        );

        run_repair(&mut img);
        assert!(run_fsck(&mut img).is_clean());
    }

    #[test]
    fn detects_and_clears_volume_dirty() {
        let mut img = load();
        // Set the VolumeDirty flag (excluded from the boot checksum, so the
        // image stays otherwise valid).
        {
            let mut fs = open(&mut img);
            let g = fs.fsck_geometry();
            let mut vbr = fs.read_raw(g.partition_offset, 512).unwrap();
            vbr[VOLUME_FLAGS_OFFSET] |= VOLUME_DIRTY_BIT;
            fs.write_raw(g.partition_offset, &vbr).unwrap();
            fs.flush_writer().unwrap();
        }

        let r = run_fsck(&mut img);
        assert!(
            r.warnings.iter().any(|w| w.code == "VolumeDirty"),
            "{:?}",
            r.warnings
        );

        let rep = run_repair(&mut img);
        assert!(rep.fixes_applied.iter().any(|f| f.contains("VolumeDirty")));
        let r2 = run_fsck(&mut img);
        assert!(!r2.warnings.iter().any(|w| w.code == "VolumeDirty"));
    }

    // ---- Oracle cross-check against the system `fsck_exfat` (macOS). ----
    // exFAT's checker needs a block device, so this attaches the image with
    // `hdiutil` and runs `fsck_exfat` on the raw device, then detaches. Gated to
    // macOS with the tools present, so it stays a no-op elsewhere.

    #[cfg(target_os = "macos")]
    #[test]
    fn oracle_fsck_exfat_agrees_after_repair() {
        use std::process::Command;
        use std::sync::atomic::{AtomicU32, Ordering};
        static NONCE: AtomicU32 = AtomicU32::new(0);

        if Command::new("/sbin/fsck_exfat").output().is_err() {
            eprintln!("skipping: fsck_exfat not available");
            return;
        }

        // Corrupt the bitmap (leak a cluster), repair with our code, dump to a
        // temp file, and confirm the system checker calls the result clean.
        let mut img = load();
        let (g, mut bm) = bitmap(&mut img);
        let leaked = (2..g.cluster_count + 2)
            .find(|&c| {
                let bit = (c - 2) as usize;
                bm[bit / 8] & (1 << (bit % 8)) == 0
            })
            .unwrap();
        let bit = (leaked - 2) as usize;
        bm[bit / 8] |= 1 << (bit % 8);
        write_bitmap(&mut img, &bm);
        run_repair(&mut img);

        let n = NONCE.fetch_add(1, Ordering::Relaxed);
        let path = std::env::temp_dir().join(format!("rb_exfat_{}_{}.img", std::process::id(), n));
        std::fs::write(&path, img.get_ref()).unwrap();

        // Attach as a raw device.
        let attach = Command::new("hdiutil")
            .args([
                "attach",
                "-nomount",
                "-imagekey",
                "diskimage-class=CRawDiskImage",
            ])
            .arg(&path)
            .output()
            .expect("hdiutil attach");
        let disk = String::from_utf8_lossy(&attach.stdout)
            .lines()
            .next()
            .and_then(|l| l.split_whitespace().next())
            .map(|s| s.to_string());
        let disk = match disk {
            Some(d) if d.starts_with("/dev/disk") => d,
            _ => {
                eprintln!("skipping: hdiutil attach did not yield a device");
                let _ = std::fs::remove_file(&path);
                return;
            }
        };
        let rdisk = disk.replace("/dev/disk", "/dev/rdisk");
        let out = Command::new("/sbin/fsck_exfat")
            .arg("-n")
            .arg(&rdisk)
            .output();
        let _ = Command::new("hdiutil").args(["detach"]).arg(&disk).output();
        let _ = std::fs::remove_file(&path);

        let ok = out.map(|o| o.status.success()).unwrap_or(false);
        assert!(ok, "fsck_exfat should find our repaired volume clean");
    }
}
