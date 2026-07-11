//! SFS (Smart File System) filesystem check (fsck) + repair.
//!
//! A port of the `SFScheck` reference validator's block-allocation phases,
//! shaped like the AFFS Disk Validator (`affs_fsck.rs`). Unlike PFS3, every
//! SFS metadata block carries a checksum (`sum(all longs) + 1 == 0`), so
//! `SfsBadChecksum` is a real structural check here.
//!
//! SFS has a single allocation bitmap covering **every** block (AmigaDOS
//! convention: **set bit = free**). Metadata blocks live inside 32-block
//! "adminspace" regions that the main bitmap reserves as whole regions, so the
//! observed allocation set is built from:
//!   - the reserved head/tail blocks (root + backup-root) and the bitmap blocks,
//!   - every adminspace region reachable from the AdminSpaceContainer chain, and
//!   - every file's data extents (and softlink target block) reached by walking
//!     the object tree from the root directory.
//!
//! Phases:
//!   1. Validate both root-block copies.
//!   2. Seed the reserved + bitmap regions.
//!   3. Walk the AdminSpaceContainer chain, reserving each 32-block region.
//!   4. Walk the object tree, validating every ObjectContainer checksum and
//!      marking file extents.
//!   5. Rebuild the bitmap from the observed set and diff it against the
//!      on-disk bitmap — the classic "validation needed" condition.
//!
//! Repair rewrites the bitmap from the observed set, but only when the
//! structural walk was clean (a broken chain could hide reachable blocks, and
//! freeing them would corrupt the volume). Structural damage is surfaced
//! read-only. Repair touches only `BTMP` blocks, so it is safe regardless of
//! the extent/node btree depth (the edit path can't split those trees, but the
//! read path — and therefore this check — descends multi-level trees fine).

use std::collections::{HashSet, VecDeque};

use super::filesystem::FilesystemError;
use super::fsck::{FsckIssue, FsckResult, FsckStats, RepairReport};
use super::sfs::{SfsChildKind, SfsFilesystem};

/// Internal issue codes, mapped to the shared `code: String` used by the GUI.
#[derive(Debug, Clone, Copy)]
enum SfsFsckCode {
    BadChecksum,
    BadType,
    OutOfRange,
    BrokenChain,
    BitmapMismatch,
    OrphanBlock,
}

impl SfsFsckCode {
    fn as_str(&self) -> &'static str {
        match self {
            SfsFsckCode::BadChecksum => "SfsBadChecksum",
            SfsFsckCode::BadType => "SfsBadType",
            SfsFsckCode::OutOfRange => "SfsOutOfRange",
            SfsFsckCode::BrokenChain => "SfsBrokenChain",
            SfsFsckCode::BitmapMismatch => "SfsBitmapMismatch",
            SfsFsckCode::OrphanBlock => "SfsOrphanBlock",
        }
    }
}

fn issue(code: SfsFsckCode, message: impl Into<String>, repairable: bool) -> FsckIssue {
    FsckIssue {
        code: code.as_str().to_string(),
        message: message.into(),
        repairable,
        debug: false,
    }
}

/// A compact bit set (one bit per block, LSB-first per byte); `set = allocated`.
struct BitSet {
    bits: Vec<u8>,
    len: u32,
}

impl BitSet {
    fn new(len: u32) -> Self {
        Self {
            bits: vec![0u8; (len as usize).div_ceil(8)],
            len,
        }
    }
    fn set(&mut self, idx: u32) {
        if idx < self.len {
            self.bits[(idx / 8) as usize] |= 1u8 << (idx % 8);
        }
    }
    fn get(&self, idx: u32) -> bool {
        idx < self.len && (self.bits[(idx / 8) as usize] >> (idx % 8)) & 1 == 1
    }
    fn count_set(&self) -> u32 {
        self.bits.iter().map(|b| b.count_ones()).sum()
    }
    fn as_bytes(&self) -> &[u8] {
        &self.bits
    }
}

/// Result of the reachability walk, shared by `check` and `repair`.
struct Walk {
    /// Observed block allocation (per absolute block; set = allocated).
    alloc: BitSet,
    files_checked: u32,
    directories_checked: u32,
    bad_ids: Vec<String>,
    bad_types: Vec<String>,
    out_of_range: Vec<String>,
    broken_chains: Vec<String>,
}

impl Walk {
    fn structurally_clean(&self) -> bool {
        self.bad_ids.is_empty()
            && self.bad_types.is_empty()
            && self.out_of_range.is_empty()
            && self.broken_chains.is_empty()
    }
    fn structural_error_count(&self) -> usize {
        self.bad_ids.len()
            + self.bad_types.len()
            + self.out_of_range.len()
            + self.broken_chains.len()
    }
    fn mark_range(&mut self, start: u32, count: u32) {
        for b in start..start.saturating_add(count) {
            self.alloc.set(b);
        }
    }
}

/// Walk one file's extent chain, marking every block it covers.
fn mark_file_extents<R: std::io::Read + std::io::Seek>(
    fs: &mut SfsFilesystem<R>,
    first_data: u32,
    name: &str,
    walk: &mut Walk,
) {
    let total = fs.total_blocks();
    if first_data == 0 {
        return;
    }
    let mut cur = first_data;
    let mut visited: HashSet<u32> = HashSet::new();
    let mut guard = 0u32;
    loop {
        if cur >= total {
            walk.out_of_range
                .push(format!("file '{name}': extent start {cur} out of range"));
            break;
        }
        let (next, _prev, blocks) = match fs.peek_extent(cur) {
            Ok(Some(t)) => t,
            Ok(None) => break,
            Err(e) => {
                walk.broken_chains
                    .push(format!("file '{name}': extent lookup at {cur} failed: {e}"));
                break;
            }
        };
        if blocks == 0 {
            walk.broken_chains
                .push(format!("file '{name}': zero-length extent at {cur}"));
            break;
        }
        if cur as u64 + blocks as u64 > total as u64 {
            walk.out_of_range.push(format!(
                "file '{name}': extent [{cur}, {cur}+{blocks}) runs past the volume"
            ));
            walk.mark_range(cur, total.saturating_sub(cur));
            break;
        }
        walk.mark_range(cur, blocks);
        if next == 0 {
            break;
        }
        if !visited.insert(next) {
            walk.broken_chains
                .push(format!("file '{name}': extent chain cycle at {next}"));
            break;
        }
        guard += 1;
        if guard > 10_000_000 {
            walk.broken_chains
                .push(format!("file '{name}': extent chain exceeds 10M links"));
            break;
        }
        cur = next;
    }
}

/// Walk the AdminSpaceContainer chain, reserving each adminspace region.
fn walk_admin_chain<R: std::io::Read + std::io::Seek>(fs: &mut SfsFilesystem<R>, walk: &mut Walk) {
    let total = fs.total_blocks();
    let mut blk = fs.adminspacecontainer();
    let mut visited: HashSet<u32> = HashSet::new();
    let mut guard = 0u32;
    while blk != 0 {
        if blk >= total {
            walk.out_of_range
                .push(format!("AdminSpaceContainer block {blk} out of range"));
            break;
        }
        if !visited.insert(blk) {
            walk.broken_chains
                .push(format!("AdminSpaceContainer chain cycle at {blk}"));
            break;
        }
        // The ADMC block itself lives inside an adminspace region, so it gets
        // reserved by one of the regions below; no need to mark it directly.
        let (next, regions) = match fs.scan_admin_container(blk) {
            Ok(v) => v,
            Err(e) => {
                walk.bad_ids.push(format!("AdminSpaceContainer {blk}: {e}"));
                break;
            }
        };
        for (space, region_size) in regions {
            if space >= total || space as u64 + region_size as u64 > total as u64 {
                walk.out_of_range.push(format!(
                    "adminspace region [{space}, {space}+{region_size}) out of range"
                ));
                continue;
            }
            walk.mark_range(space, region_size);
        }
        guard += 1;
        if guard > 1_000_000 {
            walk.broken_chains
                .push("AdminSpaceContainer chain exceeds 1M links".into());
            break;
        }
        blk = next;
    }
}

/// Walk the object tree from the root directory, validating every
/// ObjectContainer and marking file extents / softlink blocks.
fn walk_object_tree<R: std::io::Read + std::io::Seek>(fs: &mut SfsFilesystem<R>, walk: &mut Walk) {
    let total = fs.total_blocks();
    let root_fdb = match fs.root_firstdirblock() {
        Ok(v) => v,
        Err(e) => {
            walk.bad_ids
                .push(format!("root object container unreadable: {e}"));
            return;
        }
    };
    walk.directories_checked = 1; // the root itself

    let mut queue: VecDeque<u32> = VecDeque::new();
    let mut visited_dirs: HashSet<u32> = HashSet::new();
    if root_fdb != 0 {
        queue.push_back(root_fdb);
        visited_dirs.insert(root_fdb);
    }

    while let Some(fdb) = queue.pop_front() {
        // Walk this directory's ObjectContainer chain.
        let mut blk = fdb;
        let mut chain_visited: HashSet<u32> = HashSet::new();
        let mut chain_guard = 0u32;
        while blk != 0 {
            if blk >= total {
                walk.out_of_range
                    .push(format!("ObjectContainer block {blk} out of range"));
                break;
            }
            let (next, children) = match fs.scan_object_container(blk) {
                Ok(v) => v,
                Err(e) => {
                    walk.bad_ids.push(format!("ObjectContainer {blk}: {e}"));
                    break;
                }
            };
            for child in children {
                match child.kind {
                    SfsChildKind::Dir => {
                        walk.directories_checked = walk.directories_checked.saturating_add(1);
                        if child.firstdirblock != 0
                            && visited_dirs.insert(child.firstdirblock)
                            && visited_dirs.len() <= total as usize
                        {
                            queue.push_back(child.firstdirblock);
                        }
                    }
                    SfsChildKind::File => {
                        walk.files_checked = walk.files_checked.saturating_add(1);
                        mark_file_extents(fs, child.first_data, &child.name, walk);
                    }
                    SfsChildKind::Softlink => {
                        // The SLNK target block is a data block; reserve it.
                        if child.first_data != 0 && child.first_data < total {
                            walk.alloc.set(child.first_data);
                        }
                    }
                }
            }
            if next == 0 {
                break;
            }
            if !chain_visited.insert(next) {
                walk.broken_chains
                    .push(format!("ObjectContainer chain cycle at {next}"));
                break;
            }
            chain_guard += 1;
            if chain_guard > 1_000_000 {
                walk.broken_chains
                    .push("ObjectContainer chain exceeds 1M links".into());
                break;
            }
            blk = next;
        }
    }
}

/// Run the full reachability walk over the volume.
fn analyze<R: std::io::Read + std::io::Seek>(
    fs: &mut SfsFilesystem<R>,
) -> Result<Walk, FilesystemError> {
    let total = fs.total_blocks();
    let mut walk = Walk {
        alloc: BitSet::new(total),
        files_checked: 0,
        directories_checked: 0,
        bad_ids: Vec::new(),
        bad_types: Vec::new(),
        out_of_range: Vec::new(),
        broken_chains: Vec::new(),
    };

    // Phase 1: both root-block copies.
    if let Some(msg) = fs.verify_root_block(0) {
        walk.bad_ids.push(format!("primary root block: {msg}"));
    }
    if total > 0 {
        if let Some(msg) = fs.verify_root_block(total - 1) {
            walk.bad_ids.push(format!("backup root block: {msg}"));
        }
    }

    // Phase 2: reserved head/tail + bitmap blocks.
    walk.mark_range(0, fs.reserved_start());
    let reserved_end = fs.reserved_end();
    if reserved_end > 0 && total >= reserved_end {
        walk.mark_range(total - reserved_end, reserved_end);
    }
    walk.mark_range(fs.bitmapbase(), fs.bitmap_blocks_needed());

    // Phase 3: adminspace regions.
    walk_admin_chain(fs, &mut walk);

    // Phase 4: object tree (file extents).
    walk_object_tree(fs, &mut walk);

    Ok(walk)
}

/// Compute `(missing, extra)` between an observed allocation set and the
/// on-disk **free** bitmap (`stored_free` bit set = free).
fn diff_against_free(observed: &BitSet, stored_free: &[u8], len: u32) -> (u32, u32) {
    let mut missing = 0u32;
    let mut extra = 0u32;
    for idx in 0..len {
        let obs_alloc = observed.get(idx);
        let sto_free = stored_free
            .get((idx / 8) as usize)
            .map(|b| (b >> (idx % 8)) & 1 == 1)
            .unwrap_or(false);
        match (obs_alloc, !sto_free) {
            (true, false) => missing += 1,
            (false, true) => extra += 1,
            _ => {}
        }
    }
    (missing, extra)
}

/// Run a non-mutating filesystem check against `fs`.
pub fn check_sfs<R: std::io::Read + std::io::Seek + Send>(
    fs: &mut SfsFilesystem<R>,
) -> Result<FsckResult, FilesystemError> {
    let mut errors = Vec::new();
    let mut warnings = Vec::new();

    let total = fs.total_blocks();
    let walk = analyze(fs)?;
    let clean_walk = walk.structurally_clean();

    push_bucket(&mut errors, SfsFsckCode::BadChecksum, &walk.bad_ids);
    push_bucket(&mut errors, SfsFsckCode::BadType, &walk.bad_types);
    push_bucket(&mut errors, SfsFsckCode::OutOfRange, &walk.out_of_range);
    push_bucket(&mut errors, SfsFsckCode::BrokenChain, &walk.broken_chains);

    let stored = fs.stored_free_bitmap()?;
    let (missing, extra) = diff_against_free(&walk.alloc, &stored, total);
    let bitmap_clean = missing == 0 && extra == 0;
    if !bitmap_clean {
        errors.push(issue(
            SfsFsckCode::BitmapMismatch,
            format!(
                "bitmap mismatch: {missing} block(s) in use but marked free, \
                 {extra} block(s) marked in use but unreachable"
            ),
            clean_walk,
        ));
    }
    if extra > 0 {
        warnings.push(issue(
            SfsFsckCode::OrphanBlock,
            format!("{extra} block(s) marked allocated but not reached from the root"),
            false,
        ));
    }

    let observed_free = total.saturating_sub(walk.alloc.count_set());
    let repairable = errors.iter().any(|e| e.repairable);
    let mut stats = FsckStats {
        files_checked: walk.files_checked,
        directories_checked: walk.directories_checked,
        extra: Vec::new(),
    };
    stats
        .extra
        .push(("Volume".to_string(), fs.volume_name().unwrap_or_default()));
    stats.extra.push((
        "Blocks".to_string(),
        format!("{observed_free} free / {total} total"),
    ));
    stats.extra.push((
        "Bitmap".to_string(),
        if bitmap_clean {
            "consistent".to_string()
        } else {
            "needs rebuild".to_string()
        },
    ));
    if !clean_walk {
        stats.extra.push((
            "Note".to_string(),
            "structural damage found — bitmap repair withheld".to_string(),
        ));
    }

    Ok(FsckResult {
        errors,
        warnings,
        stats,
        repairable,
        orphaned_entries: Vec::new(),
    })
}

/// Repair entry point. Rewrites the block bitmap from the observed allocation
/// set, but only when the structural walk was clean.
pub fn repair_sfs<R: std::io::Read + std::io::Write + std::io::Seek + Send>(
    fs: &mut SfsFilesystem<R>,
) -> Result<RepairReport, FilesystemError> {
    use super::filesystem::EditableFilesystem;

    let mut fixes_applied = Vec::new();
    let mut fixes_failed = Vec::new();

    let total = fs.total_blocks();
    let walk = analyze(fs)?;
    let unrepairable_count = walk.structural_error_count();

    if !walk.structurally_clean() {
        return Ok(RepairReport {
            fixes_applied,
            fixes_failed,
            unrepairable_count,
        });
    }

    let stored = fs.stored_free_bitmap()?;
    let (missing, extra) = diff_against_free(&walk.alloc, &stored, total);
    if missing == 0 && extra == 0 {
        return Ok(RepairReport {
            fixes_applied,
            fixes_failed,
            unrepairable_count,
        });
    }

    let observed = walk.alloc.as_bytes().to_vec();
    let diff = missing + extra;
    drop(walk);

    match fs.rewrite_bitmap_from(&observed) {
        Ok(()) => {
            if let Err(e) = fs.sync_metadata() {
                fixes_failed.push(format!("bitmap rewrite flushed with error: {e}"));
            } else {
                fixes_applied.push(format!(
                    "rebuilt block bitmap from filesystem walk ({diff} bit(s) corrected)"
                ));
            }
        }
        Err(e) => fixes_failed.push(format!("bitmap rewrite failed: {e}")),
    }

    Ok(RepairReport {
        fixes_applied,
        fixes_failed,
        unrepairable_count,
    })
}

/// Turn a bucket of structural findings into `FsckIssue` errors, capping the
/// count so a pathologically corrupt volume can't produce an unbounded list.
fn push_bucket(errors: &mut Vec<FsckIssue>, code: SfsFsckCode, msgs: &[String]) {
    const CAP: usize = 32;
    for msg in msgs.iter().take(CAP) {
        errors.push(issue(code, msg.clone(), false));
    }
    if msgs.len() > CAP {
        errors.push(issue(
            code,
            format!("... and {} more", msgs.len() - CAP),
            false,
        ));
    }
}
