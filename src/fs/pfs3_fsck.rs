//! PFS3 filesystem check (fsck) + repair.
//!
//! Modelled on the AmigaDOS/AFFS Disk Validator (`affs_fsck.rs`), adapted to
//! PFS3's structure. PFS3 has no per-block checksums, so integrity leans on
//! block IDs, pointer ranges, and chain structure. It also carries **two
//! independent allocation bitmaps** (AmigaDOS convention: **set bit = free**):
//!
//!   - the **data** bitmap (user-area sectors), spread across `BM` blocks
//!     reachable from the rootblock's `bitmapindex → MI → BM` chain, and
//!   - the **reserved** bitmap (metadata-area clusters), a single word-array
//!     at offset 512 of the rootblock cluster.
//!
//! Phases:
//!   1. Root-block sanity (geometry — mostly already gated at `open`).
//!   2. Enumerate the reserved index structure (rootblock, extension, and
//!      every `MI`/`BM`/`IB`/`AB`/`SB` block reachable from the root arrays)
//!      and walk the directory tree from the root anode, marking every
//!      reachable data sector and reserved cluster and validating block IDs,
//!      entry types, pointer ranges, and chain structure.
//!   3. Rebuild both bitmaps from the observed allocations and diff them
//!      against the on-disk bitmaps. A mismatch is the classic
//!      "validation needed" condition after an unclean unmount.
//!   4. Reconcile the `blocks_free` / `reserved_free` counters.
//!
//! Repair rewrites both bitmaps (and restamps the two free counters) from the
//! observed allocation set — **but only when the structural walk was clean**.
//! If any broken chain / bad ID / out-of-range pointer was found, the bitmap
//! and counter mismatches are still reported, but repair is withheld: freeing
//! a metadata block that a *broken* chain merely failed to reach would corrupt
//! the volume. Fix the structure (restore from backup) first. This mirrors the
//! AFFS philosophy of never silently locking in corruption.

use std::collections::{HashSet, VecDeque};

use super::filesystem::FilesystemError;
use super::fsck::{FsckIssue, FsckResult, FsckStats, RepairReport};
use super::pfs3::{
    rd_u16, rd_u32, Pfs3ChildKind, Pfs3Filesystem, ANODE_ROOTDIR, ID_BITMAPINDEXBLOCK, ID_DIRBLOCK,
    ID_INDEXBLOCK, ID_SUPERBLOCK,
};

/// Internal issue codes, mapped to the shared `code: String` used by the GUI.
#[derive(Debug, Clone, Copy)]
enum Pfs3FsckCode {
    BadBlockId,
    BadType,
    OutOfRange,
    BrokenChain,
    DataBitmapMismatch,
    ReservedBitmapMismatch,
    BlocksFreeMismatch,
    ReservedFreeMismatch,
    OrphanBlock,
}

impl Pfs3FsckCode {
    fn as_str(&self) -> &'static str {
        match self {
            Pfs3FsckCode::BadBlockId => "Pfs3BadBlockId",
            Pfs3FsckCode::BadType => "Pfs3BadType",
            Pfs3FsckCode::OutOfRange => "Pfs3OutOfRange",
            Pfs3FsckCode::BrokenChain => "Pfs3BrokenChain",
            Pfs3FsckCode::DataBitmapMismatch => "Pfs3DataBitmapMismatch",
            Pfs3FsckCode::ReservedBitmapMismatch => "Pfs3ReservedBitmapMismatch",
            Pfs3FsckCode::BlocksFreeMismatch => "Pfs3BlocksFreeMismatch",
            Pfs3FsckCode::ReservedFreeMismatch => "Pfs3ReservedFreeMismatch",
            Pfs3FsckCode::OrphanBlock => "Pfs3OrphanBlock",
        }
    }
}

fn issue(code: Pfs3FsckCode, message: impl Into<String>, repairable: bool) -> FsckIssue {
    FsckIssue {
        code: code.as_str().to_string(),
        message: message.into(),
        repairable,
        debug: false,
    }
}

/// A compact bit set (one bit per index, LSB-first within each byte). Used
/// for both observed allocation sets; `set bit = allocated`.
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
    /// Observed data-area allocation (per local data sector; set = allocated).
    data_alloc: BitSet,
    /// Observed reserved allocation (per reserved cluster; set = allocated).
    reserved_alloc: BitSet,
    files_checked: u32,
    directories_checked: u32,
    bad_ids: Vec<String>,
    bad_types: Vec<String>,
    out_of_range: Vec<String>,
    broken_chains: Vec<String>,
}

impl Walk {
    /// True when the structural walk found nothing wrong — the precondition
    /// for trusting the observed set enough to rewrite the bitmaps.
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
}

/// Mark the reserved cluster that owns HW sector `sec`, or record an
/// out-of-range finding if the sector isn't a cluster-aligned reserved block.
fn mark_reserved_sector<R: std::io::Read + std::io::Seek>(
    fs: &Pfs3Filesystem<R>,
    walk: &mut Walk,
    sec: u32,
    what: &str,
) {
    match fs.reserved_cluster_of_sector(sec) {
        Some(c) => walk.reserved_alloc.set(c),
        None => walk.out_of_range.push(format!(
            "{what} block at sector {sec} is outside the reserved area"
        )),
    }
}

/// Enumerate the reserved metadata structure that is reachable from the
/// rootblock's index arrays (independent of the directory tree): the
/// rootblock cluster, the extension block, and every `MI`/`BM`/`IB`/`AB`/`SB`
/// block. Marks each in `reserved_alloc` and validates its block ID.
fn enumerate_reserved_structure<R: std::io::Read + std::io::Seek>(
    fs: &mut Pfs3Filesystem<R>,
    walk: &mut Walk,
) -> Result<(), FilesystemError> {
    let ipb = fs.indexperblock() as usize;

    // Rootblock cluster (also holds the reserved bitmap) + extension.
    mark_reserved_sector(fs, walk, fs.rootblock_sector(), "rootblock");
    let ext = fs.extension_block();
    if ext != 0 {
        mark_reserved_sector(fs, walk, ext, "rootblock extension");
    }

    // Bitmap-index (MI) blocks and the data-bitmap (BM) blocks they point at.
    let bitmapindex = fs.bitmapindex();
    for &mi in &bitmapindex {
        if mi == 0 {
            continue;
        }
        mark_reserved_sector(fs, walk, mi, "bitmap-index (MI)");
        let blk = match fs.peek_reserved_block(mi) {
            Ok(b) => b,
            Err(e) => {
                walk.bad_ids.push(format!("MI block {mi} unreadable: {e}"));
                continue;
            }
        };
        if rd_u16(&blk, 0) != ID_BITMAPINDEXBLOCK {
            walk.bad_ids.push(format!("MI block {mi}: bad block id"));
            continue;
        }
        for i in 0..ipb {
            let o = 12 + i * 4;
            if o + 4 > blk.len() {
                break;
            }
            let bm = rd_u32(&blk, o);
            if bm != 0 {
                mark_reserved_sector(fs, walk, bm, "data-bitmap (BM)");
            }
        }
    }

    // Index (IB) blocks — from the small-layout array or, in supermode, via
    // the super-index (SB) blocks.
    let mut ibs: Vec<u32> = Vec::new();
    if fs.is_supermode() {
        let super_index = fs.super_index();
        for &sb in &super_index {
            if sb == 0 {
                continue;
            }
            mark_reserved_sector(fs, walk, sb, "super-index (SB)");
            let blk = match fs.peek_reserved_block(sb) {
                Ok(b) => b,
                Err(e) => {
                    walk.bad_ids.push(format!("SB block {sb} unreadable: {e}"));
                    continue;
                }
            };
            if rd_u16(&blk, 0) != ID_SUPERBLOCK {
                walk.bad_ids.push(format!("SB block {sb}: bad block id"));
                continue;
            }
            for i in 0..ipb {
                let o = 12 + i * 4;
                if o + 4 > blk.len() {
                    break;
                }
                let ib = rd_u32(&blk, o);
                if ib != 0 {
                    ibs.push(ib);
                }
            }
        }
    } else {
        for ib in fs.small_indexblocks() {
            if ib != 0 {
                ibs.push(ib);
            }
        }
    }

    // Anode (AB) blocks reachable from every IB.
    for ib in ibs {
        mark_reserved_sector(fs, walk, ib, "anode-index (IB)");
        let blk = match fs.peek_reserved_block(ib) {
            Ok(b) => b,
            Err(e) => {
                walk.bad_ids.push(format!("IB block {ib} unreadable: {e}"));
                continue;
            }
        };
        if rd_u16(&blk, 0) != ID_INDEXBLOCK {
            walk.bad_ids.push(format!("IB block {ib}: bad block id"));
            continue;
        }
        for i in 0..ipb {
            let o = 12 + i * 4;
            if o + 4 > blk.len() {
                break;
            }
            let ab = rd_u32(&blk, o);
            if ab != 0 {
                mark_reserved_sector(fs, walk, ab, "anodeblock (AB)");
            }
        }
    }
    Ok(())
}

/// Walk one directory's dirblock chain, marking each dirblock's reserved
/// cluster and returning the children parsed from every dirblock.
fn scan_directory<R: std::io::Read + std::io::Seek>(
    fs: &mut Pfs3Filesystem<R>,
    dir_anode: u32,
    walk: &mut Walk,
) -> Vec<super::pfs3::Pfs3Child> {
    let rscluster = fs.rscluster();
    let mut children = Vec::new();
    let mut visited: HashSet<u32> = HashSet::new();
    let mut anode = match fs.peek_anode(dir_anode) {
        Ok(a) => a,
        Err(e) => {
            walk.broken_chains
                .push(format!("directory anode {dir_anode} unreadable: {e}"));
            return children;
        }
    };
    let mut guard = 0u32;
    loop {
        if anode.blocknr == 0 || anode.blocknr == 0xFFFF_FFFF {
            break;
        }
        let cluster_sectors = anode.clustersize.saturating_mul(rscluster);
        let mut offset = 0u32;
        while offset < cluster_sectors {
            let sec = anode.blocknr.saturating_add(offset);
            mark_reserved_sector(fs, walk, sec, "dirblock (DB)");
            match fs.peek_reserved_block(sec) {
                Ok(blk) => {
                    if blk.len() > 20 && rd_u16(&blk, 0) == ID_DIRBLOCK {
                        children.extend(fs.parse_dir_children(&blk));
                    } else {
                        walk.bad_ids
                            .push(format!("dirblock at sector {sec}: expected DB id"));
                    }
                }
                Err(e) => walk
                    .bad_ids
                    .push(format!("dirblock at sector {sec} unreadable: {e}")),
            }
            offset += rscluster;
        }
        if anode.next == 0 {
            break;
        }
        if !visited.insert(anode.next) {
            walk.broken_chains.push(format!(
                "directory anode chain cycle at anode {}",
                anode.next
            ));
            break;
        }
        guard += 1;
        if guard > 1_000_000 {
            walk.broken_chains
                .push("directory anode chain exceeds 1M links — likely corrupt".into());
            break;
        }
        anode = match fs.peek_anode(anode.next) {
            Ok(a) => a,
            Err(e) => {
                walk.broken_chains
                    .push(format!("directory anode {} unreadable: {e}", anode.next));
                break;
            }
        };
    }
    children
}

/// Walk a file/softlink anode chain, marking every data sector it covers.
fn mark_file_data<R: std::io::Read + std::io::Seek>(
    fs: &mut Pfs3Filesystem<R>,
    file_anode: u32,
    name: &str,
    walk: &mut Walk,
) {
    let data_start = fs.data_bitmap_start();
    let data_sectors = fs.total_sectors().saturating_sub(data_start);
    let mut visited: HashSet<u32> = HashSet::new();
    let mut anode = match fs.peek_anode(file_anode) {
        Ok(a) => a,
        Err(e) => {
            walk.broken_chains
                .push(format!("file '{name}' anode {file_anode} unreadable: {e}"));
            return;
        }
    };
    let mut guard = 0u32;
    loop {
        if anode.blocknr == 0 || anode.blocknr == 0xFFFF_FFFF {
            break;
        }
        if anode.clustersize as u64 > data_sectors as u64 {
            walk.out_of_range.push(format!(
                "file '{name}': anode clustersize {} exceeds the data area",
                anode.clustersize
            ));
            break;
        }
        for s in 0..anode.clustersize {
            let sec = anode.blocknr.saturating_add(s);
            if sec < data_start {
                walk.out_of_range.push(format!(
                    "file '{name}': data sector {sec} lies in the reserved area"
                ));
                break;
            }
            let local = sec - data_start;
            if local < data_sectors {
                walk.data_alloc.set(local);
            } else {
                walk.out_of_range.push(format!(
                    "file '{name}': data sector {sec} is past the end of the volume"
                ));
                break;
            }
        }
        if anode.next == 0 {
            break;
        }
        if !visited.insert(anode.next) {
            walk.broken_chains.push(format!(
                "file '{name}': anode chain cycle at {}",
                anode.next
            ));
            break;
        }
        guard += 1;
        if guard > 1_000_000 {
            walk.broken_chains
                .push(format!("file '{name}': anode chain exceeds 1M links"));
            break;
        }
        anode = match fs.peek_anode(anode.next) {
            Ok(a) => a,
            Err(e) => {
                walk.broken_chains.push(format!(
                    "file '{name}': anode {} unreadable: {e}",
                    anode.next
                ));
                break;
            }
        };
    }
}

/// Run the full reachability walk over the volume.
fn analyze<R: std::io::Read + std::io::Seek>(
    fs: &mut Pfs3Filesystem<R>,
) -> Result<Walk, FilesystemError> {
    let data_start = fs.data_bitmap_start();
    let data_sectors = fs.total_sectors().saturating_sub(data_start);
    let numreserved = fs.numreserved();
    let mut walk = Walk {
        data_alloc: BitSet::new(data_sectors),
        reserved_alloc: BitSet::new(numreserved),
        files_checked: 0,
        directories_checked: 0,
        bad_ids: Vec::new(),
        bad_types: Vec::new(),
        out_of_range: Vec::new(),
        broken_chains: Vec::new(),
    };

    enumerate_reserved_structure(fs, &mut walk)?;

    // BFS over the directory tree from the root anode.
    let mut queue: VecDeque<u32> = VecDeque::new();
    let mut visited: HashSet<u32> = HashSet::new();
    queue.push_back(ANODE_ROOTDIR);
    visited.insert(ANODE_ROOTDIR);
    // Upper bound: each directory owns at least one reserved dirblock cluster,
    // so more directories than reserved clusters means a cycle we missed.
    let dir_cap = numreserved.saturating_add(16);

    while let Some(dir) = queue.pop_front() {
        walk.directories_checked = walk.directories_checked.saturating_add(1);
        if walk.directories_checked > dir_cap {
            walk.broken_chains
                .push("directory count exceeds reserved capacity — likely a cycle".into());
            break;
        }
        let children = scan_directory(fs, dir, &mut walk);
        for child in children {
            match child.kind {
                Pfs3ChildKind::Dir => {
                    if visited.insert(child.anode) {
                        queue.push_back(child.anode);
                    }
                }
                Pfs3ChildKind::File => {
                    walk.files_checked = walk.files_checked.saturating_add(1);
                    mark_file_data(fs, child.anode, &child.name, &mut walk);
                }
                Pfs3ChildKind::Softlink => {
                    mark_file_data(fs, child.anode, &child.name, &mut walk);
                }
                // Hardlinks (ST_LINKFILE / ST_LINKDIR) reference a primary
                // object reached elsewhere in the tree; their own anode lives
                // in an already-enumerated AB block, and they own no data.
                Pfs3ChildKind::LinkFile | Pfs3ChildKind::LinkDir => {}
                Pfs3ChildKind::Unknown(t) => walk
                    .bad_types
                    .push(format!("entry '{}': unknown ST type {t}", child.name)),
            }
        }
    }

    Ok(walk)
}

/// Compute `(missing, extra)` between an observed allocation set and the
/// on-disk **free** bitmap (`stored_free` bit set = free). `missing` = blocks
/// reachable but marked free; `extra` = blocks marked allocated but not
/// reachable.
fn diff_against_free(observed: &BitSet, stored_free: &[u8], len: u32) -> (u32, u32) {
    let mut missing = 0u32;
    let mut extra = 0u32;
    for idx in 0..len {
        let obs_alloc = observed.get(idx);
        let sto_free = stored_free
            .get((idx / 8) as usize)
            .map(|b| (b >> (idx % 8)) & 1 == 1)
            .unwrap_or(false);
        let sto_alloc = !sto_free;
        match (obs_alloc, sto_alloc) {
            (true, false) => missing += 1,
            (false, true) => extra += 1,
            _ => {}
        }
    }
    (missing, extra)
}

/// Run a non-mutating filesystem check against `fs`.
pub fn check_pfs3<R: std::io::Read + std::io::Seek + Send>(
    fs: &mut Pfs3Filesystem<R>,
) -> Result<FsckResult, FilesystemError> {
    let mut errors = Vec::new();
    let mut warnings = Vec::new();

    let data_sectors = fs.total_sectors().saturating_sub(fs.data_bitmap_start());
    let numreserved = fs.numreserved();
    let walk = analyze(fs)?;
    let clean_walk = walk.structurally_clean();

    // Phase 1/2 structural findings — never auto-repaired.
    push_bucket(&mut errors, Pfs3FsckCode::BadBlockId, &walk.bad_ids);
    push_bucket(&mut errors, Pfs3FsckCode::BadType, &walk.bad_types);
    push_bucket(&mut errors, Pfs3FsckCode::OutOfRange, &walk.out_of_range);
    push_bucket(&mut errors, Pfs3FsckCode::BrokenChain, &walk.broken_chains);

    // Phase 3: bitmap reconciliation.
    let stored_data = fs.read_data_bitmap()?;
    let (data_missing, data_extra) =
        diff_against_free(&walk.data_alloc, &stored_data, data_sectors);
    let data_clean = data_missing == 0 && data_extra == 0;
    if !data_clean {
        errors.push(issue(
            Pfs3FsckCode::DataBitmapMismatch,
            format!(
                "data bitmap mismatch: {data_missing} sector(s) in use but marked free, \
                 {data_extra} sector(s) marked in use but unreachable"
            ),
            clean_walk,
        ));
    }

    let stored_reserved = fs.read_reserved_bitmap()?;
    let (res_missing, res_extra) =
        diff_against_free(&walk.reserved_alloc, &stored_reserved, numreserved);
    let reserved_clean = res_missing == 0 && res_extra == 0;
    if !reserved_clean {
        errors.push(issue(
            Pfs3FsckCode::ReservedBitmapMismatch,
            format!(
                "reserved bitmap mismatch: {res_missing} cluster(s) in use but marked free, \
                 {res_extra} cluster(s) marked in use but unreachable"
            ),
            clean_walk,
        ));
    }

    // Phase 4: free-counter reconciliation.
    let observed_data_free = data_sectors.saturating_sub(walk.data_alloc.count_set());
    if fs.blocks_free() != observed_data_free {
        errors.push(issue(
            Pfs3FsckCode::BlocksFreeMismatch,
            format!(
                "rootblock blocks_free = {} but the data bitmap accounts for {} free sector(s)",
                fs.blocks_free(),
                observed_data_free
            ),
            clean_walk,
        ));
    }
    let observed_reserved_free = numreserved.saturating_sub(walk.reserved_alloc.count_set());
    if fs.reserved_free() != observed_reserved_free {
        errors.push(issue(
            Pfs3FsckCode::ReservedFreeMismatch,
            format!(
                "rootblock reserved_free = {} but {} reserved cluster(s) are free",
                fs.reserved_free(),
                observed_reserved_free
            ),
            clean_walk,
        ));
    }

    // Informational: blocks marked allocated but not reached by the walk.
    if data_extra > 0 || res_extra > 0 {
        warnings.push(issue(
            Pfs3FsckCode::OrphanBlock,
            format!(
                "{data_extra} data sector(s) and {res_extra} reserved cluster(s) are marked \
                 allocated but were not reached from the root"
            ),
            false,
        ));
    }

    let repairable = errors.iter().any(|e| e.repairable);
    let mut stats = FsckStats {
        files_checked: walk.files_checked,
        directories_checked: walk.directories_checked,
        extra: Vec::new(),
    };
    stats
        .extra
        .push(("Volume".to_string(), fs.disk_name_owned()));
    stats.extra.push((
        "Data blocks".to_string(),
        format!("{} free / {} total", observed_data_free, data_sectors),
    ));
    stats.extra.push((
        "Reserved blocks".to_string(),
        format!("{} free / {} total", observed_reserved_free, numreserved),
    ));
    stats.extra.push((
        "Bitmaps".to_string(),
        if data_clean && reserved_clean {
            "consistent".to_string()
        } else {
            "need rebuild".to_string()
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

/// Repair entry point. Rewrites both bitmaps + restamps both free counters
/// from the observed allocation set, but only when the structural walk was
/// clean; otherwise withholds the rewrite and reports the structural damage.
pub fn repair_pfs3<R: std::io::Read + std::io::Write + std::io::Seek + Send>(
    fs: &mut Pfs3Filesystem<R>,
) -> Result<RepairReport, FilesystemError> {
    use super::filesystem::EditableFilesystem;

    let mut fixes_applied = Vec::new();
    let mut fixes_failed = Vec::new();

    let data_sectors = fs.total_sectors().saturating_sub(fs.data_bitmap_start());
    let numreserved = fs.numreserved();
    let walk = analyze(fs)?;
    let unrepairable_count = walk.structural_error_count();

    if !walk.structurally_clean() {
        // Structural damage means the reachability set may be incomplete;
        // rewriting the bitmaps could free live metadata. Refuse.
        return Ok(RepairReport {
            fixes_applied,
            fixes_failed,
            unrepairable_count,
        });
    }

    // Is anything actually out of sync? If not, there's nothing to do.
    let stored_data = fs.read_data_bitmap()?;
    let (dm, de) = diff_against_free(&walk.data_alloc, &stored_data, data_sectors);
    let stored_reserved = fs.read_reserved_bitmap()?;
    let (rm, re) = diff_against_free(&walk.reserved_alloc, &stored_reserved, numreserved);
    let observed_data_free = data_sectors.saturating_sub(walk.data_alloc.count_set());
    let observed_reserved_free = numreserved.saturating_sub(walk.reserved_alloc.count_set());
    let counters_ok =
        fs.blocks_free() == observed_data_free && fs.reserved_free() == observed_reserved_free;
    if dm == 0 && de == 0 && rm == 0 && re == 0 && counters_ok {
        return Ok(RepairReport {
            fixes_applied,
            fixes_failed,
            unrepairable_count,
        });
    }

    // Snapshot the observed sets before we hand them to the rewriters.
    let data_bytes = walk.data_alloc.as_bytes().to_vec();
    let reserved_bytes = walk.reserved_alloc.as_bytes().to_vec();
    drop(walk);

    let data_diff = dm + de;
    let res_diff = rm + re;
    match fs.rewrite_data_bitmap_from(&data_bytes) {
        Ok(()) => fixes_applied.push(format!(
            "rebuilt data bitmap from filesystem walk ({data_diff} bit(s) corrected); \
             blocks_free set to {observed_data_free}"
        )),
        Err(e) => fixes_failed.push(format!("data bitmap rewrite failed: {e}")),
    }
    match fs.rewrite_reserved_bitmap_from(&reserved_bytes) {
        Ok(()) => fixes_applied.push(format!(
            "rebuilt reserved bitmap from filesystem walk ({res_diff} bit(s) corrected); \
             reserved_free set to {observed_reserved_free}"
        )),
        Err(e) => fixes_failed.push(format!("reserved bitmap rewrite failed: {e}")),
    }

    if fixes_applied.is_empty() {
        return Ok(RepairReport {
            fixes_applied,
            fixes_failed,
            unrepairable_count,
        });
    }

    if let Err(e) = fs.sync_metadata() {
        fixes_failed.push(format!("bitmap rewrite flushed with error: {e}"));
    }

    Ok(RepairReport {
        fixes_applied,
        fixes_failed,
        unrepairable_count,
    })
}

/// Turn a bucket of structural findings into `FsckIssue` errors, capping the
/// count so a pathologically corrupt volume can't produce an unbounded list.
fn push_bucket(errors: &mut Vec<FsckIssue>, code: Pfs3FsckCode, msgs: &[String]) {
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
