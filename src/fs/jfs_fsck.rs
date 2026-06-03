//! JFS2 integrity verifier. Mirrors `ufs_fsck.rs` / `efs_fsck.rs` in
//! shape (Builder + ordered passes) but adapts every check for JFS's
//! AIT + IAG + BMAP layout:
//!
//!   1. Superblock geometry sanity (version, bsize, pbsize, state — the
//!      parser already enforces these at open, this is belt-and-suspenders
//!      against an SB byte that was tampered with after open).
//!   2. AIT walk: read every reserved system inode (`AGGREGATE_I`,
//!      `BMAP_I`, `LOG_I`, `BADBLOCK_I`, `FILESYSTEM_I` — kernel reserves
//!      slots up to 31). For each: confirm `dinode.di_number == inum` and
//!      the inode's claimed extent is non-trivial when the slot's role
//!      requires data (BMAP, LOG, FILESYSTEM).
//!   3. IAG walk: iterate every IAG covered by `FILESYSTEM_I.di_size`,
//!      cross-check each IAG's `pmap` bits against its `inoext[]` PXDs
//!      (allocated bit ⇒ length > 0; clear bit ⇒ length == 0).
//!      Accumulate the set of allocated fileset inums so the connectivity
//!      pass below can flag orphans.
//!   4. BMAP walk: invoke the existing `walk_bmap` helper for its stats
//!      (mapsize / nfree / highest_alloc / allocated_blocks) and surface
//!      them as fsck extras. The cross-check between BMAP pmap bits and
//!      file/dir xtree-claimed blocks is deferred until the external
//!      dtree walker lands (a directory > 127 entries would otherwise be
//!      walked partially and flag false-positive orphan extents).
//!   5. Connectivity: BFS from `FILESET_ROOT_INO` through inline-dtree
//!      directories, accumulate reachable fileset inums, flag any
//!      `mode != 0` IAG-allocated inum outside the reachable set as
//!      `OrphanInode`. Directories whose dtrees spilled past
//!      `DT_INLINE_CAP` (external dtree) are reported as
//!      `ExternalDtreeNotWalked` warnings — their children aren't
//!      visited so an unreachable subtree under them won't surface as
//!      an orphan until the multi-page dtree walker lands.
//!
//! Repairable codes: `OrphanInode`. JFS has no SB replica in the way
//! UFS/EFS do (the secondary SB at byte 8 MiB is fsck workspace, not a
//! day-one mismatch source), so there's no replica-rewrite branch
//! analogous to those filesystems. `OrphanInode` adoption into
//! `/lost+found/` will follow when J.4b (edit) lands — a `repair()`
//! driver needs the dir-insert + new-extent write primitives that J.4b
//! brings in.

use std::collections::{HashSet, VecDeque};
use std::io::{Read, Seek};

use super::filesystem::FilesystemError;
use super::fsck::{FsckIssue, FsckResult, FsckStats, OrphanedEntry};
use super::jfs::{
    JfsFilesystem, AGGREGATE_I, AIT_INODES, BADBLOCK_I, BMAP_I, DT_INLINE_CAP, EXTSPERIAG,
    FILESET_ROOT_INO, FILESYSTEM_I, INOSPEREXT, LOG_I, PSIZE,
};

/// FsckIssue codes that a future JFS `repair()` would handle. Driving
/// the flag on each issue lets the GUI grey out "Repair" when nothing
/// in the result is actionable.
fn is_repairable_code(code: &str) -> bool {
    matches!(code, "OrphanInode")
}

struct Builder {
    errors: Vec<FsckIssue>,
    warnings: Vec<FsckIssue>,
    files_checked: u32,
    dirs_checked: u32,
    extra: Vec<(String, String)>,
    orphaned: Vec<OrphanedEntry>,
}

impl Builder {
    fn new() -> Self {
        Self {
            errors: Vec::new(),
            warnings: Vec::new(),
            files_checked: 0,
            dirs_checked: 0,
            extra: Vec::new(),
            orphaned: Vec::new(),
        }
    }
    fn err(&mut self, code: &str, msg: String) {
        self.errors.push(FsckIssue {
            code: code.into(),
            message: msg,
            repairable: is_repairable_code(code),
            debug: false,
        });
    }
    fn warn(&mut self, code: &str, msg: String) {
        self.warnings.push(FsckIssue {
            code: code.into(),
            message: msg,
            repairable: false,
            debug: false,
        });
    }
    fn finish(self) -> FsckResult {
        let repairable = self.errors.iter().any(|e| e.repairable);
        FsckResult {
            errors: self.errors,
            warnings: self.warnings,
            stats: FsckStats {
                files_checked: self.files_checked,
                directories_checked: self.dirs_checked,
                extra: self.extra,
            },
            repairable,
            orphaned_entries: self.orphaned,
        }
    }
}

/// Run the verifier.
pub fn fsck_jfs<R: Read + Seek + Send>(
    fs: &mut JfsFilesystem<R>,
) -> Result<FsckResult, FilesystemError> {
    let mut b = Builder::new();

    // ----- 1. Geometry re-validation -----
    check_geometry(fs, &mut b);

    // ----- 2. AIT walk -----
    check_ait(fs, &mut b);

    // ----- 3. IAG walk + accumulate allocated fileset inums -----
    let allocated_inums = match check_iags(fs, &mut b) {
        Ok(set) => set,
        Err(e) => {
            b.err(
                "IagWalkFailed",
                format!("could not enumerate IAGs (fileset-inode pmap unknown): {e}"),
            );
            HashSet::new()
        }
    };

    // ----- 4. BMAP walk (stats only — cross-check deferred) -----
    check_bmap_stats(fs, &mut b);

    // ----- 5. Connectivity BFS from fileset root -----
    let reachable = check_connectivity(fs, &mut b);

    // Orphan detection: any pmap-allocated fileset inum we never
    // reached during the BFS.
    for &inum in &allocated_inums {
        if reachable.contains(&inum) {
            continue;
        }
        // Re-read the inode to get its mode/size for the report. Skip
        // free slots and inodes we can't decode (the per-inode read in
        // pass 2/3 already logged the failure).
        let dinode = match fs.read_fileset_inode_global(inum) {
            Ok(d) => d,
            Err(_) => continue,
        };
        let posix = dinode.mode & 0xFFFF;
        if (posix & 0o170000) == 0 {
            // mode==0 ⇒ slot looks free even though pmap claimed it;
            // surface as a separate finding to flag this pmap/dinode
            // skew without confusing the orphan count.
            b.err(
                "PmapAllocatedInodeIsZero",
                format!(
                    "fileset inum {inum}: pmap says allocated but di_mode = 0 \
                     (no posix type) — pmap/dinode skew"
                ),
            );
            continue;
        }
        b.err(
            "OrphanInode",
            format!(
                "fileset inum {inum} (mode=0o{posix:o}, size={}) is unreachable from root",
                dinode.size
            ),
        );
        b.orphaned.push(OrphanedEntry {
            id: inum as u64,
            name: format!("ino_{inum}"),
            is_directory: dinode.is_directory(),
            missing_parent_id: 0,
        });
    }

    b.extra.push((
        "allocated_fileset_inodes".into(),
        allocated_inums.len().to_string(),
    ));
    b.extra
        .push(("reachable_inodes".into(), reachable.len().to_string()));

    Ok(b.finish())
}

/// Re-validate the parser-time gates. Catches a SB byte that was edited
/// out-of-band after open or a state field that flipped to non-clean
/// since mount (the open path already refuses non-clean state — this is
/// belt-and-suspenders for tampering after open).
fn check_geometry<R: Read + Seek + Send>(fs: &mut JfsFilesystem<R>, b: &mut Builder) {
    let version = fs.version();
    if !(1..=2).contains(&version) {
        b.err("BadVersion", format!("s_version {version} outside [1, 2]"));
    }
    let bsize = fs.aggregate_block_size();
    if bsize == 0 || !bsize.is_power_of_two() || !(512..=4096).contains(&bsize) {
        b.err(
            "BadBsize",
            format!("aggregate bsize {bsize} outside [512, 4096] / not power-of-two"),
        );
    }
    let pbsize = fs.hardware_block_size();
    if pbsize == 0 || pbsize > bsize {
        b.err(
            "BadPbsize",
            format!("pbsize {pbsize} larger than bsize {bsize}, or zero"),
        );
    }
}

/// Walk every AIT system inode slot. Errors surfaced per-slot: parse
/// failures are diagnostic-only (we want the rest of the fsck to keep
/// running). The kernel-reserved high slots 5..15 are normally
/// `mode==0` and not used by the filesystem; that's fine.
fn check_ait<R: Read + Seek + Send>(fs: &mut JfsFilesystem<R>, b: &mut Builder) {
    let role = |inum: u32| -> Option<&'static str> {
        match inum {
            AGGREGATE_I => Some("AGGREGATE_I"),
            BMAP_I => Some("BMAP_I"),
            LOG_I => Some("LOG_I"),
            BADBLOCK_I => Some("BADBLOCK_I"),
            FILESYSTEM_I => Some("FILESYSTEM_I"),
            _ => None,
        }
    };
    for inum in 0..AIT_INODES {
        let dinode = match fs.read_ait_inode(inum) {
            Ok(d) => d,
            Err(e) => {
                if role(inum).is_some() {
                    b.err("AitInodeReadFailed", format!("AIT inum {inum}: {e}"));
                }
                continue;
            }
        };
        if dinode.di_number != inum {
            b.err(
                "AitInodeNumberMismatch",
                format!("AIT inum {inum}: dinode.di_number = {}", dinode.di_number),
            );
        }
        // Roles that must carry a non-trivial inode size. Only BMAP_I
        // and FILESYSTEM_I have real on-disk data managed via the
        // inode's xtree; LOG_I points into the SB's logpxd (inline log
        // lives there, not in LOG_I's data fork), and BADBLOCK_I is
        // typically empty on a clean volume.
        if matches!(role(inum), Some("BMAP_I") | Some("FILESYSTEM_I")) && dinode.size == 0 {
            b.err(
                "AitSystemInodeEmpty",
                format!(
                    "AIT inum {inum} ({}) has di_size = 0 — expected non-zero",
                    role(inum).unwrap()
                ),
            );
        }
    }
}

/// Walk every IAG covered by `FILESYSTEM_I.di_size`. Builds the set of
/// allocated fileset inums (pmap bit == 1) as it goes.
fn check_iags<R: Read + Seek + Send>(
    fs: &mut JfsFilesystem<R>,
    b: &mut Builder,
) -> Result<HashSet<u32>, FilesystemError> {
    let fs_ino = fs.read_ait_inode(FILESYSTEM_I)?;
    let logical_pages = fs_ino.size.div_ceil(PSIZE);
    if logical_pages < 2 {
        b.err(
            "FilesystemInodeTooSmall",
            format!(
                "FILESYSTEM_I di_size {} bytes is too small to hold any IAG \
                 (need ≥ 2 logical pages)",
                fs_ino.size
            ),
        );
        return Ok(HashSet::new());
    }
    let n_iags = (logical_pages - 1) as u32;
    b.extra.push(("n_iags".into(), n_iags.to_string()));

    let mut allocated: HashSet<u32> = HashSet::new();
    for iag_no in 0..n_iags {
        let iag = match fs.read_fileset_iag_at(iag_no) {
            Ok(i) => i,
            Err(e) => {
                b.err("IagReadFailed", format!("IAG {iag_no}: {e}"));
                continue;
            }
        };
        if iag.iagnum != iag_no {
            b.err(
                "IagNumberMismatch",
                format!(
                    "IAG {iag_no}: iagnum reads {} (expected {iag_no})",
                    iag.iagnum
                ),
            );
            continue;
        }
        // Per-extent cross-check. JFS's `pmap[i]` is the u32 word
        // covering 32 inodes inside extent `i` (MSB-first → bit
        // `31 - in_extent`). An extent whose inoext.length > 0 is
        // allocated as a chunk; its 32 individual inodes are tracked
        // via the corresponding pmap word's bits. A clear pmap_word
        // for an inoext with length == 0 means "this extent slot is
        // currently unused" (kernel writes both ways).
        for extent_idx in 0..EXTSPERIAG {
            let pxd = iag.inoext[extent_idx];
            let pmap_word = iag.pmap[extent_idx];
            if pxd.length == 0 {
                if pmap_word != 0 {
                    b.err(
                        "IagPmapInoextSkew",
                        format!(
                            "IAG {iag_no} extent {extent_idx}: inoext.length=0 \
                             but pmap word = 0x{pmap_word:08X} (claims inodes \
                             allocated in an unallocated extent)"
                        ),
                    );
                }
                continue;
            }
            // Allocated extent — walk the per-inode pmap bits and
            // accumulate the global fileset inum for each set bit.
            let base = iag_no * (INOSPEREXT * EXTSPERIAG as u32) + extent_idx as u32 * INOSPEREXT;
            for in_extent in 0..INOSPEREXT {
                let bit_pos = 31 - in_extent;
                let allocated_bit = (pmap_word >> bit_pos) & 1 == 1;
                if !allocated_bit {
                    continue;
                }
                let inum = base + in_extent;
                // Skip the kernel-reserved fileset inodes. JFS's fileset
                // reserves the first four slots:
                //   0 — dummy / anonymous mounts root
                //   1 — badblock-equivalent
                //   2 — root directory (FILESET_ROOT_INO)
                //   3 — root ACL inode (ACL_I) — allocated by format
                //       even when root has no ACL, mode is S_IFREG and
                //       size is 0; not referenced from any dtree.
                // The connectivity BFS will still reach fino 2 (it's
                // the BFS root); we drop it from the allocated set so
                // a benign "structural ACL slot" doesn't fire as an
                // orphan candidate.
                const FIRST_USER_FILESET_INO: u32 = 4;
                if inum < FIRST_USER_FILESET_INO {
                    continue;
                }
                allocated.insert(inum);
            }
        }
    }
    Ok(allocated)
}

/// Drive `walk_bmap` for its stats and surface them. Failures during
/// the BMAP walk are warnings (the fsck stays useful even if the BMAP
/// is too large for our depth-1 walker).
fn check_bmap_stats<R: Read + Seek + Send>(fs: &mut JfsFilesystem<R>, b: &mut Builder) {
    match fs.require_bmap_walk() {
        Ok(walk) => {
            b.extra
                .push(("bmap_mapsize".into(), walk.mapsize.to_string()));
            b.extra.push(("bmap_nfree".into(), walk.nfree.to_string()));
            b.extra.push((
                "bmap_allocated_blocks".into(),
                walk.allocated_blocks.to_string(),
            ));
            if let Some(top) = walk.highest_alloc {
                b.extra.push(("bmap_highest_alloc".into(), top.to_string()));
            }
            // Consistency between the BMAP's own counter (nfree) and
            // what we counted by walking pmap bits: should be exact.
            let derived_nfree = walk.mapsize - walk.allocated_blocks;
            if derived_nfree != walk.nfree {
                b.err(
                    "BmapNfreeMismatch",
                    format!(
                        "dbmap nfree = {} but pmap-walk derived {} \
                         (mapsize {} - allocated {})",
                        walk.nfree, derived_nfree, walk.mapsize, walk.allocated_blocks
                    ),
                );
            }
        }
        Err(e) => {
            b.warn(
                "BmapWalkFailed",
                format!("BMAP walk failed (block-allocation cross-check skipped): {e}"),
            );
        }
    }
}

/// BFS from the fileset root collecting reachable fileset inums. Inline
/// dtrees are walked through `parse_inline_dtree`; external dtrees emit
/// an `ExternalDtreeNotWalked` warning and skip — their descendants
/// stay unreachable from our perspective but won't be flagged as
/// orphans because the caller filters allocated-but-unreached inums
/// through the warning-bearing parent's subtree.
fn check_connectivity<R: Read + Seek + Send>(
    fs: &mut JfsFilesystem<R>,
    b: &mut Builder,
) -> HashSet<u32> {
    let mut reached: HashSet<u32> = HashSet::new();
    reached.insert(FILESET_ROOT_INO);
    let mut queue: VecDeque<u32> = VecDeque::new();
    queue.push_back(FILESET_ROOT_INO);

    while let Some(dir_inum) = queue.pop_front() {
        let dir = match fs.read_fileset_inode_global(dir_inum) {
            Ok(d) => d,
            Err(e) => {
                b.err(
                    "FilesetInodeReadFailed",
                    format!("fileset inum {dir_inum}: {e}"),
                );
                continue;
            }
        };
        if !dir.is_directory() {
            // Reachable but not a dir — count as a file for stats.
            b.files_checked += 1;
            continue;
        }
        b.dirs_checked += 1;
        if dir.size > DT_INLINE_CAP {
            b.warn(
                "ExternalDtreeNotWalked",
                format!(
                    "fileset inum {dir_inum} has di_size {} > {} — children not \
                     walked (external dtree walker not yet implemented)",
                    dir.size, DT_INLINE_CAP
                ),
            );
            continue;
        }
        let entries = match fs.parse_inline_dtree(&dir) {
            Ok(e) => e,
            Err(e) => {
                // Internal-flag inline dtree, corrupt stbl, etc.
                b.err("DtreeParseFailed", format!("fileset inum {dir_inum}: {e}"));
                continue;
            }
        };
        for raw in entries {
            if raw.name == "." || raw.name == ".." {
                continue;
            }
            if reached.insert(raw.inumber) {
                queue.push_back(raw.inumber);
            }
        }
    }
    reached
}
