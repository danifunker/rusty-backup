//! XFS integrity verifier (read-only).
//!
//! This is the check side of the XFS edit + repair plan
//! (`docs/xfs_edit_and_repair.md`). It produces the shared
//! `crate::fs::fsck` types so the GUI and trait layer stay
//! filesystem-agnostic, mirroring `efs_fsck` / `hfs_fsck`.
//!
//! Phasing (see the doc):
//!   * **Phase 1** — superblock geometry, per-AG superblock replica
//!     consistency, and AGF/AGI parse + bounds checks. Pure
//!     fixed-location-metadata reads; no inode or btree walk.
//!   * **Phase 2** — inode-btree walk + block-ownership map (R1): claim
//!     every allocated inode's blocks, flag cross-links / out-of-bounds.
//!   * **Phase 3** — directory connectivity: BFS from root, flag orphaned
//!     (allocated-but-unreachable) inodes and dangling directory entries.
//!
//! Porting reference: `xfs_repair` @ v3.1.11 (`repair/agheader.c`,
//! `repair/sb.c`) under `~/efs-xfs-refs/xfsprogs/`. v4 on-disk layout per
//! "XFS Algorithms & Data Structures" 3rd ed.

use std::collections::HashSet;
use std::io::{Read, Seek};

use byteorder::{BigEndian, ByteOrder};

use super::ag::{XfsAgf, XfsAgi};
use super::sb::XfsSuperblock;
use super::types::{
    NULLAGBLOCK, XFS_BTREE_SBLOCK_CRC_LEN, XFS_BTREE_SBLOCK_LEN, XFS_IBT_CRC_MAGIC, XFS_IBT_MAGIC,
    XFS_INODES_PER_CHUNK,
};
use super::{read_at_aligned, XfsFilesystem};
use crate::fs::filesystem::{Filesystem, FilesystemError};
use crate::fs::fsck::{FsckIssue, FsckResult, FsckStats, OrphanedEntry};

/// FsckIssue codes the XFS repair path knows how to fix.
///
/// `ReplicaSbMismatch` — a per-AG superblock copy can be rewritten verbatim
/// from the authoritative primary (R4). `OrphanInode` — an
/// allocated-but-unreachable inode can be reconnected into `lost+found/`
/// (R7). Structural damage (bad AG headers, cross-links, dangling entries)
/// is surfaced for diagnosis until the rebuild phases land.
fn is_repairable_code(code: &str) -> bool {
    matches!(code, "ReplicaSbMismatch" | "OrphanInode")
}

/// Accumulates findings; keeps the per-check code focused.
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
    fn stat(&mut self, label: &str, value: String) {
        self.extra.push((label.into(), value));
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

/// Per-AG header summary captured in Phase 1 and reused by Phase 2.
struct AgHeaders {
    expected_len: u64,
    /// AG-relative root block of the inode-allocation btree, if the AGI
    /// parsed. `None` means we can't enumerate this AG's inodes.
    agi_root: Option<u32>,
}

impl<R: Read + Seek + Send> XfsFilesystem<R> {
    /// Run the XFS verifier and return a shared `FsckResult`.
    pub(crate) fn run_fsck(&mut self) -> Result<FsckResult, FilesystemError> {
        let mut b = Builder::new();
        let sb = self.superblock().clone();

        check_superblock_geometry(&sb, &mut b);
        let ag_headers = self.check_ag_headers(&sb, &mut b);
        let allocated = self.check_allocations(&sb, &ag_headers, &mut b);
        self.check_connectivity(&sb, &allocated, &mut b);

        Ok(b.finish())
    }

    /// Phase 1: per-AG superblock-replica consistency + AGF/AGI sanity.
    ///
    /// Every allocation group begins with a copy of the superblock (sector
    /// 0), then the AGF (sector 1), AGI (sector 2) and AGFL (sector 3),
    /// where "sector" is `sb_sectsize`. We read the first four sectors of
    /// each AG in one aligned read and validate them against the primary.
    /// Returns per-AG info Phase 2 needs (inobt roots, AG lengths).
    fn check_ag_headers(&mut self, sb: &XfsSuperblock, b: &mut Builder) -> Vec<AgHeaders> {
        let agcount = sb.agcount as u64;
        let agblocks = sb.agblocks as u64;
        let blocksize = sb.blocksize as u64;
        let sectsize = sb.sectsize as u64;

        let mut total_free_blocks: u64 = 0;
        let mut total_inodes: u64 = 0;
        let mut total_free_inodes: u64 = 0;
        let mut out = Vec::with_capacity(agcount as usize);

        // Header span: 4 sectors (sb / AGF / AGI / AGFL). sectsize is a
        // power of two >= 512, so 4*sectsize is 512-aligned as required by
        // read_at_aligned.
        let span = 4 * sectsize;
        let mut buf = vec![0u8; span as usize];

        for agno in 0..agcount {
            // Expected block count for this AG: the last AG is short.
            let expected_len = if agno == agcount - 1 {
                sb.dblocks - agno * agblocks
            } else {
                agblocks
            };

            let ag_start = agno * agblocks * blocksize;
            if let Err(e) = read_at_aligned(
                &mut self.reader,
                self.partition_offset + ag_start,
                span,
                &mut buf,
            ) {
                b.warn(
                    "AgHeaderReadFailed",
                    format!("AG {agno}: could not read headers: {e}"),
                );
                out.push(AgHeaders {
                    expected_len,
                    agi_root: None,
                });
                continue;
            }

            // --- AG superblock replica (sector 0) ---
            match XfsSuperblock::parse(&buf[0..sectsize as usize]) {
                Ok(rep) => check_sb_replica(sb, &rep, agno, b),
                Err(e) => b.err(
                    "ReplicaSbUnreadable",
                    format!("AG {agno} superblock replica did not parse: {e}"),
                ),
            }

            // --- AGF (sector 1) ---
            let agf_off = sectsize as usize;
            match XfsAgf::parse(&buf[agf_off..agf_off + sectsize as usize]) {
                Ok(agf) => {
                    check_agf(&agf, agno, expected_len, b);
                    total_free_blocks += agf.freeblks as u64;
                }
                Err(e) => b.err("AgfBadMagic", format!("AG {agno} AGF: {e}")),
            }

            // --- AGI (sector 2) ---
            let agi_off = 2 * sectsize as usize;
            let agi_root = match XfsAgi::parse(&buf[agi_off..agi_off + sectsize as usize]) {
                Ok(agi) => {
                    check_agi(&agi, agno, expected_len, b);
                    total_inodes += agi.count as u64;
                    total_free_inodes += agi.freecount as u64;
                    Some(agi.root)
                }
                Err(e) => {
                    b.err("AgiBadMagic", format!("AG {agno} AGI: {e}"));
                    None
                }
            };

            out.push(AgHeaders {
                expected_len,
                agi_root,
            });
        }

        b.stat(
            "XFS version",
            if sb.is_v5() { "v5".into() } else { "v4".into() },
        );
        b.stat("Block size", format!("{} bytes", sb.blocksize));
        b.stat("Allocation groups", format!("{agcount}"));
        b.stat(
            "Total blocks",
            format!("{} ({} free)", sb.dblocks, total_free_blocks),
        );
        b.stat(
            "Allocated inodes",
            format!("{total_inodes} ({total_free_inodes} free)"),
        );
        out
    }

    /// Phase 2: block-ownership map (the in-memory rmap substitute, R1 in
    /// `docs/xfs_edit_and_repair.md`). Enumerate every allocated inode via
    /// each AG's inode btree, claim its blocks in a per-volume state map,
    /// and report any block claimed twice (`DoubleAllocation`) or any extent
    /// outside the volume (`ExtentPastVolume`).
    ///
    /// Returns the set of allocated inode numbers (used by Phase 3 for
    /// connectivity / orphan detection).
    fn check_allocations(
        &mut self,
        sb: &XfsSuperblock,
        ags: &[AgHeaders],
        b: &mut Builder,
    ) -> HashSet<u64> {
        let mut allocated: HashSet<u64> = HashSet::new();

        // Bound the in-memory map. Vintage IRIX disks are a few GB (≤ ~1M
        // 4 KiB blocks); refuse only absurd geometries to avoid OOM.
        const MAX_MAP_BLOCKS: u64 = 256 * 1024 * 1024; // 256M blocks (256 MiB map)
        if sb.dblocks > MAX_MAP_BLOCKS {
            b.warn(
                "OwnershipMapSkipped",
                format!(
                    "volume has {} blocks (> {MAX_MAP_BLOCKS}); skipping block-ownership scan",
                    sb.dblocks
                ),
            );
            return allocated;
        }

        let agblocks = sb.agblocks as u64;
        let mut map = OwnerMap::new(sb.dblocks);
        // blocks spanned by one 64-inode chunk.
        let blocks_per_chunk =
            ((XFS_INODES_PER_CHUNK as u64) * sb.inodesize as u64).div_ceil(sb.blocksize as u64);

        for (agno, ag) in ags.iter().enumerate() {
            let agno = agno as u64;
            // AG header block 0 holds sb/AGF/AGI/AGFL.
            map.mark(agno * agblocks, S_INUSE_FS);

            let Some(root) = ag.agi_root else { continue };
            let recs =
                match self.collect_inobt_records(sb, agno, root, ag.expected_len, &mut map, b) {
                    Ok(r) => r,
                    Err(e) => {
                        b.err("InobtWalkFailed", format!("AG {agno} inode btree: {e}"));
                        continue;
                    }
                };

            for rec in recs {
                // Claim the inode-chunk blocks themselves.
                let chunk_agbno = (rec.start_agino as u64) >> sb.inopblog;
                for blk in 0..blocks_per_chunk {
                    map.mark(agno * agblocks + chunk_agbno + blk, S_INO);
                }
                // Visit each allocated inode in the chunk.
                for slot in 0..XFS_INODES_PER_CHUNK {
                    let is_free = (rec.free >> slot) & 1 == 1;
                    if is_free {
                        continue;
                    }
                    let agino = rec.start_agino as u64 + slot as u64;
                    let ino = (agno << (sb.agblklog + sb.inopblog)) | agino;
                    allocated.insert(ino);
                    self.scan_inode_blocks(sb, ino, &mut map, b);
                }
            }
        }

        let mult = map.count_mult();
        b.stat("Blocks scanned", format!("{} claimed", map.count_claimed()));
        if mult > 0 {
            b.stat("Cross-linked blocks", format!("{mult}"));
        }
        allocated
    }

    /// Phase 3: directory connectivity / orphan detection (R7 input).
    ///
    /// BFS from the root inode through the directory tree, collecting every
    /// reachable inode. Any allocated inode not reached is an orphan
    /// (repairable: R7 will reconnect it into lost+found). Directory entries
    /// pointing at inodes the inobt didn't mark allocated are dangling.
    fn check_connectivity(
        &mut self,
        sb: &XfsSuperblock,
        allocated: &HashSet<u64>,
        b: &mut Builder,
    ) {
        if allocated.is_empty() {
            // Phase 2 was skipped or found nothing; orphan analysis needs the
            // allocated set, so there's nothing to compare against.
            return;
        }

        let root = match self.root() {
            Ok(r) => r,
            Err(e) => {
                b.err("RootUnreadable", format!("cannot read root directory: {e}"));
                return;
            }
        };

        let mut reachable: HashSet<u64> = HashSet::new();
        // Seed the superblock-referenced internal inodes (root + realtime
        // bitmap/summary + quota inodes). They're allocated but reached only
        // via the superblock, not the directory tree.
        for ino in sb.internal_inodes() {
            reachable.insert(ino);
        }
        let mut queue = vec![root];
        let mut visited_dirs: HashSet<u64> = HashSet::new();

        while let Some(dir) = queue.pop() {
            if !visited_dirs.insert(dir.location) {
                continue; // already expanded (guards against dir cycles)
            }
            let children = match self.list_directory(&dir) {
                Ok(c) => c,
                Err(e) => {
                    b.err(
                        "DirReadFailed",
                        format!("inode {}: directory unreadable: {e}", dir.location),
                    );
                    continue;
                }
            };
            for child in children {
                if !allocated.contains(&child.location) {
                    b.err(
                        "DanglingEntry",
                        format!(
                            "directory inode {} entry '{}' points at inode {} which is not allocated",
                            dir.location, child.name, child.location
                        ),
                    );
                    continue;
                }
                reachable.insert(child.location);
                if child.is_directory() {
                    queue.push(child);
                }
            }
        }

        // Orphans: allocated but unreachable from root.
        let mut orphans: Vec<u64> = allocated.difference(&reachable).copied().collect();
        orphans.sort_unstable();
        for ino in orphans {
            let is_dir = self.read_inode(ino).map(|c| c.is_dir()).unwrap_or(false);
            b.err(
                "OrphanInode",
                format!("inode {ino} is allocated but unreachable from the root directory"),
            );
            b.orphaned.push(OrphanedEntry {
                id: ino,
                name: format!("inode_{ino}"),
                is_directory: is_dir,
                missing_parent_id: 0, // unknown — XFS orphans have lost their parent link
            });
        }

        b.stat("Reachable inodes", format!("{}", reachable.len()));
    }

    /// Read one allocated inode, count it, and claim its data-fork blocks in
    /// the ownership map. Read or decode failures are reported, not fatal.
    fn scan_inode_blocks(
        &mut self,
        sb: &XfsSuperblock,
        ino: u64,
        map: &mut OwnerMap,
        b: &mut Builder,
    ) {
        let (core, inode_buf) = match self.read_inode_buf(ino) {
            Ok(v) => v,
            Err(e) => {
                b.err("InodeReadFailed", format!("inode {ino}: {e}"));
                return;
            }
        };
        if core.mode == 0 {
            // inobt said allocated but the inode is zeroed — inconsistency.
            b.err(
                "AllocatedInodeZeroed",
                format!("inode {ino} marked allocated in inobt but has mode 0"),
            );
            return;
        }
        if core.is_dir() {
            b.dirs_checked += 1;
        } else {
            b.files_checked += 1;
        }

        // Only extent / btree formats own separate disk blocks. Local
        // (inline dirs, short symlinks, devices) own none.
        use super::types::DiFormat;
        if !matches!(core.format, DiFormat::Extents | DiFormat::Btree) {
            return;
        }
        let extents = match self.decode_data_extents(&core, &inode_buf) {
            Ok(e) => e,
            Err(e) => {
                b.err("ExtentDecodeFailed", format!("inode {ino}: {e}"));
                return;
            }
        };
        for ext in &extents {
            self.claim_extent(sb, ino, ext.startblock, ext.blockcount, map, b);
        }
    }

    /// Claim `[startblock, startblock+count)` (XFS fsblocks) for `ino`,
    /// flagging out-of-bounds extents and cross-links.
    fn claim_extent(
        &self,
        sb: &XfsSuperblock,
        ino: u64,
        startblock: u64,
        count: u64,
        map: &mut OwnerMap,
        b: &mut Builder,
    ) {
        let agblocks = sb.agblocks as u64;
        let agmask = (1u64 << sb.agblklog) - 1;
        let mut collided = false;
        for i in 0..count {
            let fsb = startblock + i;
            let agno = fsb >> sb.agblklog;
            let agbno = fsb & agmask;
            if agno >= sb.agcount as u64 || agbno >= agblocks {
                b.err(
                    "ExtentPastVolume",
                    format!(
                        "inode {ino}: extent block {fsb} (AG {agno}, agbno {agbno}) is outside the volume"
                    ),
                );
                return;
            }
            let linear = agno * agblocks + agbno;
            if map.mark(linear, S_INUSE) {
                collided = true;
            }
        }
        if collided {
            b.err(
                "DoubleAllocation",
                format!(
                    "inode {ino}: extent [{startblock}..{}) overlaps blocks already claimed elsewhere",
                    startblock + count
                ),
            );
        }
    }

    /// Walk an AG's inode-allocation btree, collecting every leaf record and
    /// marking the btree's own blocks as filesystem metadata in `map`.
    fn collect_inobt_records(
        &mut self,
        sb: &XfsSuperblock,
        agno: u64,
        root_agbno: u32,
        expected_len: u64,
        map: &mut OwnerMap,
        b: &mut Builder,
    ) -> Result<Vec<InobtRec>, FilesystemError> {
        let bs = sb.blocksize as usize;
        let hdr = if sb.is_v5() {
            XFS_BTREE_SBLOCK_CRC_LEN
        } else {
            XFS_BTREE_SBLOCK_LEN
        };
        // Internal nodes: key(4) + ptr(4); leaves: rec(16).
        let max_intern = (bs - hdr) / 8;
        let max_leaf = (bs - hdr) / 16;
        let agblocks = sb.agblocks as u64;

        let mut recs = Vec::new();
        let mut block = vec![0u8; bs];
        let mut stack = vec![root_agbno];
        let mut visited: HashSet<u32> = HashSet::new();

        while let Some(agbno) = stack.pop() {
            if agbno == NULLAGBLOCK || agbno as u64 >= expected_len {
                continue;
            }
            if !visited.insert(agbno) {
                b.err(
                    "InobtCycle",
                    format!("AG {agno} inode btree: block {agbno} visited twice"),
                );
                continue;
            }
            let fsblock = (agno << sb.agblklog) | agbno as u64;
            self.read_fsblock(fsblock, &mut block)?;
            map.mark(agno * agblocks + agbno as u64, S_INUSE_FS);

            let magic = BigEndian::read_u32(&block[0..4]);
            if magic != XFS_IBT_MAGIC && magic != XFS_IBT_CRC_MAGIC {
                return Err(FilesystemError::Parse(format!(
                    "bad inode btree magic 0x{magic:08X} at AG {agno} block {agbno}"
                )));
            }
            let level = BigEndian::read_u16(&block[4..6]);
            let numrecs = BigEndian::read_u16(&block[6..8]) as usize;

            if level == 0 {
                if numrecs > max_leaf {
                    return Err(FilesystemError::Parse(format!(
                        "AG {agno} inode btree leaf numrecs {numrecs} > max {max_leaf}"
                    )));
                }
                for i in 0..numrecs {
                    let off = hdr + i * 16;
                    let start_agino = BigEndian::read_u32(&block[off..off + 4]);
                    let free = BigEndian::read_u64(&block[off + 8..off + 16]);
                    recs.push(InobtRec { start_agino, free });
                }
            } else {
                if numrecs > max_intern {
                    return Err(FilesystemError::Parse(format!(
                        "AG {agno} inode btree node numrecs {numrecs} > max {max_intern}"
                    )));
                }
                let ptr_base = hdr + max_intern * 4;
                for i in 0..numrecs {
                    let off = ptr_base + i * 4;
                    stack.push(BigEndian::read_u32(&block[off..off + 4]));
                }
            }
        }
        Ok(recs)
    }
}

/// One inode-allocation btree leaf record: the chunk's first AG-relative
/// inode number and its 64-bit free bitmap (set bit = free).
struct InobtRec {
    start_agino: u32,
    free: u64,
}

// Block-ownership states (subset of xfs_repair's XR_E_*; the rebuild phases
// add the FREE/FREE1 states once the free-space btree walk lands).
const S_UNKNOWN: u8 = 0;
const S_INUSE: u8 = 1; // file/dir data
const S_INUSE_FS: u8 = 2; // AG headers / btree blocks
const S_INO: u8 = 3; // inode chunk blocks
const S_MULT: u8 = 4; // multiply claimed (cross-link)

/// Per-volume block-state map, indexed by partition-linear block number
/// (`agno * agblocks + agbno`). The R1 ownership map from the plan.
struct OwnerMap {
    states: Vec<u8>,
}

impl OwnerMap {
    fn new(dblocks: u64) -> Self {
        Self {
            states: vec![S_UNKNOWN; dblocks as usize],
        }
    }
    /// Claim `linear` for `state`. Returns true if the block was already
    /// claimed (transition to `S_MULT`).
    fn mark(&mut self, linear: u64, state: u8) -> bool {
        let Some(slot) = self.states.get_mut(linear as usize) else {
            return false;
        };
        if *slot == S_UNKNOWN {
            *slot = state;
            false
        } else {
            *slot = S_MULT;
            true
        }
    }
    fn count_claimed(&self) -> usize {
        self.states.iter().filter(|&&s| s != S_UNKNOWN).count()
    }
    fn count_mult(&self) -> usize {
        self.states.iter().filter(|&&s| s == S_MULT).count()
    }
}

/// Phase 1: primary superblock geometry sanity. Most of this is already
/// enforced at `XfsSuperblock::parse`; we re-affirm the relationships that
/// matter for the rest of the checker and would make later phases compute
/// garbage if violated.
fn check_superblock_geometry(sb: &XfsSuperblock, b: &mut Builder) {
    if sb.agcount == 0 {
        b.err("BadAgGeometry", "sb_agcount is 0".into());
        return;
    }
    let agblocks = sb.agblocks as u64;
    let agcount = sb.agcount as u64;

    // The AGs must cover the volume, and all but the last must be full.
    let covered = agblocks * agcount;
    if covered < sb.dblocks {
        b.err(
            "BadAgGeometry",
            format!(
                "agblocks {} * agcount {} = {covered} < dblocks {}",
                sb.agblocks, sb.agcount, sb.dblocks
            ),
        );
    }
    if agcount > 1 && (agcount - 1) * agblocks >= sb.dblocks {
        b.err(
            "BadAgGeometry",
            format!(
                "first {} full AGs already exceed dblocks {} (agblocks {})",
                agcount - 1,
                sb.dblocks,
                sb.agblocks
            ),
        );
    }

    // inopblock must be blocksize / inodesize.
    if sb.inodesize != 0 {
        let expect = sb.blocksize / sb.inodesize as u32;
        if expect as u16 != sb.inopblock {
            b.err(
                "InopblockMismatch",
                format!(
                    "sb_inopblock {} != blocksize {} / inodesize {} = {expect}",
                    sb.inopblock, sb.blocksize, sb.inodesize
                ),
            );
        }
    }

    // Root inode must live in AG 0 (XFS always places it there).
    let root_agno = sb.rootino >> (sb.agblklog + sb.inopblog);
    if root_agno != 0 {
        b.err(
            "RootInoOutOfRange",
            format!(
                "sb_rootino {} resolves to AG {root_agno}, expected AG 0",
                sb.rootino
            ),
        );
    }
}

/// Compare one AG's superblock replica against the authoritative primary.
/// Only the **geometry** fields are replicated and must match exactly, or the
/// AG is uninterpretable. Inode-pointer fields (`rootino`, `rbmino`,
/// `rsumino`, quota inodes) are deliberately **not** compared: XFS only
/// stores them in the primary and leaves the secondaries' copies as
/// NULLFSINO (verified against `xfs_db`: `sb 2` shows `rootino = null` on a
/// clean mkfs'd v4 volume). Comparing them produced a false-positive
/// `ReplicaSbMismatch` that `xfs_repair -n` did not report. A geometry
/// mismatch is repairable: R4 rewrites the copy from the primary.
fn check_sb_replica(primary: &XfsSuperblock, rep: &XfsSuperblock, agno: u64, b: &mut Builder) {
    let mut report = |field: &str, p: String, r: String| {
        b.err(
            "ReplicaSbMismatch",
            format!("AG {agno} superblock: {field} = {r}, primary = {p}"),
        );
    };
    if rep.dblocks != primary.dblocks {
        report(
            "dblocks",
            primary.dblocks.to_string(),
            rep.dblocks.to_string(),
        );
    }
    if rep.agblocks != primary.agblocks {
        report(
            "agblocks",
            primary.agblocks.to_string(),
            rep.agblocks.to_string(),
        );
    }
    if rep.agcount != primary.agcount {
        report(
            "agcount",
            primary.agcount.to_string(),
            rep.agcount.to_string(),
        );
    }
    if rep.blocksize != primary.blocksize {
        report(
            "blocksize",
            primary.blocksize.to_string(),
            rep.blocksize.to_string(),
        );
    }
    if rep.inodesize != primary.inodesize {
        report(
            "inodesize",
            primary.inodesize.to_string(),
            rep.inodesize.to_string(),
        );
    }
}

/// AGF structural + bounds sanity. The free-block *count* is only fully
/// verifiable against a btree walk (Phase 2); here we bound-check it and the
/// btree roots against the AG size.
fn check_agf(agf: &XfsAgf, agno: u64, expected_len: u64, b: &mut Builder) {
    if agf.seqno as u64 != agno {
        b.err(
            "AgfSeqnoMismatch",
            format!("AG {agno} AGF seqno = {}, expected {agno}", agf.seqno),
        );
    }
    if agf.length as u64 != expected_len {
        b.err(
            "AgfLengthMismatch",
            format!(
                "AG {agno} AGF length = {}, expected {expected_len}",
                agf.length
            ),
        );
    }
    for (name, root) in [("bno", agf.bno_root), ("cnt", agf.cnt_root)] {
        if root as u64 >= expected_len {
            b.err(
                "AgfRootOutOfAg",
                format!("AG {agno} AGF {name} btree root block {root} >= AG length {expected_len}"),
            );
        }
    }
    if agf.freeblks as u64 > expected_len {
        b.err(
            "AgfFreeblksBad",
            format!(
                "AG {agno} AGF freeblks {} > AG length {expected_len}",
                agf.freeblks
            ),
        );
    }
}

/// AGI structural + bounds sanity.
fn check_agi(agi: &XfsAgi, agno: u64, expected_len: u64, b: &mut Builder) {
    if agi.seqno as u64 != agno {
        b.err(
            "AgiSeqnoMismatch",
            format!("AG {agno} AGI seqno = {}, expected {agno}", agi.seqno),
        );
    }
    if agi.length as u64 != expected_len {
        b.err(
            "AgiLengthMismatch",
            format!(
                "AG {agno} AGI length = {}, expected {expected_len}",
                agi.length
            ),
        );
    }
    if agi.root as u64 >= expected_len {
        b.err(
            "AgiRootOutOfAg",
            format!(
                "AG {agno} AGI inode btree root block {} >= AG length {expected_len}",
                agi.root
            ),
        );
    }
    if agi.freecount > agi.count {
        b.err(
            "AgiFreecountBad",
            format!(
                "AG {agno} AGI freecount {} > count {}",
                agi.freecount, agi.count
            ),
        );
    }
}
