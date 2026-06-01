//! XFS integrity verifier (read-only).
//!
//! This is the check side of the XFS edit + repair plan
//! (`docs/xfs_edit_and_repair.md`). It produces the shared
//! `crate::fs::fsck` types so the GUI and trait layer stay
//! filesystem-agnostic, mirroring `efs_fsck` / `hfs_fsck`.
//!
//! Phasing (see the doc):
//!   * **Phase 1 (this commit)** — superblock geometry, per-AG superblock
//!     replica consistency, and AGF/AGI parse + bounds checks. Pure
//!     fixed-location-metadata reads; no inode or btree walk.
//!   * Phase 2 — allocation + inode btree walk and block-ownership map.
//!   * Phase 3 — directory connectivity / orphan detection.
//!
//! Porting reference: `xfs_repair` @ v3.1.11 (`repair/agheader.c`,
//! `repair/sb.c`) under `~/efs-xfs-refs/xfsprogs/`. v4 on-disk layout per
//! "XFS Algorithms & Data Structures" 3rd ed.

use std::io::{Read, Seek};

use super::ag::{XfsAgf, XfsAgi};
use super::sb::XfsSuperblock;
use super::{read_at_aligned, XfsFilesystem};
use crate::fs::filesystem::FilesystemError;
use crate::fs::fsck::{FsckIssue, FsckResult, FsckStats, OrphanedEntry};

/// FsckIssue codes the XFS repair path knows how to fix. Phase 1 only
/// emits `ReplicaSbMismatch` as repairable (a per-AG superblock copy can be
/// rewritten verbatim from the authoritative primary, R4). Structural
/// damage to AG headers is surfaced for diagnosis until the rebuild phases
/// land. Later phases extend this set.
fn is_repairable_code(code: &str) -> bool {
    matches!(code, "ReplicaSbMismatch")
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

impl<R: Read + Seek + Send> XfsFilesystem<R> {
    /// Run the XFS verifier and return a shared `FsckResult`.
    pub(crate) fn run_fsck(&mut self) -> Result<FsckResult, FilesystemError> {
        let mut b = Builder::new();
        let sb = self.superblock().clone();

        check_superblock_geometry(&sb, &mut b);
        self.check_ag_headers(&sb, &mut b);

        Ok(b.finish())
    }

    /// Phase 1: per-AG superblock-replica consistency + AGF/AGI sanity.
    ///
    /// Every allocation group begins with a copy of the superblock (sector
    /// 0), then the AGF (sector 1), AGI (sector 2) and AGFL (sector 3),
    /// where "sector" is `sb_sectsize`. We read the first four sectors of
    /// each AG in one aligned read and validate them against the primary.
    fn check_ag_headers(&mut self, sb: &XfsSuperblock, b: &mut Builder) {
        let agcount = sb.agcount as u64;
        let agblocks = sb.agblocks as u64;
        let blocksize = sb.blocksize as u64;
        let sectsize = sb.sectsize as u64;

        let mut total_free_blocks: u64 = 0;
        let mut total_inodes: u64 = 0;
        let mut total_free_inodes: u64 = 0;

        // Header span: 4 sectors (sb / AGF / AGI / AGFL). sectsize is a
        // power of two >= 512, so 4*sectsize is 512-aligned as required by
        // read_at_aligned.
        let span = 4 * sectsize;
        let mut buf = vec![0u8; span as usize];

        for agno in 0..agcount {
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
                continue;
            }

            // Expected block count for this AG: the last AG is short.
            let expected_len = if agno == agcount - 1 {
                sb.dblocks - agno * agblocks
            } else {
                agblocks
            };

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
            match XfsAgi::parse(&buf[agi_off..agi_off + sectsize as usize]) {
                Ok(agi) => {
                    check_agi(&agi, agno, expected_len, b);
                    total_inodes += agi.count as u64;
                    total_free_inodes += agi.freecount as u64;
                }
                Err(e) => b.err("AgiBadMagic", format!("AG {agno} AGI: {e}")),
            }
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
/// Only the geometry fields matter — they must match exactly or the AG is
/// uninterpretable. A mismatch is repairable: R4 rewrites the copy from the
/// primary.
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
    if rep.rootino != primary.rootino {
        report(
            "rootino",
            primary.rootino.to_string(),
            rep.rootino.to_string(),
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
