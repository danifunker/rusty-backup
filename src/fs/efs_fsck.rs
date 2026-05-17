//! EFS integrity verifier. Used both as a standalone check (via
//! `Filesystem::fsck`) and as the final sanity pass after every
//! grow/shrink before the new superblock is sealed. The companion
//! repair driver lives in `efs.rs::repair_efs` (called from
//! `EditableFilesystem::repair`).
//!
//! Checks (in order):
//!   1. Superblock geometry sanity (firstcg + ncg * cgfsize <= fs_size,
//!      bmsize covers volume, replsb inside reasonable bounds).
//!   2. Replica superblock matches the primary in the resize-relevant
//!      fields (fs_size, firstcg, cgfsize, cgisize, ncg, bmsize).
//!   3. Inode table: every `mode != 0` inode parses cleanly, has
//!      numextents <= 12, no extent has a non-zero `magic` byte, and
//!      every extent's [bn, bn+length) lies inside [0, fs_size).
//!   4. Bitmap shadow: count set bits, compare against the sum of all
//!      in-use inodes' allocated extents. Flag double-allocations.
//!   5. Connectivity: BFS from root inum 2; any mode!=0 inode outside
//!      the reachable set is reported as `OrphanInode` and pushed into
//!      `orphaned_entries` so the repair pass can adopt them into
//!      `lost+found/`.
//!
//! Repairable codes (handled by `repair_efs`): Replica*Mismatch,
//! BitmapMissingAllocation, OrphanInode. Unrepairable codes
//! (geometry damage, ExtentPastVolume, DoubleAllocation, TooManyExtents,
//! BitmapTooSmall, InodeReadFailed) are surfaced for diagnosis only.

use std::collections::HashSet;
use std::io::{Read, Seek};

use super::efs::{
    parse_dir_block, EfsFilesystem, EfsSuperblock, EFS_BLOCKSIZE, EFS_DIRECTEXTENTS_MAX,
};
use super::filesystem::FilesystemError;
use super::fsck::{FsckIssue, FsckResult, FsckStats, OrphanedEntry};

/// FsckIssue codes that the EFS `repair()` path knows how to fix.
/// Issues with codes outside this set are emitted with `repairable =
/// false` so the GUI can grey out the "Repair" button when none of the
/// findings are actionable.
fn is_repairable_code(code: &str) -> bool {
    matches!(
        code,
        "ReplicaFsSizeMismatch"
            | "ReplicaFirstCgMismatch"
            | "ReplicaCgfSizeMismatch"
            | "ReplicaCgiSizeMismatch"
            | "ReplicaNcgMismatch"
            | "ReplicaBmsizeMismatch"
            | "ReplicaBmblockMismatch"
            | "ReplicaReplsbMismatch"
            | "BitmapMissingAllocation"
            | "OrphanInode"
    )
}

/// Wraps the boilerplate of building a `FsckResult` so the
/// per-check closures stay focused on findings.
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

/// Run the verifier. Requires read-only access; the editable surface
/// `R: Read + Write + Seek + Send` works fine here because all
/// methods used are inherited from the read-only impl.
pub fn fsck_efs<R: Read + Seek>(fs: &mut EfsFilesystem<R>) -> Result<FsckResult, FilesystemError> {
    let mut b = Builder::new();
    let sb = fs.sb_clone();

    // --- Step 1: superblock geometry ---
    check_geometry(&sb, &mut b);

    // --- Step 2: replica matches primary ---
    if let Err(e) = check_replica(fs, &sb, &mut b) {
        // I/O errors reading the replica are warnings, not fatal —
        // a truncated fixture can hit this.
        b.warn("ReplicaReadFailed", format!("could not read replica: {e}"));
    }

    // --- Step 3 + 4: inode + bitmap scan ---
    let bitmap = fs.read_bitmap_readonly().ok();
    let mut allocated: HashSet<u32> = HashSet::new();
    let total = fs.total_inodes_readonly();
    for inum in 2..total {
        let ino = match fs.read_inode_readonly(inum) {
            Ok(i) => i,
            Err(e) => {
                b.err("InodeReadFailed", format!("inode {inum}: {e}"));
                continue;
            }
        };
        if ino.mode == 0 {
            continue;
        }
        if ino.is_dir() {
            b.dirs_checked += 1;
        } else {
            b.files_checked += 1;
        }
        if ino.numextents as usize > EFS_DIRECTEXTENTS_MAX {
            b.err(
                "TooManyExtents",
                format!(
                    "inode {inum} has numextents={} (max {EFS_DIRECTEXTENTS_MAX})",
                    ino.numextents
                ),
            );
        }
        for i in 0..(ino.numextents as usize).min(EFS_DIRECTEXTENTS_MAX) {
            let ext = ino.extents[i];
            if ext.magic != 0 {
                b.err(
                    "ExtentBadMagic",
                    format!(
                        "inode {inum} extent {i}: magic=0x{:02X} (expected 0)",
                        ext.magic
                    ),
                );
            }
            if ext.length == 0 {
                continue;
            }
            let end = ext.bn.saturating_add(ext.length as u32);
            if end > sb.fs_size {
                b.err(
                    "ExtentPastVolume",
                    format!(
                        "inode {inum} extent {i}: [{}..{}) extends past fs_size {}",
                        ext.bn, end, sb.fs_size
                    ),
                );
                continue;
            }
            for blk in ext.bn..end {
                if !allocated.insert(blk) {
                    b.err(
                        "DoubleAllocation",
                        format!("block {blk} allocated to inode {inum} but also to another inode"),
                    );
                }
            }
        }
    }

    if let Some(bm) = bitmap {
        // Cross-check: every "in use" inode-claimed block should have
        // its bitmap bit set. (We don't check the reverse — the bitmap
        // legitimately marks inode-table + reserved regions as in-use
        // and those don't show up in inode extents.)
        for blk in &allocated {
            let byte = (*blk / 8) as usize;
            if byte >= bm.len() {
                b.err(
                    "BitmapTooSmall",
                    format!(
                        "inode-allocated block {} past bitmap end ({} bytes)",
                        blk,
                        bm.len()
                    ),
                );
                continue;
            }
            let bit = 7 - (blk % 8);
            if bm[byte] & (1 << bit) == 0 {
                b.err(
                    "BitmapMissingAllocation",
                    format!("inode-allocated block {blk} is not marked in-use in bitmap"),
                );
            }
        }
        let set_bits: u32 = bm.iter().map(|b| b.count_ones()).sum();
        b.extra
            .push(("bitmap_set_bits".into(), set_bits.to_string()));
    } else {
        b.warn("BitmapReadFailed", "could not read bitmap".into());
    }

    b.extra
        .push(("allocated_data_blocks".into(), allocated.len().to_string()));
    b.extra
        .push(("fs_size_blocks".into(), sb.fs_size.to_string()));
    b.extra.push(("ncg".into(), sb.ncg.to_string()));

    // --- Step 5: connectivity (BFS from root inum 2) ---
    // Every mode!=0 inode not reachable from the root is an orphan.
    // The repair path can adopt orphans into a synthetic lost+found
    // directory under the root.
    check_connectivity(fs, &mut b)?;

    Ok(b.finish())
}

/// BFS from root inum 2 collecting reachable inums. Any in-use inode
/// (mode != 0) outside the reachable set is reported as an
/// `OrphanInode` error and pushed into `orphaned_entries` so the
/// repair pass can adopt them.
fn check_connectivity<R: Read + Seek>(
    fs: &mut EfsFilesystem<R>,
    b: &mut Builder,
) -> Result<(), FilesystemError> {
    use std::collections::VecDeque;
    let mut reached: HashSet<u32> = HashSet::new();
    reached.insert(2);
    let mut queue: VecDeque<u32> = VecDeque::new();
    queue.push_back(2);
    while let Some(dir_inum) = queue.pop_front() {
        let dir = match fs.read_inode_readonly(dir_inum) {
            Ok(d) => d,
            Err(_) => continue, // already reported elsewhere
        };
        if !dir.is_dir() {
            continue;
        }
        for i in 0..(dir.numextents as usize).min(EFS_DIRECTEXTENTS_MAX) {
            let ext = dir.extents[i];
            if ext.length == 0 {
                continue;
            }
            for blk in ext.bn..ext.bn.saturating_add(ext.length as u32) {
                let mut buf = [0u8; EFS_BLOCKSIZE as usize];
                if fs.read_block_readonly(blk, &mut buf).is_err() {
                    continue;
                }
                for de in parse_dir_block(&buf) {
                    if de.name == b"." || de.name == b".." {
                        continue;
                    }
                    if de.inum < 2 {
                        continue;
                    }
                    if reached.insert(de.inum) {
                        queue.push_back(de.inum);
                    }
                }
            }
        }
    }

    let total = fs.total_inodes_readonly();
    for inum in 2..total {
        let ino = fs.read_inode_readonly(inum)?;
        if ino.mode == 0 {
            continue;
        }
        if !reached.contains(&inum) {
            b.err(
                "OrphanInode",
                format!(
                    "inode {inum} (mode=0o{:o}, size={}) is unreachable from root",
                    ino.mode, ino.size
                ),
            );
            b.orphaned.push(OrphanedEntry {
                id: inum as u64,
                name: format!("ino_{inum}"),
                is_directory: ino.is_dir(),
                missing_parent_id: 0,
            });
        }
    }
    Ok(())
}

fn check_geometry(sb: &EfsSuperblock, b: &mut Builder) {
    if sb.fs_size == 0 {
        b.err("ZeroFsSize", "fs_size is zero".into());
    }
    if sb.firstcg == 0 {
        b.err("ZeroFirstCG", "firstcg is zero".into());
    }
    if sb.cgfsize == 0 {
        b.err("ZeroCgFsize", "cgfsize is zero".into());
    }
    if sb.ncg == 0 {
        b.err("ZeroNcg", "ncg is zero".into());
    }
    let cg_total = (sb.ncg as u64).saturating_mul(sb.cgfsize as u64);
    let region_end = (sb.firstcg as u64).saturating_add(cg_total);
    if region_end > sb.fs_size as u64 + 1024 {
        // Slack of 1024 blocks accommodates the small trailing
        // reserved zone where the replica may live.
        b.err(
            "CgRegionPastFsSize",
            format!(
                "firstcg + ncg * cgfsize = {region_end} exceeds fs_size {} (+ trailing slack)",
                sb.fs_size
            ),
        );
    }
    if sb.bmsize == 0 {
        b.err("ZeroBmsize", "bmsize is zero".into());
    }
    // bmblock = 0 is accepted (IRIX mkfs_efs convention; see Slice 2).
    if sb.replsb == 0 {
        b.err("ZeroReplsb", "replsb is zero".into());
    }
}

fn check_replica<R: Read + Seek>(
    fs: &mut EfsFilesystem<R>,
    primary: &EfsSuperblock,
    b: &mut Builder,
) -> Result<(), FilesystemError> {
    let replica = fs.read_replica_superblock()?;
    macro_rules! cmp {
        ($field:ident, $code:literal) => {
            if replica.$field != primary.$field {
                b.err(
                    $code,
                    format!(
                        "replica.{} = {:?} differs from primary {:?}",
                        stringify!($field),
                        replica.$field,
                        primary.$field
                    ),
                );
            }
        };
    }
    cmp!(fs_size, "ReplicaFsSizeMismatch");
    cmp!(firstcg, "ReplicaFirstCgMismatch");
    cmp!(cgfsize, "ReplicaCgfSizeMismatch");
    cmp!(cgisize, "ReplicaCgiSizeMismatch");
    cmp!(ncg, "ReplicaNcgMismatch");
    cmp!(bmsize, "ReplicaBmsizeMismatch");
    cmp!(bmblock, "ReplicaBmblockMismatch");
    cmp!(replsb, "ReplicaReplsbMismatch");
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Cursor;

    fn build_clean_volume() -> Vec<u8> {
        // Same shape as the Slice-2 fixture but trimmed to one CG.
        let mut img = vec![0u8; 256 * 512];
        let sb_off = 512;
        let total_blocks = (img.len() / 512) as u32;
        let sb = EfsSuperblock {
            fs_size: total_blocks,
            firstcg: 18,
            cgfsize: 64,
            cgisize: 2,
            sectors: 63,
            heads: 1,
            ncg: 1,
            dirty: 0,
            fs_time: 0,
            magic: 0x0007_2959, // EFS_MAGIC_OLD — re-exported via efs.rs
            fname: *b"fsckt0",
            fpack: *b"fsckt0",
            bmsize: 32,
            tfree: 200,
            tinode: 8,
            bmblock: 2,
            replsb: total_blocks - 1,
            lastialloc: 2,
            checksum: 0,
        };
        sb.write_into(&mut img[sb_off..sb_off + super::super::efs::EFS_SUPERBLOCK_SIZE]);

        // Bitmap: blocks 0/1/2 + 18/19 (CG 0 inode region) + 25 (root dir) + last block.
        let bm_off = 2 * 512;
        for b in [0u32, 1, 2, 18, 19, 25, total_blocks - 1] {
            let by = (b / 8) as usize;
            let bb = 7 - (b % 8);
            img[bm_off + by] |= 1 << bb;
        }
        // Mirror the primary SB into the replica slot.
        let replica_off = (total_blocks as usize - 1) * 512;
        let mut primary = [0u8; super::super::efs::EFS_SUPERBLOCK_SIZE];
        primary.copy_from_slice(&img[sb_off..sb_off + super::super::efs::EFS_SUPERBLOCK_SIZE]);
        img[replica_off..replica_off + super::super::efs::EFS_SUPERBLOCK_SIZE]
            .copy_from_slice(&primary);
        img
    }

    #[test]
    fn fsck_clean_volume_has_no_errors() {
        let img = build_clean_volume();
        let mut fs = EfsFilesystem::open(Cursor::new(img), 0).expect("open");
        let result = fsck_efs(&mut fs).expect("fsck");
        assert!(
            result.errors.is_empty(),
            "expected no errors, got: {:?}",
            result
                .errors
                .iter()
                .map(|e| (&e.code, &e.message))
                .collect::<Vec<_>>()
        );
    }

    #[test]
    fn fsck_flags_replica_mismatch() {
        let mut img = build_clean_volume();
        // Corrupt the replica's fs_size to provoke a mismatch.
        let total_blocks = (img.len() / 512) as u32;
        let replica_off = (total_blocks as usize - 1) * 512;
        img[replica_off..replica_off + 4].copy_from_slice(&0xDEADu32.to_be_bytes());
        let mut fs = EfsFilesystem::open(Cursor::new(img), 0).expect("open");
        let result = fsck_efs(&mut fs).expect("fsck");
        assert!(
            result
                .errors
                .iter()
                .any(|e| e.code == "ReplicaFsSizeMismatch"),
            "expected ReplicaFsSizeMismatch, got: {:?}",
            result.errors.iter().map(|e| &e.code).collect::<Vec<_>>()
        );
        // Repairable flag should propagate through to the aggregate.
        assert!(result.repairable, "replica mismatch should be repairable");
    }

    #[test]
    fn repair_fixes_replica_mismatch() {
        use super::super::filesystem::EditableFilesystem;
        let mut img = build_clean_volume();
        let total_blocks = (img.len() / 512) as u32;
        let replica_off = (total_blocks as usize - 1) * 512;
        img[replica_off..replica_off + 4].copy_from_slice(&0xDEADu32.to_be_bytes());

        let mut fs = EfsFilesystem::open(Cursor::new(img), 0).expect("open");
        let report = fs.repair().expect("repair");
        assert!(
            report.fixes_applied.iter().any(|s| s.contains("Replica")),
            "expected replica fix applied, got: {:?}",
            report
        );

        // Re-running fsck should now report no errors.
        let after = fsck_efs(&mut fs).expect("fsck after repair");
        assert!(
            after.errors.is_empty(),
            "expected no errors after repair, got: {:?}",
            after.errors.iter().map(|e| &e.code).collect::<Vec<_>>()
        );
    }
}
