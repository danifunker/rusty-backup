//! SGI EFS v1 integrity verifier, the companion to `efs_v1.rs`.
//!
//! There is no public `fsck_efs` for the 1986 format to check against — IRIX's
//! own was never open — so this follows the pattern the PFS3 and SFS verifiers
//! use: check the volume against itself, and only repair what a clean
//! structure makes unambiguous. The strongest invariant comes straight from
//! the format's own accounting and is confirmed on all three volumes of the
//! IRIS 3130 disk: the blocks reachable from every in-use inode, plus the
//! metadata blocks the geometry implies, equal `fs_size - fs_tfree` exactly.
//!
//! Checks, in order:
//!   1. Geometry — `fs_size == firstcg + ncg * cgfsize`, the inode table fits
//!      inside a cylinder group, the bitmap covers the volume and the first
//!      cylinder group clears it.
//!   2. Superblock checksum over 0..0x9E (see `efs_v1_superblock_checksum`).
//!   3. Inode table — every `di_mode != 0` inode parses, its extents carry a
//!      zero `ex_magic`, sit inside the volume, and cover only cylinder-group
//!      data blocks. Two inodes claiming one block is a double allocation.
//!   4. Bitmap shadow — a block an inode owns must read as in-use, and a block
//!      marked in-use must be owned by something.
//!   5. `fs_tfree` / `fs_tinode` against the counts just computed.
//!   6. Connectivity — breadth-first from inode 2; any live inode outside the
//!      reachable set becomes an `OrphanInode` for the repair pass to adopt.
//!
//! `ExtentInMetadata` is the one finding that cannot be repaired by editing
//! bookkeeping: it means file content has already been written over an inode
//! table, which no bitmap or superblock edit undoes.

use std::collections::{HashMap, HashSet, VecDeque};
use std::io::{Read, Seek};

use super::efs_v1::{
    efs_v1_superblock_checksum, EfsV1DataRegions, EfsV1Filesystem, EfsV1Superblock,
    EFS_V1_BITMAPBB, EFS_V1_DIRECTEXTENTS, EFS_V1_MAXEXTENTS, EFS_V1_ROOT_INODE,
};
use super::filesystem::FilesystemError;
use super::fsck::{FsckIssue, FsckResult, FsckStats, OrphanedEntry};

/// Codes the EFS v1 `repair()` path knows how to fix. Anything else is
/// reported with `repairable = false` so the UI can grey the button out.
pub(crate) fn is_repairable_code(code: &str) -> bool {
    matches!(
        code,
        "SuperblockChecksumMismatch"
            | "BitmapMissingAllocation"
            | "BitmapLeakedBlock"
            | "TfreeMismatch"
            | "TinodeMismatch"
            | "OrphanInode"
    )
}

/// Findings that mean the on-disk structure is damaged rather than merely
/// mis-accounted. Repair refuses to run while any of these stand.
pub(crate) fn is_structural_code(code: &str) -> bool {
    matches!(
        code,
        "GeometryDoesNotClose"
            | "InodeTableOverrunsGroup"
            | "BitmapTooSmall"
            | "BitmapOverlapsFirstGroup"
            | "ExtentPastVolume"
            | "ExtentInMetadata"
            | "ExtentBadMagic"
            | "ExtentBadLength"
            | "DoubleAllocation"
            | "TooManyExtents"
            | "InodeReadFailed"
    )
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

/// Run the verifier. Read-only: every method it calls is on the read impl, so
/// a volume opened from an immutable source checks fine.
pub fn fsck_efs_v1<R: Read + Seek>(
    fs: &mut EfsV1Filesystem<R>,
) -> Result<FsckResult, FilesystemError> {
    let mut b = Builder::new();
    let sb = fs.superblock().clone();
    check_geometry(&sb, &mut b);
    check_checksum(fs, &mut b)?;

    let regions = EfsV1DataRegions::from_sb(&sb);
    let total_inodes = sb.total_inodes();
    // block -> the inode that claims it, so a double allocation can name both.
    let mut owner: HashMap<u32, u32> = HashMap::new();
    let mut free_inodes: u32 = 0;
    let mut live: Vec<u32> = Vec::new();

    for inum in 0..total_inodes {
        let inode = match fs.read_inode(inum) {
            Ok(i) => i,
            Err(e) => {
                b.err("InodeReadFailed", format!("inode {inum}: {e}"));
                continue;
            }
        };
        if inode.is_free() {
            free_inodes += 1;
            continue;
        }
        live.push(inum);
        if inode.is_dir() {
            b.dirs_checked += 1;
        } else {
            b.files_checked += 1;
        }
        // A device node keeps its major/minor where the first extent would be,
        // so it owns no blocks and must not be walked as if it did.
        if inode.is_device() {
            continue;
        }
        if inode.numextents as usize > EFS_V1_MAXEXTENTS {
            b.err(
                "TooManyExtents",
                format!(
                    "inode {inum} claims {} extents (max {EFS_V1_MAXEXTENTS})",
                    inode.numextents
                ),
            );
            continue;
        }
        // Inline slots exactly as stored: `extents_of` would reject a bad
        // extent before fsck could say *how* it is bad.
        let owned: Vec<super::efs_v1::EfsV1Extent> =
            if inode.numextents as usize <= EFS_V1_DIRECTEXTENTS {
                inode.extents[..inode.numextents as usize].to_vec()
            } else {
                match fs.extents_of(&inode) {
                    Ok(mut exts) => {
                        // The inline slots are index runs in indirect mode, and
                        // those blocks are allocated too, or they look leaked.
                        let direxts = (inode.extents[0].offset as usize).min(EFS_V1_DIRECTEXTENTS);
                        exts.extend_from_slice(&inode.extents[..direxts]);
                        exts
                    }
                    Err(e) => {
                        b.err("InodeReadFailed", format!("inode {inum} extents: {e}"));
                        continue;
                    }
                }
            };
        for (idx, ext) in owned.iter().enumerate() {
            walk_extent(&mut b, &mut owner, &sb, &regions, inum, ext, idx);
        }
    }

    check_bitmap(fs, &sb, &regions, &owner, &mut b)?;
    check_inode_accounting(&sb, free_inodes, &mut b);
    check_connectivity(fs, &live, &mut b)?;

    b.extra
        .push(("inodes in use".into(), live.len().to_string()));
    b.extra
        .push(("blocks in use".into(), owner.len().to_string()));
    b.extra.push((
        "word order".into(),
        fs.byte_order().display_name().to_string(),
    ));
    Ok(b.finish())
}

/// Account one extent's blocks, flagging anything out of bounds, in metadata,
/// or already claimed by another inode.
fn walk_extent(
    b: &mut Builder,
    owner: &mut HashMap<u32, u32>,
    sb: &EfsV1Superblock,
    regions: &EfsV1DataRegions,
    inum: u32,
    ext: &super::efs_v1::EfsV1Extent,
    idx: usize,
) {
    if ext.is_hole() {
        return;
    }
    if ext.magic != 0 {
        b.err(
            "ExtentBadMagic",
            format!(
                "inode {inum} extent {idx}: ex_magic is 0x{:02X}, must be zero",
                ext.magic
            ),
        );
        return;
    }
    if ext.length == 0 {
        b.err(
            "ExtentBadLength",
            format!("inode {inum} extent {idx}: zero length"),
        );
        return;
    }
    let end = ext.bn.saturating_add(ext.length as u32);
    if end > sb.fs_size {
        b.err(
            "ExtentPastVolume",
            format!(
                "inode {inum} extent {idx}: [{}..{end}) runs past fs_size {}",
                ext.bn, sb.fs_size
            ),
        );
        return;
    }
    if let Some(bad) = (ext.bn..end).find(|blk| !regions.contains(*blk)) {
        b.err(
            "ExtentInMetadata",
            format!(
                "inode {inum} extent {idx}: [{}..{end}) covers block {bad}, which is filesystem \
                 metadata rather than a cylinder-group data block",
                ext.bn
            ),
        );
    }
    for blk in ext.bn..end {
        if let Some(prev) = owner.insert(blk, inum) {
            b.err(
                "DoubleAllocation",
                format!("block {blk} is claimed by both inode {prev} and inode {inum}"),
            );
        }
    }
}

/// `fs_size == firstcg + ncg * cgfsize`, and the metadata head has to fit.
fn check_geometry(sb: &EfsV1Superblock, b: &mut Builder) {
    let expect = (sb.firstcg as u64) + (sb.ncg as u64) * (sb.cgfsize as u64);
    if expect != sb.fs_size as u64 {
        b.err(
            "GeometryDoesNotClose",
            format!(
                "firstcg {} + ncg {} * cgfsize {} = {expect}, but fs_size is {}",
                sb.firstcg, sb.ncg, sb.cgfsize, sb.fs_size
            ),
        );
    }
    if sb.cgisize as u32 >= sb.cgfsize {
        b.err(
            "InodeTableOverrunsGroup",
            format!(
                "cgisize {} leaves no data blocks in a {}-block cylinder group",
                sb.cgisize, sb.cgfsize
            ),
        );
    }
    if (sb.bmsize as u64) * 8 < sb.fs_size as u64 {
        b.err(
            "BitmapTooSmall",
            format!(
                "bmsize {} bytes covers {} blocks, short of fs_size {}",
                sb.bmsize,
                sb.bmsize as u64 * 8,
                sb.fs_size
            ),
        );
    }
    let bitmap_end = EFS_V1_BITMAPBB + sb.bitmap_blocks();
    if sb.firstcg < bitmap_end {
        b.err(
            "BitmapOverlapsFirstGroup",
            format!(
                "first cylinder group starts at block {} but the bitmap runs to {bitmap_end}",
                sb.firstcg
            ),
        );
    }
}

/// The stored `fs_checksum` against a recomputation over 0..0x9E.
fn check_checksum<R: Read + Seek>(
    fs: &mut EfsV1Filesystem<R>,
    b: &mut Builder,
) -> Result<(), FilesystemError> {
    let sector = fs.read_superblock_sector()?;
    let stored = fs.superblock().checksum;
    let mut probe = sector;
    probe[0x9E..0xA2].copy_from_slice(&[0, 0, 0, 0]);
    let computed = efs_v1_superblock_checksum(&probe);
    if stored != computed {
        b.err(
            "SuperblockChecksumMismatch",
            format!("fs_checksum is 0x{stored:08X}, recomputes to 0x{computed:08X}"),
        );
    }
    Ok(())
}

/// Cross-check the bitmap against what the inodes actually claim.
fn check_bitmap<R: Read + Seek>(
    fs: &mut EfsV1Filesystem<R>,
    sb: &EfsV1Superblock,
    regions: &EfsV1DataRegions,
    owner: &HashMap<u32, u32>,
    b: &mut Builder,
) -> Result<(), FilesystemError> {
    let bm = match fs.read_bitmap() {
        Ok(bm) => bm,
        Err(e) => {
            b.err("BitmapTooSmall", format!("cannot read the bitmap: {e}"));
            return Ok(());
        }
    };
    let is_free = |blk: u32| -> bool {
        let byte = (blk / 8) as usize;
        byte < bm.len() && (bm[byte] >> (blk % 8)) & 1 == 1
    };

    // A block an inode owns that the bitmap calls free is the dangerous
    // direction: the allocator would hand it out and overwrite live data.
    let mut missing = 0u32;
    for (&blk, &inum) in owner {
        if is_free(blk) {
            missing += 1;
            if missing <= 8 {
                b.err(
                    "BitmapMissingAllocation",
                    format!("block {blk} is used by inode {inum} but the bitmap marks it free"),
                );
            }
        }
    }
    if missing > 8 {
        b.err(
            "BitmapMissingAllocation",
            format!(
                "... and {} further blocks marked free while in use",
                missing - 8
            ),
        );
    }

    // The other direction only wastes space, so it is a warning unless the
    // repair pass is asked to reclaim it.
    let mut leaked = 0u32;
    for (lo, hi) in regions.ranges() {
        for blk in lo..hi {
            if !is_free(blk) && !owner.contains_key(&blk) {
                leaked += 1;
            }
        }
    }
    if leaked > 0 {
        b.err(
            "BitmapLeakedBlock",
            format!("{leaked} block(s) marked in use but claimed by no inode"),
        );
    }

    let free_now = count_free(&bm, regions);
    if free_now != sb.tfree {
        b.err(
            "TfreeMismatch",
            format!("fs_tfree is {}, the bitmap counts {free_now}", sb.tfree),
        );
    }
    Ok(())
}

fn count_free(bm: &[u8], regions: &EfsV1DataRegions) -> u32 {
    let total_bits = ((bm.len() as u64) * 8).min(u32::MAX as u64) as u32;
    let mut n = 0u32;
    for (lo, hi) in regions.ranges() {
        for bit in lo..hi.min(total_bits) {
            n += ((bm[(bit / 8) as usize] >> (bit % 8)) & 1) as u32;
        }
    }
    n
}

/// `fs_tinode` sits one below the free-inode count on a healthy volume — the
/// convention both volumes of the IRIS 3130 disk follow.
fn check_inode_accounting(sb: &EfsV1Superblock, free_inodes: u32, b: &mut Builder) {
    let expect = free_inodes.saturating_sub(1);
    if sb.tinode != expect {
        b.err(
            "TinodeMismatch",
            format!(
                "fs_tinode is {}, but {free_inodes} inodes are free (expected {expect})",
                sb.tinode
            ),
        );
    }
}

/// Breadth-first from the root; anything live but unreachable is an orphan.
fn check_connectivity<R: Read + Seek>(
    fs: &mut EfsV1Filesystem<R>,
    live: &[u32],
    b: &mut Builder,
) -> Result<(), FilesystemError> {
    let root = match fs.read_inode(EFS_V1_ROOT_INODE) {
        Ok(r) => r,
        Err(e) => {
            b.err("InodeReadFailed", format!("root inode: {e}"));
            return Ok(());
        }
    };
    if !root.is_dir() {
        b.err(
            "RootNotADirectory",
            format!("inode {EFS_V1_ROOT_INODE} has mode 0o{:06o}", root.mode),
        );
        return Ok(());
    }

    let total = fs.superblock().total_inodes();
    let mut seen: HashSet<u32> = HashSet::new();
    seen.insert(EFS_V1_ROOT_INODE);
    let mut queue: VecDeque<u32> = VecDeque::new();
    queue.push_back(EFS_V1_ROOT_INODE);

    while let Some(inum) = queue.pop_front() {
        let inode = match fs.read_inode(inum) {
            Ok(i) => i,
            Err(_) => continue,
        };
        if !inode.is_dir() {
            continue;
        }
        let entries = match fs.read_dir_entries(&inode) {
            Ok(e) => e,
            Err(e) => {
                b.warn(
                    "DirectoryUnreadable",
                    format!("directory inode {inum}: {e}"),
                );
                continue;
            }
        };
        for (child, name) in entries {
            if child >= total {
                b.err(
                    "DirentPastInodeTable",
                    format!("directory inode {inum} entry '{name}' points at inode {child}"),
                );
                continue;
            }
            if seen.insert(child) {
                queue.push_back(child);
            }
        }
    }

    for &inum in live {
        if !seen.contains(&inum) {
            let is_dir = fs.read_inode(inum).map(|i| i.is_dir()).unwrap_or(false);
            b.err(
                "OrphanInode",
                format!("inode {inum} is in use but no directory references it"),
            );
            b.orphaned.push(OrphanedEntry {
                id: inum as u64,
                name: format!("inode_{inum}"),
                is_directory: is_dir,
                missing_parent_id: 0,
            });
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::fs::efs_v1::{create_blank_efs_v1, EFS_V1_SUPERBB};
    use byteorder::{BigEndian, ByteOrder};
    use std::io::Cursor;

    const BS: usize = 512;

    fn blank() -> Vec<u8> {
        create_blank_efs_v1(4 * 1024 * 1024, "fscktest").unwrap()
    }

    /// (firstcg, cgfsize, cgisize) straight out of the superblock sector.
    fn geom(img: &[u8]) -> (u32, u32, u32) {
        let sb = &img[BS..];
        (
            BigEndian::read_u32(&sb[0x04..0x08]),
            BigEndian::read_u32(&sb[0x08..0x0C]),
            BigEndian::read_u16(&sb[0x0C..0x0E]) as u32,
        )
    }

    /// Recompute `fs_checksum` so a deliberate edit elsewhere is tested on its
    /// own rather than also tripping the checksum check.
    fn reseal(img: &mut [u8]) {
        let sb = &mut img[BS..BS + BS];
        BigEndian::write_u32(&mut sb[0x9E..0xA2], 0);
        let c = efs_v1_superblock_checksum(sb);
        BigEndian::write_u32(&mut sb[0x9E..0xA2], c);
    }

    fn inode_pos(img: &[u8], inum: u32) -> usize {
        let (firstcg, cgfsize, cgisize) = geom(img);
        let ipcg = cgisize * 4;
        let cg = inum / ipcg;
        let cg_bb = (inum / 4) % cgisize;
        let block = firstcg + cg * cgfsize + cg_bb;
        block as usize * BS + (inum % 4) as usize * 128
    }

    fn run(img: Vec<u8>) -> FsckResult {
        let mut fs = EfsV1Filesystem::open(Cursor::new(img), 0).unwrap();
        fsck_efs_v1(&mut fs).unwrap()
    }

    fn codes(r: &FsckResult) -> Vec<String> {
        r.errors.iter().map(|e| e.code.clone()).collect()
    }

    #[test]
    fn a_freshly_formatted_volume_is_clean() {
        let r = run(blank());
        assert!(r.is_clean(), "unexpected findings: {:?}", codes(&r));
        assert_eq!(r.stats.directories_checked, 1);
    }

    #[test]
    fn a_bad_superblock_checksum_is_caught() {
        let mut img = blank();
        let pos = BS * EFS_V1_SUPERBB as usize + 0x9E;
        BigEndian::write_u32(&mut img[pos..pos + 4], 0xDEAD_BEEF);
        assert!(codes(&run(img)).contains(&"SuperblockChecksumMismatch".to_string()));
    }

    #[test]
    fn a_block_in_use_but_marked_free_is_caught() {
        let mut img = blank();
        // The root directory's data block is the first data block of group 0.
        let (firstcg, _, cgisize) = geom(&img);
        let blk = firstcg + cgisize;
        let bit = BS * 2 + (blk / 8) as usize;
        img[bit] |= 1u8 << (blk % 8); // set = free
        reseal(&mut img);
        let c = codes(&run(img));
        assert!(c.contains(&"BitmapMissingAllocation".to_string()), "{c:?}");
    }

    #[test]
    fn a_block_marked_used_but_owned_by_nobody_is_caught() {
        let mut img = blank();
        let (firstcg, _, cgisize) = geom(&img);
        let blk = firstcg + cgisize + 5; // a free data block
        let byte = BS * 2 + (blk / 8) as usize;
        img[byte] &= !(1u8 << (blk % 8)); // clear = in use
        reseal(&mut img);
        let c = codes(&run(img));
        assert!(c.contains(&"BitmapLeakedBlock".to_string()), "{c:?}");
    }

    #[test]
    fn a_wrong_tfree_is_caught() {
        let mut img = blank();
        let pos = BS + 0x32;
        let cur = BigEndian::read_u32(&img[pos..pos + 4]);
        BigEndian::write_u32(&mut img[pos..pos + 4], cur - 7);
        reseal(&mut img);
        assert!(codes(&run(img)).contains(&"TfreeMismatch".to_string()));
    }

    #[test]
    fn a_wrong_tinode_is_caught() {
        let mut img = blank();
        let pos = BS + 0x36;
        let cur = BigEndian::read_u32(&img[pos..pos + 4]);
        BigEndian::write_u32(&mut img[pos..pos + 4], cur + 3);
        reseal(&mut img);
        assert!(codes(&run(img)).contains(&"TinodeMismatch".to_string()));
    }

    #[test]
    fn an_unreferenced_live_inode_is_reported_as_an_orphan() {
        let mut img = blank();
        let p = inode_pos(&img, 5);
        BigEndian::write_u16(&mut img[p..p + 2], 0o100644); // mode: a live file
        BigEndian::write_u16(&mut img[p + 2..p + 4], 1); // nlink
        reseal(&mut img);
        let r = run(img);
        assert!(codes(&r).contains(&"OrphanInode".to_string()));
        assert_eq!(r.orphaned_entries.len(), 1);
        assert_eq!(r.orphaned_entries[0].id, 5);
    }

    #[test]
    fn an_extent_pointing_into_an_inode_table_is_caught_and_unrepairable() {
        let mut img = blank();
        let (firstcg, _, _) = geom(&img);
        let p = inode_pos(&img, 6);
        BigEndian::write_u16(&mut img[p..p + 2], 0o100644);
        BigEndian::write_u16(&mut img[p + 2..p + 4], 1);
        BigEndian::write_u16(&mut img[p + 0x1C..p + 0x1E], 1); // numextents
                                                               // Extent 0: bn = firstcg (an inode-table block), length 1, offset 0.
        BigEndian::write_u32(&mut img[p + 0x20..p + 0x24], firstcg & 0x00FF_FFFF);
        BigEndian::write_u32(&mut img[p + 0x24..p + 0x28], 1 << 24);
        reseal(&mut img);
        let r = run(img);
        let c = codes(&r);
        assert!(c.contains(&"ExtentInMetadata".to_string()), "{c:?}");
        let issue = r
            .errors
            .iter()
            .find(|e| e.code == "ExtentInMetadata")
            .unwrap();
        assert!(
            !issue.repairable,
            "metadata overwrite must not be repairable"
        );
        assert!(is_structural_code("ExtentInMetadata"));
    }

    #[test]
    fn an_extent_past_the_volume_is_caught() {
        let mut img = blank();
        let fs_size = BigEndian::read_u32(&img[BS..BS + 4]);
        let p = inode_pos(&img, 7);
        BigEndian::write_u16(&mut img[p..p + 2], 0o100644);
        BigEndian::write_u16(&mut img[p + 2..p + 4], 1);
        BigEndian::write_u16(&mut img[p + 0x1C..p + 0x1E], 1);
        BigEndian::write_u32(&mut img[p + 0x20..p + 0x24], (fs_size - 1) & 0x00FF_FFFF);
        BigEndian::write_u32(&mut img[p + 0x24..p + 0x28], 8 << 24); // 8 blocks
        reseal(&mut img);
        let c = codes(&run(img));
        assert!(c.contains(&"ExtentPastVolume".to_string()), "{c:?}");
    }

    // ---- repair -----------------------------------------------------------

    fn repair(img: Vec<u8>) -> (crate::fs::fsck::RepairReport, FsckResult, Vec<u8>) {
        let mut fs = EfsV1Filesystem::open(Cursor::new(img), 0).unwrap();
        let rep = crate::fs::efs_v1::repair_efs_v1(&mut fs).unwrap();
        let after = fsck_efs_v1(&mut fs).unwrap();
        let out = fs.reader_into_inner().into_inner();
        (rep, after, out)
    }

    #[test]
    fn repair_rebuilds_a_damaged_bitmap() {
        let mut img = blank();
        let (firstcg, _, cgisize) = geom(&img);
        // Mark the root's block free, and a free block used: both directions.
        let used = firstcg + cgisize;
        let unused = firstcg + cgisize + 9;
        img[BS * 2 + (used / 8) as usize] |= 1u8 << (used % 8);
        img[BS * 2 + (unused / 8) as usize] &= !(1u8 << (unused % 8));
        reseal(&mut img);

        let (rep, after, _) = repair(img);
        assert!(after.is_clean(), "still dirty: {:?}", codes(&after));
        assert!(
            rep.fixes_applied.iter().any(|f| f.contains("bitmap")),
            "{rep:?}"
        );
        assert_eq!(rep.unrepairable_count, 0);
    }

    #[test]
    fn repair_corrects_counters_and_checksum() {
        let mut img = blank();
        let tf = BS + 0x32;
        BigEndian::write_u32(&mut img[tf..tf + 4], 1);
        let ti = BS + 0x36;
        BigEndian::write_u32(&mut img[ti..ti + 4], 1);
        // Leave fs_checksum stale on purpose: all three must come right.
        let (rep, after, _) = repair(img);
        assert!(after.is_clean(), "still dirty: {:?}", codes(&after));
        assert!(
            rep.fixes_applied.iter().any(|f| f.contains("counters")),
            "{rep:?}"
        );
    }

    #[test]
    fn repair_adopts_an_orphan_into_lost_and_found() {
        let mut img = blank();
        let p = inode_pos(&img, 5);
        BigEndian::write_u16(&mut img[p..p + 2], 0o100644);
        BigEndian::write_u16(&mut img[p + 2..p + 4], 1);
        reseal(&mut img);

        let (rep, after, out) = repair(img);
        assert!(after.is_clean(), "still dirty: {:?}", codes(&after));
        assert!(
            rep.fixes_applied.iter().any(|f| f.contains("lost+found")),
            "{rep:?}"
        );

        // The adopted inode is now reachable by name from the root.
        let mut fs = EfsV1Filesystem::open(Cursor::new(out), 0).unwrap();
        let root = crate::fs::filesystem::Filesystem::root(&mut fs).unwrap();
        let top = crate::fs::filesystem::Filesystem::list_directory(&mut fs, &root).unwrap();
        let lf = top
            .iter()
            .find(|e| e.name == "lost+found")
            .expect("lost+found");
        let inside = crate::fs::filesystem::Filesystem::list_directory(&mut fs, lf).unwrap();
        assert!(
            inside.iter().any(|e| e.name == "inode_5"),
            "{:?}",
            inside.iter().map(|e| &e.name).collect::<Vec<_>>()
        );
    }

    #[test]
    fn repair_refuses_structural_damage_and_writes_nothing() {
        let mut img = blank();
        let (firstcg, _, _) = geom(&img);
        let p = inode_pos(&img, 6);
        BigEndian::write_u16(&mut img[p..p + 2], 0o100644);
        BigEndian::write_u16(&mut img[p + 2..p + 4], 1);
        BigEndian::write_u16(&mut img[p + 0x1C..p + 0x1E], 1);
        BigEndian::write_u32(&mut img[p + 0x20..p + 0x24], firstcg & 0x00FF_FFFF);
        BigEndian::write_u32(&mut img[p + 0x24..p + 0x28], 1 << 24);
        reseal(&mut img);

        let before = img.clone();
        let (rep, _after, out) = repair(img);
        assert!(
            rep.fixes_applied.is_empty(),
            "must not touch a damaged volume: {rep:?}"
        );
        assert!(rep.unrepairable_count > 0);
        assert!(
            rep.fixes_failed
                .iter()
                .any(|f| f.contains("ExtentInMetadata")),
            "{rep:?}"
        );
        assert_eq!(out, before, "repair wrote to a structurally damaged volume");
    }

    #[test]
    fn repair_of_a_clean_volume_is_a_no_op() {
        let img = blank();
        let before = img.clone();
        let (rep, after, out) = repair(img);
        assert!(after.is_clean());
        assert!(rep.fixes_applied.is_empty());
        assert_eq!(out, before);
    }
}
