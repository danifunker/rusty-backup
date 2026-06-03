//! UFS integrity verifier. Mirrors `efs_fsck.rs` in shape (Builder +
//! ordered passes) but adapts every check for UFS's per-cylinder-group
//! layout: each CG owns a header, a SB replica, an inode table, and a
//! free-fragment bitmap whose "set bit = FREE" convention is the
//! opposite of every Linux FS we touch.
//!
//! Checks (in order):
//!   1. Superblock geometry sanity (re-validation of the fields the
//!      `UfsFilesystem::open` parser also enforces, plus version-magic
//!      cross-check).
//!   2. Per-CG header (`CG_MAGIC` + `cg_cgx` matches the index +
//!      `cg_ndblk` within `fpg`) and replica SB match against the
//!      primary on geometry fields (magic / bsize / fsize / frag /
//!      ncg / fpg / ipg / sblkno / cblkno / iblkno / total_frags).
//!   3. Inode-table walk: every `mode != 0` inode parses cleanly; every
//!      direct + indirect block pointer is within `[0, total_frags)`;
//!      and the union of all owned fragments is collected for the
//!      double-allocation + bitmap-consistency checks below.
//!   4. Bitmap consistency: every inode-claimed fragment's bit must be
//!      CLEAR (= allocated). A SET bit (= free) under an in-use claim
//!      surfaces as `BitmapMissingAllocation`. The reverse direction
//!      (free bits that no inode claims) is not flagged — CG metadata
//!      (header / SB replica / inode table) legitimately occupies clear
//!      bits without ever appearing in an inode's block list.
//!   5. Connectivity: BFS from root inum 2; any `mode != 0` inode that
//!      the walk doesn't reach is reported as `OrphanInode` and pushed
//!      into `orphaned_entries` so a future repair pass can adopt it.
//!
//! Repairable codes (handled by the upcoming `repair_ufs` companion):
//! `ReplicaSb*Mismatch`, `BitmapMissingAllocation`, `OrphanInode`.
//! Geometry damage and double-allocations are surfaced for diagnosis
//! only — they need the editor's free-extent allocator to fix.

use std::collections::{HashSet, VecDeque};
use std::io::{Read, Seek};

use super::filesystem::FilesystemError;
use super::fsck::{FsckIssue, FsckResult, FsckStats, OrphanedEntry};
use super::ufs::{
    read_i32, read_i64, read_u32, read_u64, UfsFilesystem, UfsVersion, CG_MAGIC, CG_OFF_CGX,
    CG_OFF_FREEOFF, CG_OFF_MAGIC, CG_OFF_NDBLK, CG_OFF_NEXTFREEOFF, DIRENT_HDR_LEN, MAGIC_OFF,
    MAGIC_UFS1, MAGIC_UFS2, OFF_BSIZE, OFF_CBLKNO, OFF_FPG, OFF_FRAG, OFF_FSIZE, OFF_IBLKNO,
    OFF_IPG, OFF_NCG, OFF_OLD_SIZE, OFF_SBLKNO, OFF_SIZE_UFS2, ROOT_INODE,
};
use super::unix_common::bitmap::BitmapReader;
use super::unix_common::inode::{unix_file_type, UnixFileType};

/// FsckIssue codes the future UFS `repair()` path knows how to fix.
fn is_repairable_code(code: &str) -> bool {
    matches!(
        code,
        "ReplicaSbMagicMismatch"
            | "ReplicaSbBsizeMismatch"
            | "ReplicaSbFsizeMismatch"
            | "ReplicaSbFragMismatch"
            | "ReplicaSbNcgMismatch"
            | "ReplicaSbFpgMismatch"
            | "ReplicaSbIpgMismatch"
            | "ReplicaSbSblknoMismatch"
            | "ReplicaSbCblknoMismatch"
            | "ReplicaSbIblknoMismatch"
            | "ReplicaSbSizeMismatch"
            | "BitmapMissingAllocation"
            | "OrphanInode"
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

/// Run the verifier.
pub fn fsck_ufs<R: Read + Seek + Send>(
    fs: &mut UfsFilesystem<R>,
) -> Result<FsckResult, FilesystemError> {
    let mut b = Builder::new();

    // ----- 1. Geometry re-validation -----
    check_geometry(fs, &mut b);

    // ----- 2. Per-CG header + replica SB + bitmap collection -----
    // Holding every CG's bitmap in memory simultaneously is fine for the
    // sizes we care about (a 2 GiB UFS2 volume at 1 KiB frags has ~256
    // KiB of bitmap total) and saves the inode pass from re-reading.
    let ncg = fs.ncg;
    let fpg = fs.fpg as u64;
    let fsize = fs.fsize;
    let total_frags = fs.total_frags;
    let mut bitmaps: Vec<(Vec<u8>, u64)> = Vec::with_capacity(ncg as usize);

    for cg in 0..ncg {
        match check_cg(fs, cg, &mut b) {
            Ok((bm, ndblk)) => bitmaps.push((bm, ndblk)),
            Err(e) => {
                b.err("CgReadFailed", format!("CG {cg}: {e}"));
                bitmaps.push((Vec::new(), 0));
            }
        }
    }

    // ----- 3. Inode-table walk + 4. Bitmap consistency -----
    let total_inodes = fs.total_inodes();
    let mut allocated: HashSet<u64> = HashSet::new();
    for inum in ROOT_INODE..(total_inodes as u32) {
        let ino = match fs.read_inode(inum) {
            Ok(i) => i,
            Err(e) => {
                b.err("InodeReadFailed", format!("inode {inum}: {e}"));
                continue;
            }
        };
        if ino.mode == 0 {
            continue;
        }
        if matches!(unix_file_type(ino.mode), UnixFileType::Directory) {
            b.dirs_checked += 1;
        } else {
            b.files_checked += 1;
        }

        let (data_frags, indirect_frags) = match fs.walk_inode_blocks(&ino) {
            Ok(pair) => pair,
            Err(e) => {
                b.err("IndirectReadFailed", format!("inode {inum}: {e}"));
                continue;
            }
        };
        record_frags(
            &mut allocated,
            &data_frags,
            "data",
            inum,
            total_frags,
            &mut b,
        );
        record_frags(
            &mut allocated,
            &indirect_frags,
            "indirect",
            inum,
            total_frags,
            &mut b,
        );
    }

    // ----- 4. (cont'd) Bitmap consistency cross-check -----
    for &frag in &allocated {
        let cg = frag / fpg;
        let in_cg = frag % fpg;
        if cg >= ncg as u64 {
            // Already flagged as out-of-bounds in record_frags; skip.
            continue;
        }
        let (bm_bytes, ndblk) = &bitmaps[cg as usize];
        if in_cg >= *ndblk {
            // Fragment past the CG's bitmap end — covered by record_frags's
            // `>= total_frags` check when it applies, or by `cg_ndblk` being
            // smaller than `fpg` for the trailing CG. Surface either way.
            b.err(
                "InodeBlockPastCgEnd",
                format!("allocated fragment {frag} (CG {cg}, in_cg {in_cg}) past cg_ndblk {ndblk}"),
            );
            continue;
        }
        let bm = BitmapReader::new(bm_bytes, *ndblk);
        if bm.is_bit_set(in_cg) {
            // Bit set = free in UFS — but an inode claims it.
            b.err(
                "BitmapMissingAllocation",
                format!(
                    "fragment {frag} (CG {cg}, in_cg {in_cg}) is marked free in bitmap but claimed by an inode"
                ),
            );
        }
    }

    b.extra
        .push(("allocated_frags".into(), allocated.len().to_string()));
    b.extra
        .push(("total_frags".into(), total_frags.to_string()));
    b.extra.push(("ncg".into(), ncg.to_string()));
    b.extra.push(("fsize".into(), fsize.to_string()));

    // ----- 5. Connectivity (BFS from root) -----
    check_connectivity(fs, &mut b)?;

    Ok(b.finish())
}

/// Re-validate the superblock fields the parser also gates on. Catches a
/// SB that was edited out-of-band after open (`tunefs -L` setting a
/// label, etc., is fine — those fields aren't checked).
fn check_geometry<R: Read + Seek + Send>(fs: &mut UfsFilesystem<R>, b: &mut Builder) {
    let bsize = fs.bsize;
    let fsize = fs.fsize;
    let frag = fs.frag;
    let ncg = fs.ncg;
    let fpg = fs.fpg;
    let ipg = fs.ipg;
    if bsize == 0 || !bsize.is_power_of_two() {
        b.err(
            "BadBsize",
            format!("bsize {bsize} not a positive power of two"),
        );
    }
    if fsize == 0 || !fsize.is_power_of_two() || fsize > bsize {
        b.err(
            "BadFsize",
            format!("fsize {fsize} (bsize {bsize}) — must be power of two ≤ bsize"),
        );
    }
    if (frag as u64) * fsize != bsize {
        b.err(
            "FragFsizeInconsistent",
            format!("frag {frag} * fsize {fsize} != bsize {bsize}"),
        );
    }
    if ncg == 0 {
        b.err("ZeroNcg", "ncg is zero".into());
    }
    if fpg == 0 {
        b.err("ZeroFpg", "fpg is zero".into());
    }
    if ipg == 0 {
        b.err("ZeroIpg", "ipg is zero".into());
    }
}

/// Walk a single CG: validate the header magic + index, read the free-
/// fragment bitmap, and (when we can) compare the CG's SB replica
/// against the primary. Returns the bitmap bytes + `cg_ndblk` so the
/// caller can keep them around for the cross-check pass.
fn check_cg<R: Read + Seek + Send>(
    fs: &mut UfsFilesystem<R>,
    cg: u32,
    b: &mut Builder,
) -> Result<(Vec<u8>, u64), FilesystemError> {
    // The bitmap reader doubles as a CG-header validator — it already
    // checks magic + cg_cgx + cg_ndblk + cg_freeoff bounds.
    let (bm, ndblk) = fs.read_cg_free_bitmap(cg)?;

    // Replica SB check. CG 0's replica overlaps the primary slot — skip.
    if cg > 0 {
        match fs.read_replica_sb_bytes(cg) {
            Ok(replica) => compare_replica(fs, cg, &replica, b),
            Err(e) => b.warn(
                "ReplicaSbReadFailed",
                format!("CG {cg}: cannot read replica SB: {e}"),
            ),
        }
    }
    let _ = (
        CG_MAGIC,
        CG_OFF_MAGIC,
        CG_OFF_CGX,
        CG_OFF_NDBLK,
        CG_OFF_FREEOFF,
        CG_OFF_NEXTFREEOFF,
    );
    Ok((bm, ndblk))
}

/// Diff replica SB bytes against the primary on geometry-critical fields.
/// Mismatches map to `ReplicaSb*Mismatch` codes which the upcoming repair
/// pass will rewrite from the primary.
fn compare_replica<R: Read + Seek + Send>(
    fs: &mut UfsFilesystem<R>,
    cg: u32,
    replica: &[u8],
    b: &mut Builder,
) {
    let endian = fs.endian_value();
    let version = fs.version();

    // Magic word — if this is wrong every other field reading is suspect,
    // so we report and stop.
    let magic = read_u32(replica, MAGIC_OFF, endian);
    let expected_magic = match version {
        UfsVersion::Ufs1 => MAGIC_UFS1,
        UfsVersion::Ufs2 => MAGIC_UFS2,
    };
    if magic != expected_magic {
        b.err(
            "ReplicaSbMagicMismatch",
            format!(
                "CG {cg} replica SB magic 0x{magic:08X} differs from primary 0x{expected_magic:08X}"
            ),
        );
        return;
    }

    macro_rules! cmp_u32 {
        ($off:expr, $field:expr, $code:literal, $label:literal) => {{
            let got = read_u32(replica, $off, endian);
            if got != $field {
                b.err(
                    $code,
                    format!(
                        "CG {cg} replica SB {} {} differs from primary {}",
                        $label, got, $field
                    ),
                );
            }
        }};
    }
    macro_rules! cmp_i32_as_u64 {
        ($off:expr, $field:expr, $code:literal, $label:literal) => {{
            let got = read_i32(replica, $off, endian) as u64;
            if got != $field {
                b.err(
                    $code,
                    format!(
                        "CG {cg} replica SB {} {} differs from primary {}",
                        $label, got, $field
                    ),
                );
            }
        }};
    }
    cmp_i32_as_u64!(OFF_BSIZE, fs.bsize, "ReplicaSbBsizeMismatch", "bsize");
    cmp_i32_as_u64!(OFF_FSIZE, fs.fsize, "ReplicaSbFsizeMismatch", "fsize");
    {
        let got = read_i32(replica, OFF_FRAG, endian) as u32;
        if got != fs.frag {
            b.err(
                "ReplicaSbFragMismatch",
                format!(
                    "CG {cg} replica SB frag {got} differs from primary {}",
                    fs.frag
                ),
            );
        }
    }
    cmp_u32!(OFF_NCG, fs.ncg, "ReplicaSbNcgMismatch", "ncg");
    {
        let got = read_i32(replica, OFF_FPG, endian) as u32;
        if got != fs.fpg {
            b.err(
                "ReplicaSbFpgMismatch",
                format!(
                    "CG {cg} replica SB fpg {got} differs from primary {}",
                    fs.fpg
                ),
            );
        }
    }
    cmp_u32!(OFF_IPG, fs.ipg, "ReplicaSbIpgMismatch", "ipg");
    {
        let got = read_i32(replica, OFF_SBLKNO, endian) as u32;
        if got != fs.sblkno {
            b.err(
                "ReplicaSbSblknoMismatch",
                format!(
                    "CG {cg} replica SB sblkno {got} differs from primary {}",
                    fs.sblkno
                ),
            );
        }
    }
    {
        let got = read_i32(replica, OFF_CBLKNO, endian) as u32;
        if got != fs.cblkno {
            b.err(
                "ReplicaSbCblknoMismatch",
                format!(
                    "CG {cg} replica SB cblkno {got} differs from primary {}",
                    fs.cblkno
                ),
            );
        }
    }
    {
        let got = read_i32(replica, OFF_IBLKNO, endian) as u32;
        if got != fs.iblkno {
            b.err(
                "ReplicaSbIblknoMismatch",
                format!(
                    "CG {cg} replica SB iblkno {got} differs from primary {}",
                    fs.iblkno
                ),
            );
        }
    }

    // Total fragments lives in version-specific fields.
    let replica_total = match version {
        UfsVersion::Ufs1 => read_i32(replica, OFF_OLD_SIZE, endian) as u64,
        UfsVersion::Ufs2 => read_i64(replica, OFF_SIZE_UFS2, endian) as u64,
    };
    if replica_total != fs.total_frags {
        b.err(
            "ReplicaSbSizeMismatch",
            format!(
                "CG {cg} replica SB total fragments {replica_total} differs from primary {}",
                fs.total_frags
            ),
        );
    }
    // Reference the unused `read_u64` so the import is kept alive — every
    // call so far uses `read_i32` or `read_u32`.
    let _ = read_u64;
}

/// Validate per-inode block ownership and accumulate the allocated set.
fn record_frags(
    allocated: &mut HashSet<u64>,
    frags: &[u64],
    label: &str,
    inum: u32,
    total_frags: u64,
    b: &mut Builder,
) {
    for &f in frags {
        if f == 0 {
            continue; // sparse hole
        }
        if f >= total_frags {
            b.err(
                "BlockPastVolume",
                format!("inode {inum} {label} fragment {f} >= total fragments {total_frags}"),
            );
            continue;
        }
        if !allocated.insert(f) {
            b.err(
                "DoubleAllocation",
                format!("fragment {f} (inode {inum} {label}) also claimed by another inode"),
            );
        }
    }
}

/// BFS from root inum 2 collecting reachable inums. Any in-use inode
/// (mode != 0) outside the reachable set is reported as `OrphanInode`.
fn check_connectivity<R: Read + Seek + Send>(
    fs: &mut UfsFilesystem<R>,
    b: &mut Builder,
) -> Result<(), FilesystemError> {
    let mut reached: HashSet<u32> = HashSet::new();
    reached.insert(ROOT_INODE);
    let mut queue: VecDeque<u32> = VecDeque::new();
    queue.push_back(ROOT_INODE);

    let endian = fs.endian_value();
    while let Some(dir_inum) = queue.pop_front() {
        let dir = match fs.read_inode(dir_inum) {
            Ok(d) => d,
            Err(_) => continue,
        };
        if !matches!(unix_file_type(dir.mode), UnixFileType::Directory) {
            continue;
        }
        let bytes = match fs.read_dir_bytes_raw(&dir) {
            Ok(b) => b,
            Err(_) => continue,
        };
        let mut off = 0usize;
        while off + DIRENT_HDR_LEN <= bytes.len() {
            let d_ino = read_u32(&bytes, off, endian);
            let d_reclen = read_u16(&bytes, off + 4, endian) as usize;
            let d_namlen = bytes[off + 7] as usize;
            if d_reclen == 0 || !d_reclen.is_multiple_of(4) || off + d_reclen > bytes.len() {
                break;
            }
            if d_ino != 0 && d_namlen > 0 && d_namlen + DIRENT_HDR_LEN <= d_reclen {
                let name = &bytes[off + DIRENT_HDR_LEN..off + DIRENT_HDR_LEN + d_namlen];
                if name != b"." && name != b".." && reached.insert(d_ino) {
                    queue.push_back(d_ino);
                }
            }
            off += d_reclen;
        }
    }

    let total_inodes = fs.total_inodes();
    for inum in ROOT_INODE..(total_inodes as u32) {
        let ino = match fs.read_inode(inum) {
            Ok(i) => i,
            Err(_) => continue, // I/O errors reported in the inode pass
        };
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
                is_directory: matches!(unix_file_type(ino.mode), UnixFileType::Directory),
                missing_parent_id: 0,
            });
        }
    }
    Ok(())
}

/// `read_u16` is private to ufs.rs's siblings — re-import here for the
/// connectivity walker (DIRENT2's `d_reclen` is a u16).
use super::ufs::read_u16;
