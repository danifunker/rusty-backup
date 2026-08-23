//! `newfs` for UFS1 — lay a blank BSD Fast File System down on blank media.
//!
//! Reached from `rb-cli new volume ufs` (4.4BSD) and `new volume ufs-43bsd`
//! (pre-4.4BSD). The reader, editor and fsck all live in [`crate::fs::ufs`];
//! this module only writes the initial state, following the same `initcg` /
//! `fsinit` sequence BSD's own `newfs(8)` runs, so a volume we format is the
//! one a real `newfs` would have produced for the same geometry.
//!
//! **Both `struct cg` generations**, in either byte order:
//!
//! * [`CgLayout::Modern`] — 4.4BSD and later (FreeBSD, NetBSD, OpenBSD,
//!   Solaris). Groups are as large as their maps allow, the map offsets live
//!   in the header, directory entries carry a `d_type` byte, and the
//!   rotational-layout tables are dead weight left zeroed.
//! * [`CgLayout::Bsd43`] — NeXTSTEP / OPENSTEP. A different `struct fs` tail,
//!   a different `struct cg`, 16-bit `d_namlen` directory entries with no
//!   `d_type`, device blocks the size of a fragment, groups staggered across
//!   tracks by `fs_cgoffset`, and live `fs_postbl` / `fs_rotbl` / `cg_btot` /
//!   `cg_b` rotational tables. Every one of those was re-derived from the two
//!   NeXTSTEP 3.3 reference disks; see
//!   `docs/partition_table_writers_backlog.md` § "UFS1 creation".
//!
//! Writing a 4.4BSD volume into a NeXT partition produces something NeXTSTEP
//! cannot read — not because of the cylinder group but because of the
//! directory entries: a 4.4BSD `.` record puts `d_type` where a 4.3BSD kernel
//! reads the high byte of a 16-bit `d_namlen`, so the root directory comes
//! back claiming a 1025-character name. That is why both generations are
//! written rather than just the modern one.

use std::io::{Seek, SeekFrom, Write};

use super::filesystem::FilesystemError;
use super::ufs::{
    dirent_record_size, write_dirent_namlen, write_i16, write_i32, write_i64, write_u16, write_u32,
    CgLayout, UfsEndian, CG_MAGIC, D1_OFF_BLOCKS, D1_OFF_DB, D1_OFF_GID, D1_OFF_MODE, D1_OFF_MTIME,
    D1_OFF_NLINK, D1_OFF_SIZE, D1_OFF_UID, DINODE1_SIZE, DIRBLKSIZ, DT_DIR, MAGIC_OFF, MAGIC_UFS1,
    OFF_MAXSYMLINKLEN, OFF_VOLNAME, ROOT_INODE, SB_OFFSET_UFS1, VOLNAME_LEN,
};

/// `SBLOCKSIZE` — the space FFS reserves for a superblock, whatever the
/// struct's actual size.
const SBLOCKSIZE: u64 = 8192;
/// Bytes we lay down per superblock copy. `struct fs` ends at 1376; 2048 is
/// what `fragroundup` gives for the default 1 KiB fragment and what the
/// reader's `SB_READ_SIZE` expects to find.
const SB_WRITE_SIZE: usize = 2048;
/// `sizeof(struct fs)` up to `fs_magic`, which is what `fs_sbsize` rounds up.
const STRUCT_FS_SIZE: u64 = 1376;
/// Fixed part of the 4.4BSD `struct cg`, i.e. where `cg_space` starts.
const CG_SPACE_OFF: u64 = 168;
/// `sizeof(struct csum)` — the per-CG entry in the cylinder summary area.
const CSUM_SIZE: u64 = 16;
/// `MAXFRAG` — fragments per block can never exceed this.
const MAXFRAG: u64 = 8;
/// `FS_44INODEFMT`.
const INODEFMT_44BSD: i32 = 2;
/// `FS_FLAGS_UPDATED` — says the 32-bit `fs_flags` word is the live one.
const FS_FLAGS_UPDATED: u8 = 0x80;
/// Cluster-summary depth. `newfs` uses `MIN(fs_maxcontig, FS_MAXCONTIG)`.
const CONTIGSUMSIZE: u64 = 8;
const MAXCONTIG: i32 = 8;
/// `fs_maxbpg` — blocks one file may take from a cylinder group.
const MAXBPG: i32 = 2048;
/// `newfs` defaults, recorded for the allocator's benefit only.
const AVGFILESIZE: i32 = 16384;
const AVGFPDIR: i32 = 64;
/// Percentage of the volume reserved for root, as FreeBSD's `newfs` sets it.
const MINFREE: i32 = 8;
/// Whole data blocks a volume must have left after its metadata. Below this a
/// structurally valid FFS is still not a useful one.
const MIN_DATA_BLOCKS: u64 = 16;

/// `fs_old_cpg` / `fs_old_nrpos` on a 4.4BSD volume: one cylinder group is
/// described as one "cylinder" with a single rotational position, which is
/// what every FFS built since rotational optimisation was abandoned records.
const OLD_CPG: u64 = 1;
const OLD_NRPOS: u64 = 1;

// ---- 4.3BSD `struct cg` / `struct fs` constants ----
//
// `cg_btot[MAXCPG]`, `cg_b[MAXCPG][NRPOS]` and `cg_iused[MAXIPG/NBBY]` are
// fixed-size arrays compiled into the kernel, which is what puts `cg_magic` at
// 980 and caps a group at 32 cylinders and 2048 inodes.

/// `NRPOS` — rotational positions per cylinder in a 4.3BSD kernel.
const NRPOS_43: u64 = 8;
/// `MAXCPG` — cylinders per group `cg_btot` / `cg_b` can describe.
const MAXCPG_43: u64 = 32;
/// `MAXIPG` — inodes per group `cg_iused` can describe.
const MAXIPG_43: u64 = 2048;
/// `cg_btot` — per-cylinder free-block totals.
const CG43_OFF_BTOT: u64 = 84;
/// `cg_b` — free blocks per cylinder per rotational position.
const CG43_OFF_B: u64 = 212;
/// `cg_iused`, `cg_magic` and `cg_free`, as [`crate::fs::ufs`] reads them.
const CG43_OFF_IUSED: u64 = 724;
const CG43_OFF_MAGIC: usize = 980;
const CG43_OFF_FREE: u64 = 984;
/// `fs_old_cpc`, `fs_postbl[MAXCPG][NRPOS]` and `fs_rotbl[]` in `struct fs`.
const FS43_OFF_CPC: usize = 856;
const FS43_OFF_POSTBL: usize = 860;
const FS43_OFF_ROTBL: usize = 1376;
/// `fs_minfree` / `fs_maxbpg` as NeXTSTEP's own `newfs` recorded them.
const MINFREE_43: i32 = 10;
const MAXBPG_43: i32 = 256;
/// Sectors per track both reference disks use. Cylinder groups are staggered
/// by exactly this much (`fs_cgoffset`).
const NSECT_43: u64 = 32;
/// Track counts to try, largest first: a bigger cylinder means fewer, larger
/// groups, and 16 x 32 is the NeXTSTEP/Intel reference disk's own geometry.
const NTRAK_43_CHOICES: [u64; 3] = [16, 4, 2];

/// The `newfs` knobs a caller may set. Everything else is derived.
#[derive(Debug, Clone)]
pub struct Ufs1FormatParams {
    pub size_bytes: u64,
    /// `fs_bsize`. Default 8192.
    pub block_size: u64,
    /// `fs_fsize`. Default 1024.
    pub frag_size: u64,
    /// Bytes of data per inode. Default `4 * frag_size`, as `newfs -i` does.
    pub bytes_per_inode: u64,
    pub endian: UfsEndian,
    /// Which `struct cg` generation to write.
    pub cg_layout: CgLayout,
    /// `fs_volname`, which `inspect` shows as the volume label. 4.3BSD has no
    /// such field, so it is ignored there.
    pub label: Option<String>,
}

impl Default for Ufs1FormatParams {
    fn default() -> Self {
        Self {
            size_bytes: 0,
            block_size: 8192,
            frag_size: 1024,
            bytes_per_inode: 0,
            endian: UfsEndian::Little,
            cg_layout: CgLayout::Modern,
            label: None,
        }
    }
}

/// Everything `newfs` derives from the caller's knobs before it writes a byte.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Ufs1Geometry {
    pub bsize: u64,
    pub fsize: u64,
    pub frag: u64,
    pub inopb: u64,
    pub nindir: u64,
    /// `fs_fsbtodb` — fragment to device-block shift. Zero on NeXTSTEP, whose
    /// device block is the fragment itself.
    pub fsbtodb: u32,
    /// `fs_old_nspf` — device blocks per fragment.
    pub nspf: u64,
    /// `fs_fpg` — fragments per cylinder group.
    pub fpg: u64,
    /// `fs_ipg` — inodes per cylinder group.
    pub ipg: u64,
    pub ncg: u64,
    pub sblkno: u64,
    pub cblkno: u64,
    pub iblkno: u64,
    pub dblkno: u64,
    /// `fs_old_size` — total fragments the filesystem claims.
    pub size_frags: u64,
    /// `fs_old_dsize` — fragments available to files.
    pub dsize_frags: u64,
    pub cssize: u64,
    pub cgsize: u64,
    pub csaddr: u64,
    /// `fs_old_ntrak` / `fs_old_nsect` / `fs_old_spc` / `fs_old_cpg`. Real
    /// geometry on a 4.3BSD volume; a synthetic one-cylinder-per-group shape
    /// on a 4.4BSD one.
    pub ntrak: u64,
    pub nsect: u64,
    pub spc: u64,
    pub cpg: u64,
    pub ncyl: u64,
    /// `fs_old_cgoffset` — fragments each successive group is staggered by.
    pub cgoffset: u64,
    /// Bits of the group number the stagger cycles over, i.e. `~fs_cgmask`.
    pub cgstagger: u64,
    /// `fs_old_cpc` — cylinders after which the rotational pattern repeats.
    pub cpc: u64,
    pub cg_layout: CgLayout,
}

impl Ufs1Geometry {
    /// Fragments the cylinder summary area occupies at the head of CG 0's
    /// data region.
    fn cssize_frags(&self) -> u64 {
        self.cssize.div_ceil(self.fsize)
    }

    /// First fragment cylinder group `c` owns.
    fn cgbase(&self, c: u64) -> u64 {
        c * self.fpg
    }

    /// Where group `c`'s own metadata starts. 4.3BSD staggers it across tracks
    /// so consecutive groups do not put their superblock replicas under the
    /// same head; `fs_cgoffset` is zero on a 4.4BSD volume, so this collapses.
    fn cgstart(&self, c: u64) -> u64 {
        self.cgbase(c) + self.cgoffset * (c & self.cgstagger)
    }

    /// Fragments cylinder group `c` covers; the last one is short.
    fn cg_ndblk(&self, c: u64) -> u64 {
        (self.cgbase(c) + self.fpg).min(self.size_frags) - self.cgbase(c)
    }

    /// Group-relative fragment of the superblock replica. Anything below it is
    /// free data on every group but the first, which keeps the boot blocks.
    fn cg_dlower(&self, c: u64) -> u64 {
        self.cgstart(c) - self.cgbase(c) + self.sblkno
    }

    /// First data fragment of cylinder group `c`, past its own metadata and —
    /// in CG 0 — past the cylinder summary.
    fn cg_dupper(&self, c: u64) -> u64 {
        let base = self.cgstart(c) - self.cgbase(c) + self.dblkno;
        if c == 0 {
            base + self.cssize_frags()
        } else {
            base
        }
    }

    /// Byte offset of `cg_iusedoff` within a 4.4BSD cylinder-group header.
    fn iusedoff(&self) -> u64 {
        CG_SPACE_OFF + OLD_CPG * 4 + OLD_CPG * OLD_NRPOS * 2
    }

    fn freeoff(&self) -> u64 {
        self.iusedoff() + self.ipg.div_ceil(8)
    }

    fn clustersumoff(&self) -> u64 {
        let raw = self.freeoff() + self.fpg.div_ceil(8) - 4;
        raw.next_multiple_of(4)
    }

    fn clusteroff(&self) -> u64 {
        self.clustersumoff() + (CONTIGSUMSIZE + 1) * 4
    }

    fn nextfreeoff(&self) -> u64 {
        self.clusteroff() + (self.fpg / self.frag).div_ceil(8)
    }

    /// Blocks per cylinder, which is what the rotational tables are indexed by.
    fn blocks_per_cylinder(&self) -> u64 {
        self.spc / (self.frag * self.nspf)
    }

    /// Rotational position of a block, by the pre-Tahoe `cbtorpos()` both
    /// reference disks were built with. Their `fs_npsect` is zero, so the
    /// later interleave-aware form cannot be the one NeXTSTEP used — it would
    /// have divided by zero on NeXT's own disks.
    fn rpos_of_block(&self, block: u64) -> u64 {
        let sector = block * self.frag * self.nspf;
        (sector % self.spc % self.nsect) * NRPOS_43 / self.nsect
    }

    fn cylinder_of_block(&self, block: u64) -> u64 {
        block * self.frag * self.nspf / self.spc
    }
}

/// Work out the whole layout, refusing anything FFS cannot express.
pub fn plan(params: &Ufs1FormatParams) -> Result<Ufs1Geometry, FilesystemError> {
    let bsize = params.block_size;
    let fsize = params.frag_size;
    let bad = FilesystemError::InvalidData;

    if !(4096..=65536).contains(&bsize) || !bsize.is_power_of_two() {
        return Err(bad(format!(
            "ufs: block size {bsize} must be a power of two in [4096, 65536]"
        )));
    }
    if fsize < 512 || !fsize.is_power_of_two() || fsize > bsize {
        return Err(bad(format!(
            "ufs: fragment size {fsize} must be a power of two in [512, {bsize}]"
        )));
    }
    let frag = bsize / fsize;
    if frag > MAXFRAG {
        return Err(bad(format!(
            "ufs: {frag} fragments per block exceeds MAXFRAG ({MAXFRAG}); raise --frag-size"
        )));
    }
    let inopb = bsize / DINODE1_SIZE;
    let inopf = inopb / frag;
    if inopf == 0 {
        return Err(bad(format!(
            "ufs: a {fsize}-byte fragment holds no whole inodes at a {bsize}-byte block"
        )));
    }

    let density = if params.bytes_per_inode == 0 {
        4 * fsize
    } else {
        params.bytes_per_inode
    };
    if density < fsize {
        return Err(bad(format!(
            "ufs: --bytes-per-inode {density} is below the {fsize}-byte fragment size"
        )));
    }

    // A 4.3BSD device block is the fragment itself on NeXTSTEP; everything
    // else counts 512-byte ones.
    let nspf = match params.cg_layout {
        CgLayout::Bsd43 => 1,
        CgLayout::Modern => fsize / 512,
    };
    let mut geo = Ufs1Geometry {
        bsize,
        fsize,
        frag,
        inopb,
        nindir: bsize / 4,
        fsbtodb: nspf.trailing_zeros(),
        nspf,
        fpg: 0,
        ipg: 0,
        ncg: 1,
        sblkno: (SB_OFFSET_UFS1 + SBLOCKSIZE)
            .div_ceil(fsize)
            .next_multiple_of(frag),
        cblkno: 0,
        iblkno: 0,
        dblkno: 0,
        size_frags: params.size_bytes / fsize,
        dsize_frags: 0,
        cssize: 0,
        cgsize: 0,
        csaddr: 0,
        ntrak: 0,
        nsect: 0,
        spc: 0,
        cpg: OLD_CPG,
        ncyl: 0,
        cgoffset: 0,
        cgstagger: 0,
        cpc: 0,
        cg_layout: params.cg_layout,
    };
    geo.cblkno = geo.sblkno + SBLOCKSIZE.div_ceil(fsize).next_multiple_of(frag);
    geo.iblkno = geo.cblkno + frag;

    match params.cg_layout {
        CgLayout::Modern => plan_group_modern(&mut geo, density),
        CgLayout::Bsd43 => plan_group_43(&mut geo, density)?,
    }
    geo.dblkno = geo.iblkno + geo.ipg / inopf;

    geo.ncg = geo.size_frags.div_ceil(geo.fpg).max(1);
    // A runt final group with no room for its own metadata is dropped, the
    // way `newfs` shrinks the filesystem rather than describe one.
    while geo.ncg > 1 && geo.cg_ndblk(geo.ncg - 1) < geo.cg_dupper(geo.ncg - 1) + geo.frag {
        geo.ncg -= 1;
        geo.size_frags = geo.ncg * geo.fpg;
    }
    if geo.cg_ndblk(geo.ncg - 1) < geo.cg_dupper(geo.ncg - 1) + geo.frag {
        return Err(bad(format!(
            "ufs: {} is too small for a UFS1 volume (one cylinder group needs {})",
            crate::partition::format_size(params.size_bytes),
            crate::partition::format_size((geo.dblkno + geo.frag) * fsize),
        )));
    }
    geo.ncyl = if geo.spc == 0 {
        geo.ncg
    } else {
        (geo.size_frags * geo.nspf).div_ceil(geo.spc)
    };

    geo.cssize = (geo.ncg * CSUM_SIZE).next_multiple_of(fsize);
    geo.csaddr = geo.cg_dupper(0) - geo.cssize_frags();
    geo.cgsize = match params.cg_layout {
        CgLayout::Modern => geo.nextfreeoff().next_multiple_of(fsize),
        CgLayout::Bsd43 => (CG43_OFF_FREE + geo.fpg.div_ceil(8)).next_multiple_of(fsize),
    };
    if geo.cgsize > bsize {
        return Err(bad(format!(
            "ufs: a {}-fragment cylinder group needs a {}-byte header, past the {bsize}-byte block",
            geo.fpg, geo.cgsize,
        )));
    }
    if geo.cg_dupper(0) + geo.frag > geo.cg_ndblk(0) {
        return Err(bad(format!(
            "ufs: {} cylinder groups need a {}-byte summary, which does not fit in the first group",
            geo.ncg, geo.cssize,
        )));
    }

    geo.dsize_frags = (0..geo.ncg)
        .map(|c| {
            let lower = if c == 0 { 0 } else { geo.cg_dlower(c) };
            lower + geo.cg_ndblk(c) - geo.cg_dupper(c)
        })
        .sum();
    if geo.dsize_frags < MIN_DATA_BLOCKS * geo.frag {
        return Err(bad(format!(
            "ufs: {} leaves only {} for files after the superblock, cylinder group and inodes",
            crate::partition::format_size(params.size_bytes),
            crate::partition::format_size(geo.dsize_frags * fsize),
        )));
    }
    Ok(geo)
}

/// 4.4BSD: the cylinder group is as large as it can be while its header,
/// bitmaps and cluster maps still fit in one block — the constraint `newfs`
/// solves for, and the reason `fs_fpg` is never a round number.
fn plan_group_modern(geo: &mut Ufs1Geometry, density: u64) {
    let fits = |g: &mut Ufs1Geometry, blocks: u64| -> bool {
        g.fpg = blocks * g.frag;
        g.ipg = ipg_for(g.fpg, g.fsize, density, g.inopb);
        g.ipg <= i16::MAX as u64 && g.nextfreeoff() <= g.bsize
    };
    let mut lo = 1u64;
    let mut hi = (geo.size_frags / geo.frag).max(1);
    while lo < hi {
        let mid = lo + (hi - lo).div_ceil(2);
        if fits(geo, mid) {
            lo = mid;
        } else {
            hi = mid - 1;
        }
    }
    fits(geo, lo);
    // One cylinder group is described as one cylinder; rotational layout has
    // been dead since 4.4BSD and every modern FFS records this shape.
    geo.nsect = geo.fpg * geo.nspf;
    geo.spc = geo.fpg * geo.nspf;
    geo.ntrak = 1;
    geo.cpg = OLD_CPG;
}

/// 4.3BSD: real cylinders, so a group is `fs_cpg` of them and the caps come
/// from the kernel's fixed-size `cg_btot` / `cg_b` / `cg_iused` arrays rather
/// than from how much fits in a block.
fn plan_group_43(geo: &mut Ufs1Geometry, density: u64) -> Result<(), FilesystemError> {
    let inopf = geo.inopb / geo.frag;
    for ntrak in NTRAK_43_CHOICES {
        let spc = ntrak * NSECT_43;
        let frags_per_cyl = spc / geo.nspf;
        if frags_per_cyl == 0 || frags_per_cyl > geo.size_frags {
            continue;
        }
        // Grow the group a cylinder at a time while the inode bitmap, the
        // free-fragment bitmap and the group itself all still fit.
        let mut cpg = 0u64;
        for candidate in 1..=MAXCPG_43 {
            let fpg = candidate * frags_per_cyl;
            let ipg = ipg_for_43(fpg, geo.fsize, density, geo.inopb);
            let wanted = fpg * geo.fsize / density;
            if wanted > MAXIPG_43
                || ipg > MAXIPG_43 - geo.inopb
                || CG43_OFF_FREE + fpg.div_ceil(8) > geo.bsize
                || fpg > geo.size_frags
            {
                break;
            }
            cpg = candidate;
        }
        if cpg == 0 {
            continue;
        }
        geo.ntrak = ntrak;
        geo.nsect = NSECT_43;
        geo.spc = spc;
        geo.cpg = cpg;
        geo.fpg = cpg * frags_per_cyl;
        geo.ipg = ipg_for_43(geo.fpg, geo.fsize, density, geo.inopb);
        // `fs_cgoffset` staggers each group by a track so consecutive
        // superblock replicas do not land under the same head; the stagger
        // cycles over the track count, which is what `fs_cgmask` selects.
        geo.cgoffset = NSECT_43.div_ceil(geo.nspf).next_multiple_of(geo.frag);
        geo.cgstagger = ntrak - 1;
        // With no interleave or track skew the rotational pattern repeats
        // every cylinder, which is what both reference disks record.
        geo.cpc = 1;
        if geo.iblkno + geo.ipg / inopf + geo.cgoffset + geo.frag > geo.fpg {
            continue;
        }
        return Ok(());
    }
    Err(FilesystemError::InvalidData(format!(
        "ufs: {} is too small for a 4.3BSD cylinder group",
        crate::partition::format_size(geo.size_frags * geo.fsize),
    )))
}

/// Inodes per group at `density` bytes each, rounded to whole inode blocks.
fn ipg_for(fpg: u64, fsize: u64, density: u64, inopb: u64) -> u64 {
    let wanted = (fpg * fsize).div_ceil(density).max(inopb);
    wanted.next_multiple_of(inopb)
}

/// The same, by NeXTSTEP's rule: the largest whole number of inode blocks
/// *strictly below* the density figure. Both reference disks land exactly one
/// inode block under the round number — 1984 where the density says 2048, and
/// 448 where it says 512 — and matching that is what makes every derived
/// field (`fs_dblkno`, `fs_csaddr`, `fs_dsize`) agree with them too.
fn ipg_for_43(fpg: u64, fsize: u64, density: u64, inopb: u64) -> u64 {
    let target = fpg * fsize / density;
    let mut ipg = target / inopb * inopb;
    if ipg >= target {
        ipg = ipg.saturating_sub(inopb);
    }
    ipg.clamp(inopb, MAXIPG_43 - inopb)
}

/// Format a blank UFS1 volume onto `out`, which must already be at least
/// `params.size_bytes` long.
pub fn write_blank_ufs1<W: Write + Seek>(
    out: &mut W,
    params: &Ufs1FormatParams,
) -> Result<Ufs1Geometry, FilesystemError> {
    let geo = plan(params)?;
    let endian = params.endian;

    // The root directory takes the first whole block of CG 0's data area; the
    // partial fragments ahead of it stay free, exactly as `initcg` leaves them.
    let root_block = geo.cg_dupper(0).next_multiple_of(geo.frag);

    let mut total = Csum::default();
    for c in 0..geo.ncg {
        let cs = write_cylinder_group(out, &geo, endian, c, root_block)?;
        total.add(&cs);
    }

    write_root_directory(out, &geo, endian, root_block)?;
    let sb = build_superblock(&geo, endian, &total, params.label.as_deref());
    out.seek(SeekFrom::Start(SB_OFFSET_UFS1))?;
    out.write_all(&sb)?;
    for c in 0..geo.ncg {
        out.seek(SeekFrom::Start((geo.cgstart(c) + geo.sblkno) * geo.fsize))?;
        out.write_all(&sb)?;
    }
    // The cylinder summary the kernel reads instead of walking every group.
    let mut csum = vec![0u8; geo.cssize as usize];
    for c in 0..geo.ncg {
        let cs = cg_summary(&geo, c, root_block);
        let at = (c * CSUM_SIZE) as usize;
        write_i32(&mut csum, at, cs.ndir as i32, endian);
        write_i32(&mut csum, at + 4, cs.nbfree as i32, endian);
        write_i32(&mut csum, at + 8, cs.nifree as i32, endian);
        write_i32(&mut csum, at + 12, cs.nffree as i32, endian);
    }
    out.seek(SeekFrom::Start(geo.csaddr * geo.fsize))?;
    out.write_all(&csum)?;
    out.flush()?;
    Ok(geo)
}

/// The four counters FFS keeps per group and totals in the superblock.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
struct Csum {
    ndir: u64,
    nbfree: u64,
    nifree: u64,
    nffree: u64,
}

impl Csum {
    fn add(&mut self, o: &Csum) {
        self.ndir += o.ndir;
        self.nbfree += o.nbfree;
        self.nifree += o.nifree;
        self.nffree += o.nffree;
    }
}

/// Free-fragment bitmap for one group, plus its counters — `initcg`'s core.
fn cg_free_map(geo: &Ufs1Geometry, c: u64, root_block: u64) -> (Vec<u8>, Csum, [u32; 8]) {
    let ndblk = geo.cg_ndblk(c);
    let dupper = geo.cg_dupper(c);
    let mut map = vec![0u8; geo.fpg.div_ceil(8) as usize];
    let mut cs = Csum::default();
    let mut frsum = [0u32; 8];
    let set = |m: &mut [u8], i: u64| m[(i / 8) as usize] |= 1 << (i % 8);

    // Group 0 keeps its head for the boot blocks and the primary superblock;
    // every other group has real space there, more of it the further the
    // 4.3BSD stagger has pushed its metadata down.
    if c > 0 {
        let mut d = 0;
        while d + geo.frag <= geo.cg_dlower(c) {
            for f in d..d + geo.frag {
                set(&mut map, f);
            }
            cs.nbfree += 1;
            d += geo.frag;
        }
    }
    // A data area that does not start on a block boundary leaves a run of
    // loose fragments, which is what `cg_frsum` counts.
    let mut d = dupper;
    let partial = d % geo.frag;
    if partial != 0 {
        let run = geo.frag - partial;
        frsum[run as usize % 8] += 1;
        for f in d..d + run {
            set(&mut map, f);
            cs.nffree += 1;
        }
        d += run;
    }
    while d + geo.frag <= ndblk {
        if !(c == 0 && d == root_block) {
            for f in d..d + geo.frag {
                set(&mut map, f);
            }
            cs.nbfree += 1;
        }
        d += geo.frag;
    }
    if d < ndblk {
        frsum[(ndblk - d) as usize % 8] += 1;
        for f in d..ndblk {
            set(&mut map, f);
            cs.nffree += 1;
        }
    }
    (map, cs, frsum)
}

/// Counters for group `c`, without building its header — used for the
/// cylinder summary area and the superblock totals.
fn cg_summary(geo: &Ufs1Geometry, c: u64, root_block: u64) -> Csum {
    let (_, mut cs, _) = cg_free_map(geo, c, root_block);
    cs.nifree = geo.ipg;
    if c == 0 {
        // Inodes 0 and 1 are reserved, 2 is the root directory.
        cs.nifree -= u64::from(ROOT_INODE) + 1;
        cs.ndir = 1;
    }
    cs
}

/// True when block `b` of a group is wholly free in `free`.
fn block_is_free(geo: &Ufs1Geometry, free: &[u8], b: u64) -> bool {
    (0..geo.frag).all(|f| {
        let bit = b * geo.frag + f;
        free[(bit / 8) as usize] & (1 << (bit % 8)) != 0
    })
}

fn write_cylinder_group<W: Write + Seek>(
    out: &mut W,
    geo: &Ufs1Geometry,
    endian: UfsEndian,
    c: u64,
    root_block: u64,
) -> Result<Csum, FilesystemError> {
    let (free, _, frsum) = cg_free_map(geo, c, root_block);
    let cs = cg_summary(geo, c, root_block);
    let ndblk = geo.cg_ndblk(c);

    let mut hdr = vec![0u8; geo.cgsize as usize];
    // `struct cg` up to `cg_frsum` is the same in both generations; only where
    // the maps live after it differs.
    write_u32(&mut hdr, 12, c as u32, endian);
    write_i16(&mut hdr, 16, geo.cpg as i16, endian);
    write_i16(&mut hdr, 18, geo.ipg as i16, endian);
    write_u32(&mut hdr, 20, ndblk as u32, endian);
    write_i32(&mut hdr, 24, cs.ndir as i32, endian);
    write_i32(&mut hdr, 28, cs.nbfree as i32, endian);
    write_i32(&mut hdr, 32, cs.nifree as i32, endian);
    write_i32(&mut hdr, 36, cs.nffree as i32, endian);
    for (i, n) in frsum.iter().enumerate() {
        write_u32(&mut hdr, 52 + i * 4, *n, endian);
    }

    let (iusedoff, freeoff) = match geo.cg_layout {
        CgLayout::Modern => {
            write_i32(&mut hdr, 4, CG_MAGIC as i32, endian);
            write_u32(&mut hdr, 84, CG_SPACE_OFF as u32, endian);
            write_u32(&mut hdr, 88, (CG_SPACE_OFF + OLD_CPG * 4) as u32, endian);
            write_u32(&mut hdr, 92, geo.iusedoff() as u32, endian);
            write_u32(&mut hdr, 96, geo.freeoff() as u32, endian);
            write_u32(&mut hdr, 100, geo.nextfreeoff() as u32, endian);
            write_u32(&mut hdr, 104, geo.clustersumoff() as u32, endian);
            write_u32(&mut hdr, 108, geo.clusteroff() as u32, endian);
            write_u32(&mut hdr, 112, (ndblk / geo.frag) as u32, endian);
            (geo.iusedoff(), geo.freeoff())
        }
        CgLayout::Bsd43 => {
            write_i32(&mut hdr, CG43_OFF_MAGIC, CG_MAGIC as i32, endian);
            (CG43_OFF_IUSED, CG43_OFF_FREE)
        }
    };

    if c == 0 {
        // Inodes 0 and 1 are reserved by FFS itself; 2 is the root directory.
        let at = iusedoff as usize;
        for i in 0..=u64::from(ROOT_INODE) {
            hdr[at + (i / 8) as usize] |= 1 << (i % 8);
        }
    }
    let at = freeoff as usize;
    hdr[at..at + free.len()].copy_from_slice(&free);

    match geo.cg_layout {
        CgLayout::Modern => write_cluster_maps(&mut hdr, geo, &free, ndblk, endian),
        CgLayout::Bsd43 => write_43_rotational_counts(&mut hdr, geo, &free, ndblk, endian),
    }

    // Zero the group's own inode table before the header, so a reformat over
    // an old volume cannot leave a stale dinode behind.
    let inode_bytes = geo.ipg * DINODE1_SIZE;
    let zeros = vec![0u8; geo.bsize as usize];
    out.seek(SeekFrom::Start((geo.cgstart(c) + geo.iblkno) * geo.fsize))?;
    let mut left = inode_bytes;
    while left > 0 {
        let n = left.min(geo.bsize) as usize;
        out.write_all(&zeros[..n])?;
        left -= n as u64;
    }
    out.seek(SeekFrom::Start((geo.cgstart(c) + geo.cblkno) * geo.fsize))?;
    out.write_all(&hdr)?;
    Ok(cs)
}

/// 4.4BSD cluster maps: one bit per wholly-free block, then a histogram of the
/// free runs the allocator can hand out contiguously.
fn write_cluster_maps(
    hdr: &mut [u8],
    geo: &Ufs1Geometry,
    free: &[u8],
    ndblk: u64,
    endian: UfsEndian,
) {
    let clusteroff = geo.clusteroff() as usize;
    let mut runs = [0u32; (CONTIGSUMSIZE + 1) as usize];
    let mut run = 0u64;
    for b in 0..ndblk / geo.frag {
        if block_is_free(geo, free, b) {
            hdr[clusteroff + (b / 8) as usize] |= 1 << (b % 8);
            run += 1;
        } else if run != 0 {
            runs[run.min(CONTIGSUMSIZE) as usize] += 1;
            run = 0;
        }
    }
    if run != 0 {
        runs[run.min(CONTIGSUMSIZE) as usize] += 1;
    }
    let sumoff = geo.clustersumoff() as usize;
    for (i, n) in runs.iter().enumerate() {
        write_u32(hdr, sumoff + i * 4, *n, endian);
    }
}

/// 4.3BSD `cg_btot` / `cg_b`: free blocks per cylinder, and per cylinder per
/// rotational position. The old allocator reads both to place a block under
/// the head about to pass over it, and on both reference disks they sum to
/// `cg_cs.cs_nbfree`.
fn write_43_rotational_counts(
    hdr: &mut [u8],
    geo: &Ufs1Geometry,
    free: &[u8],
    ndblk: u64,
    endian: UfsEndian,
) {
    for b in 0..ndblk / geo.frag {
        if !block_is_free(geo, free, b) {
            continue;
        }
        let cyl = geo.cylinder_of_block(b);
        if cyl >= MAXCPG_43 {
            continue;
        }
        let btot = (CG43_OFF_BTOT + cyl * 4) as usize;
        let now = read_i32_at(hdr, btot, endian) + 1;
        write_i32(hdr, btot, now, endian);
        let slot = (CG43_OFF_B + (cyl * NRPOS_43 + geo.rpos_of_block(b)) * 2) as usize;
        let now = read_i16_at(hdr, slot, endian) + 1;
        write_i16(hdr, slot, now, endian);
    }
}

fn read_i32_at(buf: &[u8], off: usize, endian: UfsEndian) -> i32 {
    let b = [buf[off], buf[off + 1], buf[off + 2], buf[off + 3]];
    match endian {
        UfsEndian::Little => i32::from_le_bytes(b),
        UfsEndian::Big => i32::from_be_bytes(b),
    }
}

fn read_i16_at(buf: &[u8], off: usize, endian: UfsEndian) -> i16 {
    let b = [buf[off], buf[off + 1]];
    match endian {
        UfsEndian::Little => i16::from_le_bytes(b),
        UfsEndian::Big => i16::from_be_bytes(b),
    }
}

/// Root directory plus its dinode: `.` and `..` both pointing at inode 2, in
/// the first `DIRBLKSIZ` of a block-sized allocation.
fn write_root_directory<W: Write + Seek>(
    out: &mut W,
    geo: &Ufs1Geometry,
    endian: UfsEndian,
    root_block: u64,
) -> Result<(), FilesystemError> {
    // A 4.3BSD `struct direct` has a 16-bit `d_namlen` where 4.4BSD put
    // `d_type` and an 8-bit length. Writing the wrong one is invisible here
    // and makes the root directory unreadable on the target system.
    let old_fmt = geo.cg_layout == CgLayout::Bsd43;
    let mut block = vec![0u8; geo.bsize as usize];
    let dot = dirent_record_size(1);
    write_u32(&mut block, 0, ROOT_INODE, endian);
    write_u16(&mut block, 4, dot as u16, endian);
    write_dirent_namlen(&mut block, 0, DT_DIR, 1, old_fmt, endian);
    block[8] = b'.';
    write_u32(&mut block, dot, ROOT_INODE, endian);
    write_u16(&mut block, dot + 4, (DIRBLKSIZ - dot) as u16, endian);
    write_dirent_namlen(&mut block, dot, DT_DIR, 2, old_fmt, endian);
    block[dot + 8] = b'.';
    block[dot + 9] = b'.';
    out.seek(SeekFrom::Start(root_block * geo.fsize))?;
    out.write_all(&block)?;

    let mut dinode = vec![0u8; DINODE1_SIZE as usize];
    write_u16(&mut dinode, D1_OFF_MODE, 0o040755, endian);
    write_i16(&mut dinode, D1_OFF_NLINK, 2, endian);
    write_i64(&mut dinode, D1_OFF_SIZE, DIRBLKSIZ as i64, endian);
    write_i32(&mut dinode, D1_OFF_MTIME, 0, endian);
    write_i32(&mut dinode, D1_OFF_DB, root_block as i32, endian);
    // `di_blocks` counts device blocks, which is the fragment itself on a
    // 4.3BSD / NeXTSTEP volume and 512 bytes everywhere else.
    let dev_bsize = geo.fsize / geo.nspf;
    write_i32(
        &mut dinode,
        D1_OFF_BLOCKS,
        (geo.bsize / dev_bsize) as i32,
        endian,
    );
    write_u32(&mut dinode, D1_OFF_UID, 0, endian);
    write_u32(&mut dinode, D1_OFF_GID, 0, endian);

    let at = (geo.iblkno * geo.fsize) + u64::from(ROOT_INODE) * DINODE1_SIZE;
    out.seek(SeekFrom::Start(at))?;
    out.write_all(&dinode)?;
    Ok(())
}

fn build_superblock(
    geo: &Ufs1Geometry,
    endian: UfsEndian,
    total: &Csum,
    label: Option<&str>,
) -> Vec<u8> {
    let mut sb = vec![0u8; SB_WRITE_SIZE];
    let put = |sb: &mut Vec<u8>, off: usize, v: i64| write_i32(sb, off, v as i32, endian);

    // Everything up to `fs_old_cstotal` is common to both generations: the
    // field names diverge (`fs_old_*` versus the 4.3BSD originals) but the
    // offsets and meanings do not.
    put(&mut sb, 0x008, geo.sblkno as i64);
    put(&mut sb, 0x00C, geo.cblkno as i64);
    put(&mut sb, 0x010, geo.iblkno as i64);
    put(&mut sb, 0x014, geo.dblkno as i64);
    put(&mut sb, 0x018, geo.cgoffset as i64);
    put(&mut sb, 0x01C, !(geo.cgstagger as i64));
    put(&mut sb, 0x024, geo.size_frags as i64);
    put(&mut sb, 0x028, geo.dsize_frags as i64);
    put(&mut sb, 0x02C, geo.ncg as i64);
    put(&mut sb, 0x030, geo.bsize as i64);
    put(&mut sb, 0x034, geo.fsize as i64);
    put(&mut sb, 0x038, geo.frag as i64);
    put(&mut sb, 0x044, 60); // fs_old_rps
    put(&mut sb, 0x048, -(geo.bsize as i64)); // fs_bmask
    put(&mut sb, 0x04C, -(geo.fsize as i64)); // fs_fmask
    put(&mut sb, 0x050, geo.bsize.trailing_zeros() as i64);
    put(&mut sb, 0x054, geo.fsize.trailing_zeros() as i64);
    put(&mut sb, 0x060, geo.frag.trailing_zeros() as i64);
    put(&mut sb, 0x064, i64::from(geo.fsbtodb));
    put(&mut sb, 0x074, geo.nindir as i64);
    put(&mut sb, 0x078, geo.inopb as i64);
    put(&mut sb, 0x07C, geo.nspf as i64);
    put(&mut sb, 0x080, 0); // fs_optim = FS_OPTTIME
    put(&mut sb, 0x098, geo.csaddr as i64);
    put(&mut sb, 0x09C, geo.cssize as i64);
    put(&mut sb, 0x0A0, geo.cgsize as i64);
    put(&mut sb, 0x0A8, geo.nsect as i64);
    put(&mut sb, 0x0AC, geo.spc as i64);
    put(&mut sb, 0x0B0, geo.ncyl as i64);
    put(&mut sb, 0x0B4, geo.cpg as i64);
    put(&mut sb, 0x0B8, geo.ipg as i64);
    put(&mut sb, 0x0BC, geo.fpg as i64);
    put(&mut sb, 0x0C0, total.ndir as i64);
    put(&mut sb, 0x0C4, total.nbfree as i64);
    put(&mut sb, 0x0C8, total.nifree as i64);
    put(&mut sb, 0x0CC, total.nffree as i64);
    sb[0x0D1] = 1; // fs_clean
    put(&mut sb, MAGIC_OFF, MAGIC_UFS1 as i64);

    match geo.cg_layout {
        CgLayout::Modern => build_superblock_modern_tail(&mut sb, geo, endian, total, label),
        CgLayout::Bsd43 => build_superblock_43_tail(&mut sb, geo, endian),
    }
    sb
}

fn build_superblock_modern_tail(
    sb: &mut Vec<u8>,
    geo: &Ufs1Geometry,
    endian: UfsEndian,
    total: &Csum,
    label: Option<&str>,
) {
    let put = |sb: &mut Vec<u8>, off: usize, v: i64| write_i32(sb, off, v as i32, endian);
    put(sb, 0x03C, MINFREE as i64);
    put(sb, 0x058, MAXCONTIG as i64);
    put(sb, 0x05C, MAXBPG as i64);
    put(sb, 0x068, STRUCT_FS_SIZE.next_multiple_of(geo.fsize) as i64);
    put(sb, 0x084, geo.spc as i64); // fs_old_npsect
    put(sb, 0x088, 1); // fs_old_interleave
    sb[0x0D3] = FS_FLAGS_UPDATED;

    if let Some(name) = label {
        let bytes = name.as_bytes();
        let n = bytes.len().min(VOLNAME_LEN - 1);
        sb[OFF_VOLNAME..OFF_VOLNAME + n].copy_from_slice(&bytes[..n]);
    }

    write_i64(sb, 1000, SB_OFFSET_UFS1 as i64, endian);
    write_i64(sb, 1008, total.ndir as i64, endian);
    write_i64(sb, 1016, total.nbfree as i64, endian);
    write_i64(sb, 1024, total.nifree as i64, endian);
    write_i64(sb, 1032, total.nffree as i64, endian);
    write_i64(sb, 1080, geo.size_frags as i64, endian);
    write_i64(sb, 1088, geo.dsize_frags as i64, endian);
    write_i64(sb, 1096, geo.csaddr as i64, endian);
    put(sb, 1196, AVGFILESIZE as i64);
    put(sb, 1200, AVGFPDIR as i64);
    put(sb, 1316, CONTIGSUMSIZE as i64);
    put(sb, OFF_MAXSYMLINKLEN, 60);
    put(sb, 1324, INODEFMT_44BSD as i64);
    write_i64(sb, 1328, max_file_size(geo), endian);
    write_i64(sb, 1336, (geo.bsize - 1) as i64, endian); // fs_qbmask
    write_i64(sb, 1344, (geo.fsize - 1) as i64, endian); // fs_qfmask
    put(sb, 1356, 1); // fs_old_postblformat = FS_DYNAMICPOSTBLFMT
    put(sb, 1360, OLD_NRPOS as i64);
}

/// The 4.3BSD tail: no 64-bit mirrors, no volume name, no `fs_maxsymlinklen`
/// — the bytes 4.4BSD put those in are `fs_postbl`, which is why a reader has
/// to tell the generations apart by where `cg_magic` sits rather than by any
/// superblock field.
fn build_superblock_43_tail(sb: &mut Vec<u8>, geo: &Ufs1Geometry, endian: UfsEndian) {
    let put = |sb: &mut Vec<u8>, off: usize, v: i64| write_i32(sb, off, v as i32, endian);
    let rotbl_len = geo.cpc * geo.blocks_per_cylinder();
    put(sb, 0x03C, MINFREE_43 as i64);
    put(sb, 0x058, 1); // fs_maxcontig
    put(sb, 0x05C, MAXBPG_43 as i64);
    put(
        sb,
        0x068,
        (STRUCT_FS_SIZE + rotbl_len).next_multiple_of(geo.fsize) as i64,
    );
    // `fs_csmask` / `fs_csshift` index the summary area; 4.4BSD dropped them.
    let per_block = geo.bsize / CSUM_SIZE;
    put(sb, 0x06C, -(per_block as i64));
    put(sb, 0x070, per_block.trailing_zeros() as i64);
    put(sb, 0x0A4, geo.ntrak as i64); // fs_ntrak, where 4.4BSD has fs_spare2
    put(sb, FS43_OFF_CPC, geo.cpc as i64);
    // `fs_fsmnt` is the last mount point; both reference disks record "/".
    sb[0x0D4] = b'/';

    // `fs_postbl[cyl][rpos]` is the first block of the cycle at that position
    // and `fs_rotbl[blk]` the distance to the next block sharing it. Unused
    // slots stay -1, which is also what makes the reader's
    // `fs_maxsymlinklen <= 0` probe land on a negative word and so pick the
    // old directory format.
    for i in 0..(MAXCPG_43 * NRPOS_43) as usize {
        write_i16(sb, FS43_OFF_POSTBL + i * 2, -1, endian);
    }
    let mut last: Vec<Option<u64>> = vec![None; (MAXCPG_43 * NRPOS_43) as usize];
    for b in 0..rotbl_len {
        let slot = (geo.cylinder_of_block(b) * NRPOS_43 + geo.rpos_of_block(b)) as usize;
        match last[slot] {
            None => write_i16(sb, FS43_OFF_POSTBL + slot * 2, b as i16, endian),
            Some(prev) => sb[FS43_OFF_ROTBL + prev as usize] = (b - prev) as u8,
        }
        last[slot] = Some(b);
    }
}

/// `fs_maxfilesize`: what the twelve direct blocks plus three levels of
/// indirection can address, less one.
fn max_file_size(geo: &Ufs1Geometry) -> i64 {
    let mut blocks: u128 = 12;
    let mut level: u128 = u128::from(geo.nindir);
    for _ in 0..3 {
        blocks += level;
        level = level.saturating_mul(u128::from(geo.nindir));
    }
    let bytes = blocks.saturating_mul(u128::from(geo.bsize));
    (bytes.min(i64::MAX as u128) as i64).saturating_sub(1)
}

/// Convenience wrapper for callers that want the whole volume in memory.
/// Only sensible for small volumes — `write_blank_ufs1` streams instead.
pub fn create_blank_ufs1(params: &Ufs1FormatParams) -> Result<Vec<u8>, FilesystemError> {
    let mut buf = std::io::Cursor::new(vec![0u8; params.size_bytes as usize]);
    write_blank_ufs1(&mut buf, params)?;
    Ok(buf.into_inner())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::fs::filesystem::{CreateDirectoryOptions, EditableFilesystem, Filesystem};
    use crate::fs::ufs::{UfsFilesystem, UfsVersion};
    use crate::fs::ufs_fsck::fsck_ufs;
    use std::io::Cursor;

    fn params(size: u64) -> Ufs1FormatParams {
        Ufs1FormatParams {
            size_bytes: size,
            label: Some("rusty-backup".to_string()),
            ..Default::default()
        }
    }

    fn params_43(size: u64) -> Ufs1FormatParams {
        Ufs1FormatParams {
            size_bytes: size,
            cg_layout: CgLayout::Bsd43,
            endian: UfsEndian::Big,
            ..Default::default()
        }
    }

    /// The geometry our planner derives has to match what `makefs` derived for
    /// the same inputs, or our volume is only self-consistent.
    #[test]
    fn geometry_matches_the_makefs_fixture() {
        // makefs sized the fixture's inodes to its content, so match that
        // density rather than newfs's; everything else is pure derivation.
        let geo = plan(&Ufs1FormatParams {
            size_bytes: 16 * 1024 * 1024,
            bytes_per_inode: 16 * 1024 * 1024 / 64,
            ..params(0)
        })
        .expect("plan");
        assert_eq!(geo.sblkno, 16);
        assert_eq!(geo.cblkno, 24);
        assert_eq!(geo.iblkno, 32);
        assert_eq!(geo.dblkno, 40);
        assert_eq!(geo.ipg, 64);
        assert_eq!(geo.fpg, 16384);
        assert_eq!(geo.ncg, 1);
        assert_eq!(geo.size_frags, 16384);
        assert_eq!(geo.cssize, 1024);
        assert_eq!(geo.cgsize, 3072);
        assert_eq!(geo.csaddr, 40);
        assert_eq!(geo.dsize_frags, 16343);
        assert_eq!(geo.nextfreeoff(), 2520);
        assert_eq!(geo.clustersumoff(), 2228);
        assert_eq!(geo.clusteroff(), 2264);
        assert_eq!(geo.iusedoff(), 174);
        assert_eq!(geo.freeoff(), 182);
    }

    /// The 4.3BSD geometry is pinned against the NeXTSTEP/Intel reference
    /// disk: 16 tracks of 32 sectors, 16-cylinder groups, and the stagger and
    /// mask that follow from them.
    #[test]
    fn geometry_matches_the_nextstep_reference_disk() {
        let geo = plan(&params_43(660 * 1024 * 1024)).expect("plan");
        assert_eq!(geo.cg_layout, CgLayout::Bsd43);
        assert_eq!((geo.ntrak, geo.nsect, geo.spc), (16, 32, 512));
        assert_eq!(geo.cpg, 16);
        assert_eq!(geo.fpg, 8192);
        assert_eq!(geo.ipg, 1984, "one inode block under the density figure");
        assert_eq!(geo.nspf, 1, "a NeXT device block is the fragment");
        assert_eq!(geo.fsbtodb, 0);
        assert_eq!(geo.cgoffset, 32, "one track, as fs_cgoffset");
        assert_eq!(geo.cgstagger, 15, "fs_cgmask is ~(ntrak - 1)");
        assert_eq!(geo.cpc, 1);
        assert_eq!((geo.sblkno, geo.cblkno, geo.iblkno), (16, 24, 32));
        // 984 + fpg/8, fragment-rounded — the 4.3BSD CGSIZE, not the 4.4 one.
        assert_eq!(geo.cgsize, 2048);
    }

    /// `fs_postbl` and `fs_rotbl` are reproduced from the reference disk, not
    /// invented: the first block of the cycle at each rotational position, and
    /// the distance from each block to the next sharing its position.
    #[test]
    fn the_rotational_tables_match_the_reference_disk() {
        let geo = plan(&params_43(660 * 1024 * 1024)).expect("plan");
        let sb = build_superblock(&geo, UfsEndian::Big, &Csum::default(), None);
        let post: Vec<i16> = (0..8)
            .map(|i| read_i16_at(&sb, FS43_OFF_POSTBL + i * 2, UfsEndian::Big))
            .collect();
        assert_eq!(post, vec![0, -1, 1, -1, 2, -1, 3, -1]);
        let nblk = (geo.cpc * geo.blocks_per_cylinder()) as usize;
        let rot = &sb[FS43_OFF_ROTBL..FS43_OFF_ROTBL + nblk];
        assert!(rot[..nblk - 4].iter().all(|&v| v == 4), "{rot:?}");
        assert!(rot[nblk - 4..].iter().all(|&v| v == 0), "{rot:?}");
        // The word the reader probes for `fs_maxsymlinklen` has to land on an
        // unused, negative `fs_postbl` slot or the old dirent format is missed.
        assert_eq!(read_i32_at(&sb, OFF_MAXSYMLINKLEN, UfsEndian::Big), -1);
    }

    /// Consecutive groups must not put their superblock replicas under the
    /// same head, which is the whole point of `fs_cgoffset`.
    #[test]
    fn cylinder_groups_are_staggered_across_tracks() {
        let geo = plan(&params_43(128 * 1024 * 1024)).expect("plan");
        assert!(geo.ncg > 2);
        for c in 0..geo.ncg.min(4) {
            assert_eq!(geo.cgstart(c), geo.cgbase(c) + 32 * (c % 16));
        }
        // Group 0 has no room below its replica; later ones do, and it is
        // free data rather than a hole.
        assert!(geo.cg_dlower(1) > geo.cg_dlower(0));
    }

    #[test]
    fn a_fresh_volume_opens_and_lists_an_empty_root() {
        let img = create_blank_ufs1(&params(16 * 1024 * 1024)).expect("format");
        let mut fs = UfsFilesystem::open(Cursor::new(img), 0).expect("open");
        assert_eq!(fs.version(), UfsVersion::Ufs1);
        let root = fs.root().expect("root");
        assert!(fs.list_directory(&root).expect("ls").is_empty());
        assert_eq!(fs.volume_label(), Some("rusty-backup"));
    }

    /// A 4.3BSD volume has to be *detected* as one, or the reader walks its
    /// cylinder groups at 4.4BSD offsets and reads its directories with a
    /// `d_type` byte that is really half a name length.
    #[test]
    fn a_43bsd_volume_is_detected_as_one_and_lists_its_root() {
        let img = create_blank_ufs1(&params_43(32 * 1024 * 1024)).expect("format");
        let mut fs = UfsFilesystem::open(Cursor::new(img), 0).expect("open");
        assert_eq!(fs.cg_layout, CgLayout::Bsd43);
        assert!(fs.old_dirent_fmt, "16-bit d_namlen, no d_type");
        assert_eq!(fs.endian(), UfsEndian::Big);
        let root = fs.root().expect("root");
        assert!(fs.list_directory(&root).expect("ls").is_empty());
    }

    #[test]
    fn a_fresh_volume_fscks_clean() {
        for size in [
            1024 * 1024u64,
            16 * 1024 * 1024,
            64 * 1024 * 1024,
            128 * 1024 * 1024,
        ] {
            let img = create_blank_ufs1(&params(size)).expect("format");
            let mut fs = UfsFilesystem::open(Cursor::new(img), 0).expect("open");
            let report = fsck_ufs(&mut fs).expect("fsck");
            assert!(
                report.errors.is_empty() && report.warnings.is_empty(),
                "{size} byte volume: {:?} / {:?}",
                report.errors,
                report.warnings,
            );
        }
    }

    #[test]
    fn a_fresh_43bsd_volume_fscks_clean() {
        for size in [8 * 1024 * 1024u64, 32 * 1024 * 1024, 128 * 1024 * 1024] {
            let img = create_blank_ufs1(&params_43(size)).expect("format");
            let mut fs = UfsFilesystem::open(Cursor::new(img), 0).expect("open");
            let report = fsck_ufs(&mut fs).expect("fsck");
            assert!(
                report.errors.is_empty() && report.warnings.is_empty(),
                "{size} byte 4.3BSD volume: {:?} / {:?}",
                report.errors,
                report.warnings,
            );
        }
    }

    /// Big-endian is a separate code path in every reader we have, so it gets
    /// the same round trip rather than a spot check on the magic.
    #[test]
    fn a_big_endian_volume_round_trips() {
        let img = create_blank_ufs1(&Ufs1FormatParams {
            endian: UfsEndian::Big,
            ..params(32 * 1024 * 1024)
        })
        .expect("format");
        let mut fs = UfsFilesystem::open(Cursor::new(img), 0).expect("open");
        assert_eq!(fs.endian(), UfsEndian::Big);
        let root = fs.root().expect("root");
        assert!(fs.list_directory(&root).expect("ls").is_empty());
    }

    /// A volume that only reads back is not a filesystem. Writing into it and
    /// checking afterwards is what proves the free maps and counters agree.
    #[test]
    fn a_fresh_volume_takes_a_directory_and_stays_clean() {
        for p in [params(32 * 1024 * 1024), params_43(32 * 1024 * 1024)] {
            let mut backing = create_blank_ufs1(&p).expect("format");
            {
                let mut fs = UfsFilesystem::open(Cursor::new(&mut backing), 0).expect("open");
                let root = fs.root().expect("root");
                fs.create_directory(&root, "docs", &CreateDirectoryOptions::default())
                    .expect("mkdir");
            }
            let mut fs = UfsFilesystem::open(Cursor::new(&mut backing), 0).expect("re-open");
            let root = fs.root().expect("root");
            let kids = fs.list_directory(&root).expect("ls");
            assert_eq!(kids.len(), 1, "{:?}", p.cg_layout);
            assert_eq!(kids[0].name, "docs");
            let report = fsck_ufs(&mut fs).expect("fsck");
            assert!(
                report.errors.is_empty() && report.warnings.is_empty(),
                "{:?}: {:?} / {:?}",
                p.cg_layout,
                report.errors,
                report.warnings,
            );
        }
    }

    #[test]
    fn a_volume_too_small_for_one_group_is_refused() {
        assert!(plan(&params(64 * 1024)).is_err());
        assert!(plan(&params_43(64 * 1024)).is_err());
    }

    /// Several groups is the case the single-group fixture cannot cover: the
    /// last one is short, only group 0 gives up space to the summary area, and
    /// on 4.3BSD the stagger moves every group's metadata.
    #[test]
    fn a_multi_group_volume_accounts_for_every_group() {
        for geo in [
            plan(&params(300 * 1024 * 1024)).expect("plan"),
            plan(&params_43(300 * 1024 * 1024)).expect("plan 4.3"),
        ] {
            assert!(geo.ncg > 1, "300 MiB should need several groups");
            let by_hand: u64 = (0..geo.ncg)
                .map(|c| {
                    let lower = if c == 0 { 0 } else { geo.cg_dlower(c) };
                    lower + geo.cg_ndblk(c) - geo.cg_dupper(c)
                })
                .sum();
            assert_eq!(geo.dsize_frags, by_hand);
            assert!(geo.dsize_frags < geo.size_frags);
        }
    }
}
