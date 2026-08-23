//! `newfs` for UFS1 — lay a blank BSD Fast File System down on blank media.
//!
//! Reached from `rb-cli new volume ufs`. The reader, editor and fsck all live
//! in [`crate::fs::ufs`]; this module only writes the initial state, following
//! the same `initcg` / `fsinit` sequence BSD's own `newfs(8)` runs, so a
//! volume we format is the one a real `newfs` would have produced for the same
//! geometry.
//!
//! **UFS1 with the 4.4BSD `struct cg` only**, in either byte order. The
//! pre-4.4BSD generation (NeXTSTEP, SunOS 4) is read and edited but not
//! created: its superblock carries rotational-layout tables (`fs_postbl` /
//! `fs_rotbl` / `fs_npsect`) that are inputs to an allocator we cannot test
//! against, and a plausible-looking wrong table is worse than no format verb.
//! See `docs/partition_table_writers_backlog.md`.
//!
//! Geometry follows `newfs`: 8 KiB blocks over 1 KiB fragments, one inode per
//! 4 fragments, and the largest cylinder group whose header, inode bitmap,
//! free-fragment bitmap and cluster maps all fit inside one block — which is
//! the constraint that decides `fs_fpg` on a real FFS.

use std::io::{Seek, SeekFrom, Write};

use super::filesystem::FilesystemError;
use super::ufs::{
    dirent_record_size, write_i32, write_i64, write_u16, write_u32, CgLayout, UfsEndian,
    D1_OFF_BLOCKS, D1_OFF_DB, D1_OFF_GID, D1_OFF_MODE, D1_OFF_MTIME, D1_OFF_NLINK, D1_OFF_SIZE,
    D1_OFF_UID, DINODE1_SIZE, DIRBLKSIZ, DT_DIR, MAGIC_OFF, MAGIC_UFS1, OFF_MAXSYMLINKLEN,
    OFF_VOLNAME, ROOT_INODE, SB_OFFSET_UFS1, VOLNAME_LEN,
};

/// `SBLOCKSIZE` — the space FFS reserves for a superblock, whatever the
/// struct's actual size.
const SBLOCKSIZE: u64 = 8192;
/// Bytes we lay down per superblock copy. `struct fs` ends at 1376; 2048 is
/// what `fragroundup` gives for the default 1 KiB fragment and what the
/// reader's `SB_READ_SIZE` expects to find.
const SB_WRITE_SIZE: usize = 2048;
/// `sizeof(struct fs)` for UFS1, which is what `fs_sbsize` rounds up.
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

/// `fs_old_cpg` / `fs_old_nrpos`: we describe one cylinder group as one
/// "cylinder" with a single rotational position, which is what every FFS
/// built since rotational optimisation was abandoned records.
const OLD_CPG: u64 = 1;
const OLD_NRPOS: u64 = 1;

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
    /// `fs_volname`, which `inspect` shows as the volume label.
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
    pub fsbtodb: u32,
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
    /// The cylinder-group generation this geometry describes.
    pub cg_layout: CgLayout,
}

impl Ufs1Geometry {
    /// Fragments the cylinder summary area occupies at the head of CG 0's
    /// data region.
    fn cssize_frags(&self) -> u64 {
        self.cssize.div_ceil(self.fsize)
    }

    /// First fragment of cylinder group `c`.
    fn cgbase(&self, c: u64) -> u64 {
        c * self.fpg
    }

    /// Fragments cylinder group `c` covers; the last one is short.
    fn cg_ndblk(&self, c: u64) -> u64 {
        (self.cgbase(c) + self.fpg).min(self.size_frags) - self.cgbase(c)
    }

    /// First data fragment of cylinder group `c`, past its own metadata and —
    /// in CG 0 — past the cylinder summary.
    fn cg_dupper(&self, c: u64) -> u64 {
        if c == 0 {
            self.dblkno + self.cssize_frags()
        } else {
            self.dblkno
        }
    }

    /// Byte offset of `cg_iusedoff` within a cylinder-group header.
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
}

/// Work out the whole layout, refusing anything FFS cannot express.
pub fn plan(params: &Ufs1FormatParams) -> Result<Ufs1Geometry, FilesystemError> {
    let bsize = params.block_size;
    let fsize = params.frag_size;
    let bad = |m: String| FilesystemError::InvalidData(m);

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

    let size_frags = params.size_bytes / fsize;
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

    let sblkno = (SB_OFFSET_UFS1 + SBLOCKSIZE)
        .div_ceil(fsize)
        .next_multiple_of(frag);
    let cblkno = sblkno + SBLOCKSIZE.div_ceil(fsize).next_multiple_of(frag);
    let iblkno = cblkno + frag;

    // The cylinder group is as large as it can be while its header, bitmaps
    // and cluster maps still fit in one block — the constraint `newfs` solves
    // for, and the reason `fs_fpg` is not a round number.
    let mut probe = Ufs1Geometry {
        bsize,
        fsize,
        frag,
        inopb,
        nindir: bsize / 4,
        fsbtodb: (fsize / 512).trailing_zeros(),
        fpg: 0,
        ipg: 0,
        ncg: 1,
        sblkno,
        cblkno,
        iblkno,
        dblkno: 0,
        size_frags,
        dsize_frags: 0,
        cssize: 0,
        cgsize: 0,
        csaddr: 0,
        cg_layout: CgLayout::Modern,
    };
    let fits = |g: &mut Ufs1Geometry, blocks: u64| -> bool {
        g.fpg = blocks * frag;
        g.ipg = ipg_for(g.fpg, fsize, density, inopb);
        g.ipg <= i16::MAX as u64 && g.nextfreeoff() <= bsize
    };
    let mut lo = 1u64;
    let mut hi = (size_frags / frag).max(1);
    if !fits(&mut probe, lo) {
        return Err(bad(format!(
            "ufs: a {bsize}-byte block cannot hold even one cylinder group's maps"
        )));
    }
    while lo < hi {
        let mid = lo + (hi - lo).div_ceil(2);
        if fits(&mut probe, mid) {
            lo = mid;
        } else {
            hi = mid - 1;
        }
    }
    fits(&mut probe, lo);

    let mut geo = probe;
    geo.dblkno = iblkno + geo.ipg / inopf;
    geo.ncg = size_frags.div_ceil(geo.fpg).max(1);

    // A runt final group with no room for its own metadata is dropped, the
    // way `newfs` shrinks the filesystem rather than describe one.
    while geo.ncg > 1 && geo.cg_ndblk(geo.ncg - 1) < geo.dblkno + geo.frag {
        geo.ncg -= 1;
        geo.size_frags = geo.ncg * geo.fpg;
    }
    if geo.cg_ndblk(geo.ncg - 1) < geo.dblkno + geo.frag {
        return Err(bad(format!(
            "ufs: {} is too small for a UFS1 volume (one cylinder group needs {})",
            crate::partition::format_size(params.size_bytes),
            crate::partition::format_size((geo.dblkno + geo.frag) * fsize),
        )));
    }

    geo.cssize = (geo.ncg * CSUM_SIZE).next_multiple_of(fsize);
    geo.csaddr = geo.dblkno;
    geo.cgsize = geo.nextfreeoff().next_multiple_of(fsize);
    if geo.cg_dupper(0) + geo.frag > geo.cg_ndblk(0) {
        return Err(bad(format!(
            "ufs: {} cylinder groups need a {}-byte summary, which does not fit in the first group",
            geo.ncg, geo.cssize,
        )));
    }

    geo.dsize_frags = (0..geo.ncg)
        .map(|c| {
            let lower = if c == 0 { 0 } else { geo.sblkno };
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

/// Inodes per group at `density` bytes each, rounded to whole inode blocks.
fn ipg_for(fpg: u64, fsize: u64, density: u64, inopb: u64) -> u64 {
    let wanted = (fpg * fsize).div_ceil(density).max(inopb);
    wanted.next_multiple_of(inopb)
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
        out.seek(SeekFrom::Start((geo.cgbase(c) + geo.sblkno) * geo.fsize))?;
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
    // every other group has real space there.
    if c > 0 {
        let mut d = 0;
        while d < geo.sblkno {
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

fn write_cylinder_group<W: Write + Seek>(
    out: &mut W,
    geo: &Ufs1Geometry,
    endian: UfsEndian,
    c: u64,
    root_block: u64,
) -> Result<Csum, FilesystemError> {
    use super::ufs::{CG_MAGIC, CG_OFF_MAGIC};

    let (free, _, frsum) = cg_free_map(geo, c, root_block);
    let cs = cg_summary(geo, c, root_block);
    let ndblk = geo.cg_ndblk(c);
    let nclusterblks = ndblk / geo.frag;

    let mut hdr = vec![0u8; geo.cgsize as usize];
    write_i32(&mut hdr, CG_OFF_MAGIC, CG_MAGIC as i32, endian);
    write_u32(&mut hdr, 12, c as u32, endian);
    write_u16(&mut hdr, 16, OLD_CPG as u16, endian);
    write_u16(&mut hdr, 18, geo.ipg as u16, endian);
    write_u32(&mut hdr, 20, ndblk as u32, endian);
    write_i32(&mut hdr, 24, cs.ndir as i32, endian);
    write_i32(&mut hdr, 28, cs.nbfree as i32, endian);
    write_i32(&mut hdr, 32, cs.nifree as i32, endian);
    write_i32(&mut hdr, 36, cs.nffree as i32, endian);
    for (i, n) in frsum.iter().enumerate() {
        write_u32(&mut hdr, 52 + i * 4, *n, endian);
    }
    write_u32(&mut hdr, 84, CG_SPACE_OFF as u32, endian);
    write_u32(&mut hdr, 88, (CG_SPACE_OFF + OLD_CPG * 4) as u32, endian);
    write_u32(&mut hdr, 92, geo.iusedoff() as u32, endian);
    write_u32(&mut hdr, 96, geo.freeoff() as u32, endian);
    write_u32(&mut hdr, 100, geo.nextfreeoff() as u32, endian);
    write_u32(&mut hdr, 104, geo.clustersumoff() as u32, endian);
    write_u32(&mut hdr, 108, geo.clusteroff() as u32, endian);
    write_u32(&mut hdr, 112, nclusterblks as u32, endian);

    if c == 0 {
        // Inodes 0 and 1 are reserved by FFS itself; 2 is the root directory.
        let iused = geo.iusedoff() as usize;
        for i in 0..=u64::from(ROOT_INODE) {
            hdr[iused + (i / 8) as usize] |= 1 << (i % 8);
        }
    }
    let freeoff = geo.freeoff() as usize;
    hdr[freeoff..freeoff + free.len()].copy_from_slice(&free);

    // Cluster maps: one bit per wholly-free block, then a histogram of the
    // free runs the allocator can hand out contiguously.
    let clusteroff = geo.clusteroff() as usize;
    let mut runs = [0u32; (CONTIGSUMSIZE + 1) as usize];
    let mut run = 0u64;
    for b in 0..nclusterblks {
        let whole = (0..geo.frag).all(|f| {
            let bit = b * geo.frag + f;
            free[(bit / 8) as usize] & (1 << (bit % 8)) != 0
        });
        if whole {
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
        write_u32(&mut hdr, sumoff + i * 4, *n, endian);
    }

    // Zero the group's own inode table before the header, so a reformat over
    // an old volume cannot leave a stale dinode behind.
    let inode_bytes = geo.ipg * DINODE1_SIZE;
    let zeros = vec![0u8; geo.bsize as usize];
    out.seek(SeekFrom::Start((geo.cgbase(c) + geo.iblkno) * geo.fsize))?;
    let mut left = inode_bytes;
    while left > 0 {
        let n = left.min(geo.bsize) as usize;
        out.write_all(&zeros[..n])?;
        left -= n as u64;
    }
    out.seek(SeekFrom::Start((geo.cgbase(c) + geo.cblkno) * geo.fsize))?;
    out.write_all(&hdr)?;
    Ok(cs)
}

/// Root directory plus its dinode: `.` and `..` both pointing at inode 2, in
/// the first `DIRBLKSIZ` of a block-sized allocation.
fn write_root_directory<W: Write + Seek>(
    out: &mut W,
    geo: &Ufs1Geometry,
    endian: UfsEndian,
    root_block: u64,
) -> Result<(), FilesystemError> {
    let mut block = vec![0u8; geo.bsize as usize];
    let dot = dirent_record_size(1);
    write_u32(&mut block, 0, ROOT_INODE, endian);
    write_u16(&mut block, 4, dot as u16, endian);
    block[6] = DT_DIR;
    block[7] = 1;
    block[8] = b'.';
    write_u32(&mut block, dot, ROOT_INODE, endian);
    write_u16(&mut block, dot + 4, (DIRBLKSIZ - dot) as u16, endian);
    block[dot + 6] = DT_DIR;
    block[dot + 7] = 2;
    block[dot + 8] = b'.';
    block[dot + 9] = b'.';
    out.seek(SeekFrom::Start(root_block * geo.fsize))?;
    out.write_all(&block)?;

    let mut dinode = vec![0u8; DINODE1_SIZE as usize];
    write_u16(&mut dinode, D1_OFF_MODE, 0o040755, endian);
    super::ufs::write_i16(&mut dinode, D1_OFF_NLINK, 2, endian);
    write_i64(&mut dinode, D1_OFF_SIZE, DIRBLKSIZ as i64, endian);
    write_i32(&mut dinode, D1_OFF_MTIME, 0, endian);
    write_i32(&mut dinode, D1_OFF_DB, root_block as i32, endian);
    // `di_blocks` counts 512-byte device blocks, whatever the fragment size.
    write_i32(&mut dinode, D1_OFF_BLOCKS, (geo.bsize / 512) as i32, endian);
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
    let sbsize = STRUCT_FS_SIZE.next_multiple_of(geo.fsize);
    let put = |sb: &mut Vec<u8>, off: usize, v: i64| write_i32(sb, off, v as i32, endian);

    put(&mut sb, 0x008, geo.sblkno as i64);
    put(&mut sb, 0x00C, geo.cblkno as i64);
    put(&mut sb, 0x010, geo.iblkno as i64);
    put(&mut sb, 0x014, geo.dblkno as i64);
    put(&mut sb, 0x018, 0); // fs_old_cgoffset — no rotational staggering
    put(&mut sb, 0x01C, -1); // fs_old_cgmask
    put(&mut sb, 0x024, geo.size_frags as i64);
    put(&mut sb, 0x028, geo.dsize_frags as i64);
    put(&mut sb, 0x02C, geo.ncg as i64);
    put(&mut sb, 0x030, geo.bsize as i64);
    put(&mut sb, 0x034, geo.fsize as i64);
    put(&mut sb, 0x038, geo.frag as i64);
    put(&mut sb, 0x03C, MINFREE as i64);
    put(&mut sb, 0x044, 60); // fs_old_rps
    put(&mut sb, 0x048, -(geo.bsize as i64)); // fs_bmask
    put(&mut sb, 0x04C, -(geo.fsize as i64)); // fs_fmask
    put(&mut sb, 0x050, geo.bsize.trailing_zeros() as i64);
    put(&mut sb, 0x054, geo.fsize.trailing_zeros() as i64);
    put(&mut sb, 0x058, MAXCONTIG as i64);
    put(&mut sb, 0x05C, MAXBPG as i64);
    put(&mut sb, 0x060, geo.frag.trailing_zeros() as i64);
    put(&mut sb, 0x064, i64::from(geo.fsbtodb));
    put(&mut sb, 0x068, sbsize as i64);
    put(&mut sb, 0x074, geo.nindir as i64);
    put(&mut sb, 0x078, geo.inopb as i64);
    put(&mut sb, 0x07C, (geo.fsize / 512) as i64); // fs_old_nspf
    put(&mut sb, 0x080, 0); // fs_optim = FS_OPTTIME
                            // The old geometry words describe one cylinder group as one cylinder,
                            // which is the shape every FFS built since rotational layout was dropped.
    let spc = geo.fpg * (geo.fsize / 512);
    put(&mut sb, 0x084, spc as i64); // fs_old_npsect
    put(&mut sb, 0x088, 1); // fs_old_interleave
    put(&mut sb, 0x098, geo.csaddr as i64);
    put(&mut sb, 0x09C, geo.cssize as i64);
    put(&mut sb, 0x0A0, geo.cgsize as i64);
    put(&mut sb, 0x0A8, spc as i64); // fs_old_nsect
    put(&mut sb, 0x0AC, spc as i64); // fs_old_spc
    put(&mut sb, 0x0B0, geo.ncg as i64); // fs_old_ncyl
    put(&mut sb, 0x0B4, OLD_CPG as i64);
    put(&mut sb, 0x0B8, geo.ipg as i64);
    put(&mut sb, 0x0BC, geo.fpg as i64);
    put(&mut sb, 0x0C0, total.ndir as i64);
    put(&mut sb, 0x0C4, total.nbfree as i64);
    put(&mut sb, 0x0C8, total.nifree as i64);
    put(&mut sb, 0x0CC, total.nffree as i64);
    sb[0x0D1] = 1; // fs_clean
    sb[0x0D3] = FS_FLAGS_UPDATED;

    if let Some(name) = label {
        let bytes = name.as_bytes();
        let n = bytes.len().min(VOLNAME_LEN - 1);
        sb[OFF_VOLNAME..OFF_VOLNAME + n].copy_from_slice(&bytes[..n]);
    }

    write_i64(&mut sb, 1000, SB_OFFSET_UFS1 as i64, endian);
    write_i64(&mut sb, 1008, total.ndir as i64, endian);
    write_i64(&mut sb, 1016, total.nbfree as i64, endian);
    write_i64(&mut sb, 1024, total.nifree as i64, endian);
    write_i64(&mut sb, 1032, total.nffree as i64, endian);
    write_i64(&mut sb, 1080, geo.size_frags as i64, endian);
    write_i64(&mut sb, 1088, geo.dsize_frags as i64, endian);
    write_i64(&mut sb, 1096, geo.csaddr as i64, endian);
    put(&mut sb, 1196, AVGFILESIZE as i64);
    put(&mut sb, 1200, AVGFPDIR as i64);
    put(&mut sb, 1316, CONTIGSUMSIZE as i64);
    put(&mut sb, OFF_MAXSYMLINKLEN, 60);
    put(&mut sb, 1324, INODEFMT_44BSD as i64);
    write_i64(&mut sb, 1328, max_file_size(geo), endian);
    write_i64(&mut sb, 1336, (geo.bsize - 1) as i64, endian); // fs_qbmask
    write_i64(&mut sb, 1344, (geo.fsize - 1) as i64, endian); // fs_qfmask
    put(&mut sb, 1356, 1); // fs_old_postblformat = FS_DYNAMICPOSTBLFMT
    put(&mut sb, 1360, OLD_NRPOS as i64);
    put(&mut sb, MAGIC_OFF, MAGIC_UFS1 as i64);
    sb
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

    #[test]
    fn a_fresh_volume_opens_and_lists_an_empty_root() {
        let img = create_blank_ufs1(&params(16 * 1024 * 1024)).expect("format");
        let mut fs = UfsFilesystem::open(Cursor::new(img), 0).expect("open");
        assert_eq!(fs.version(), UfsVersion::Ufs1);
        let root = fs.root().expect("root");
        assert!(fs.list_directory(&root).expect("ls").is_empty());
        assert_eq!(fs.volume_label(), Some("rusty-backup"));
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
        let img = create_blank_ufs1(&params(32 * 1024 * 1024)).expect("format");
        let mut backing = img;
        {
            let mut fs = UfsFilesystem::open(Cursor::new(&mut backing), 0).expect("open");
            let root = fs.root().expect("root");
            fs.create_directory(&root, "docs", &CreateDirectoryOptions::default())
                .expect("mkdir");
        }
        let mut fs = UfsFilesystem::open(Cursor::new(&mut backing), 0).expect("re-open");
        let root = fs.root().expect("root");
        let kids = fs.list_directory(&root).expect("ls");
        assert_eq!(kids.len(), 1);
        assert_eq!(kids[0].name, "docs");
        let report = fsck_ufs(&mut fs).expect("fsck");
        assert!(
            report.errors.is_empty() && report.warnings.is_empty(),
            "{:?} / {:?}",
            report.errors,
            report.warnings,
        );
    }

    #[test]
    fn a_volume_too_small_for_one_group_is_refused() {
        assert!(plan(&params(64 * 1024)).is_err());
    }

    /// Several groups is the case the single-group fixture cannot cover: the
    /// last one is short and only group 0 gives up space to the summary area.
    #[test]
    fn a_multi_group_volume_accounts_for_every_group() {
        let geo = plan(&params(300 * 1024 * 1024)).expect("plan");
        assert!(geo.ncg > 1, "300 MiB should need several groups");
        let by_hand: u64 = (0..geo.ncg)
            .map(|c| {
                let lower = if c == 0 { 0 } else { geo.sblkno };
                lower + geo.cg_ndblk(c) - geo.cg_dupper(c)
            })
            .sum();
        assert_eq!(geo.dsize_frags, by_hand);
        assert!(geo.dsize_frags < geo.size_frags);
    }
}
