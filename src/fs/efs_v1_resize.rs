//! In-place grow and shrink for SGI EFS v1.
//!
//! The geometry invariant `fs_size == firstcg + ncg * cgfsize` is the whole
//! design: `firstcg` and `cgfsize` are fixed when the volume is formatted, so
//! the only free variable is how many whole cylinder groups fit. A requested
//! size that is not a whole number of groups rounds **down**, which is what
//! the era's mkfs did too.
//!
//! Two constraints have no equivalent in the IRIX EFS resizer:
//!
//! * There is no replica superblock to keep in step — v1 has one superblock
//!   and a checksum, both resealed at the end.
//! * The bitmap lives at a fixed block 2 and is sized from `fs_size`, so
//!   growing can need more bitmap blocks than the gap below `firstcg`
//!   provides. That is refused rather than worked around: moving `firstcg`
//!   would relocate every cylinder group.
//!
//! A shrink is refused outright while anything live sits in the region being
//! dropped — an inode past the new table, or an extent past the new end. The
//! conservative answer is correct here: EFS v1 has no journal, and there is no
//! `fsck_efs` of the era to fall back on.

use std::io::{Read, Seek, SeekFrom, Write};

use super::efs_v1::{
    EfsV1Filesystem, EFS_V1_BITMAPBB, EFS_V1_BLOCKSIZE, EFS_V1_MAGIC, EFS_V1_MAGIC2, EFS_V1_SUPERBB,
};
use super::filesystem::FilesystemError;

/// The geometry a volume of `total_blocks` can carry, keeping `firstcg` and
/// `cgfsize` fixed.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct Fit {
    fs_size: u32,
    ncg: u32,
    bmsize: u32,
}

fn fit(firstcg: u32, cgfsize: u32, total_blocks: u32) -> Result<Fit, FilesystemError> {
    if cgfsize == 0 {
        return Err(FilesystemError::InvalidData(
            "EFS v1 resize: cgfsize is 0".to_string(),
        ));
    }
    let usable = total_blocks.saturating_sub(firstcg);
    let ncg = usable / cgfsize;
    if ncg == 0 {
        return Err(FilesystemError::InvalidData(format!(
            "EFS v1 resize: {total_blocks} blocks leaves no room for a {cgfsize}-block \
             cylinder group past the {firstcg}-block metadata head"
        )));
    }
    let fs_size = firstcg + ncg * cgfsize;
    let bmsize = fs_size.div_ceil(8);
    let bitmap_blocks = bmsize.div_ceil(EFS_V1_BLOCKSIZE as u32);
    if EFS_V1_BITMAPBB + bitmap_blocks > firstcg {
        return Err(FilesystemError::InvalidData(format!(
            "EFS v1 resize: a {fs_size}-block volume needs {bitmap_blocks} bitmap blocks, but \
             only {} are free below the first cylinder group at {firstcg}",
            firstcg.saturating_sub(EFS_V1_BITMAPBB)
        )));
    }
    Ok(Fit {
        fs_size,
        ncg,
        bmsize,
    })
}

/// Resize the EFS v1 volume at `partition_offset`. A silent no-op when the
/// magic does not match, so `resize_filesystem_for` can call every hook blind.
pub fn resize_efs_v1_in_place(
    file: &mut (impl Read + Write + Seek),
    partition_offset: u64,
    new_size_bytes: u64,
    log: &mut impl FnMut(&str),
) -> anyhow::Result<()> {
    if !looks_like_efs_v1(file, partition_offset)? {
        return Ok(());
    }

    let total_blocks = (new_size_bytes / EFS_V1_BLOCKSIZE) as u32;
    let mut fs = EfsV1Filesystem::open(&mut *file, partition_offset)?;
    let old = fs.superblock().clone();
    let target = fit(old.firstcg, old.cgfsize, total_blocks)?;

    if target.fs_size == old.fs_size {
        log(&format!(
            "EFS v1: already {} blocks; nothing to do",
            old.fs_size
        ));
        return Ok(());
    }

    if target.fs_size < old.fs_size {
        check_shrink_is_safe(&mut fs, &target)?;
        log(&format!(
            "EFS v1: shrinking {} -> {} blocks ({} -> {} cylinder groups)",
            old.fs_size, target.fs_size, old.ncg, target.ncg
        ));
    } else {
        log(&format!(
            "EFS v1: growing {} -> {} blocks ({} -> {} cylinder groups)",
            old.fs_size, target.fs_size, old.ncg, target.ncg
        ));
        zero_new_inode_tables(&mut fs, old.ncg as u32, target.ncg)?;
    }

    {
        let sb = fs.sb_mut();
        sb.fs_size = target.fs_size;
        sb.ncg = target.ncg as u16;
        sb.bmsize = target.bmsize;
    }

    // The inode table is authoritative, so the bitmap for the new geometry is
    // derived rather than patched, and the counters follow from it.
    let (bm, free_inodes) = fs.rebuild_bitmap()?;
    fs.write_bitmap(&bm)?;
    fs.refresh_tfree(&bm);
    fs.sb_mut().tinode = free_inodes.saturating_sub(1);
    let tfree = fs.superblock().tfree;
    fs.sync_superblock()?;

    // Sanity gate: the volume has to verify at its new size.
    let after = super::efs_v1_fsck::fsck_efs_v1(&mut fs)?;
    if !after.errors.is_empty() {
        anyhow::bail!(
            "EFS v1 resize left the volume inconsistent ({} finding(s), first: {})",
            after.errors.len(),
            after.errors[0].message
        );
    }
    log(&format!(
        "EFS v1: resized to {} blocks, {} free",
        target.fs_size, tfree
    ));
    Ok(())
}

/// Refuse a shrink while anything live sits past the new end.
fn check_shrink_is_safe<R: Read + Write + Seek>(
    fs: &mut EfsV1Filesystem<R>,
    target: &Fit,
) -> Result<(), FilesystemError> {
    let sb = fs.superblock().clone();
    let per_cg = sb.inodes_per_cg();
    let new_total_inodes = per_cg.saturating_mul(target.ncg);
    for inum in 0..sb.total_inodes() {
        let inode = fs.read_inode(inum)?;
        if inode.is_free() {
            continue;
        }
        if inum >= new_total_inodes {
            return Err(FilesystemError::InvalidData(format!(
                "EFS v1 shrink refused: inode {inum} is in use but the new geometry only has \
                 {new_total_inodes} inodes"
            )));
        }
        for e in fs.owned_extents(&inode)? {
            if e.is_hole() {
                continue;
            }
            let end = e.bn + e.length as u32;
            if end > target.fs_size {
                return Err(FilesystemError::InvalidData(format!(
                    "EFS v1 shrink refused: inode {inum} owns blocks [{}..{end}) past the new \
                     end at {}",
                    e.bn, target.fs_size
                )));
            }
        }
    }
    Ok(())
}

/// Zero the inode tables of groups appended by a grow, so their inodes read as
/// free rather than as whatever the medium happened to hold.
fn zero_new_inode_tables<R: Read + Write + Seek>(
    fs: &mut EfsV1Filesystem<R>,
    old_ncg: u32,
    new_ncg: u32,
) -> Result<(), FilesystemError> {
    let sb = fs.superblock().clone();
    let zeros = vec![0u8; sb.cgisize as usize * EFS_V1_BLOCKSIZE as usize];
    for cg in old_ncg..new_ncg {
        let start = sb.firstcg + cg * sb.cgfsize;
        fs.write_blocks(start, &zeros)?;
    }
    Ok(())
}

/// Cheap magic probe in either word order, so a non-EFS-v1 partition costs one
/// sector read and nothing else.
fn looks_like_efs_v1(file: &mut (impl Read + Seek), partition_offset: u64) -> anyhow::Result<bool> {
    file.seek(SeekFrom::Start(
        partition_offset + EFS_V1_SUPERBB * EFS_V1_BLOCKSIZE,
    ))?;
    let mut probe = [0u8; 0x2A];
    if file.read_exact(&mut probe).is_err() {
        return Ok(false);
    }
    let native = u32::from_be_bytes([probe[0x26], probe[0x27], probe[0x28], probe[0x29]]);
    let swabbed = u32::from_be_bytes([probe[0x27], probe[0x26], probe[0x29], probe[0x28]]);
    Ok([native, swabbed]
        .iter()
        .any(|m| *m == EFS_V1_MAGIC || *m == EFS_V1_MAGIC2))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::fs::efs_v1::create_blank_efs_v1;
    use crate::fs::filesystem::{CreateFileOptions, EditableFilesystem, Filesystem};
    use std::io::Cursor;

    fn blank(mb: u64) -> Vec<u8> {
        create_blank_efs_v1(mb * 1024 * 1024, "rz").unwrap()
    }

    fn fs_size_of(img: &[u8]) -> u32 {
        let fs = EfsV1Filesystem::open(Cursor::new(img.to_vec()), 0).unwrap();
        fs.superblock().fs_size
    }

    fn put(img: &mut Vec<u8>, name: &str, bytes: &[u8]) {
        let mut fs = EfsV1Filesystem::open(Cursor::new(std::mem::take(img)), 0).unwrap();
        let root = fs.root().unwrap();
        fs.create_file(
            &root,
            name,
            &mut Cursor::new(bytes.to_vec()),
            bytes.len() as u64,
            &CreateFileOptions::default(),
        )
        .unwrap();
        *img = fs.reader_into_inner().into_inner();
    }

    fn resize(img: &mut Vec<u8>, new_bytes: u64) -> anyhow::Result<()> {
        let mut cur = Cursor::new(std::mem::take(img));
        let r = resize_efs_v1_in_place(&mut cur, 0, new_bytes, &mut |_| {});
        *img = cur.into_inner();
        r
    }

    #[test]
    fn a_non_efs_v1_partition_is_left_alone() {
        let mut img = vec![0u8; 1024 * 1024];
        let before = img.clone();
        resize(&mut img, 512 * 1024).unwrap();
        assert_eq!(img, before);
    }

    #[test]
    fn grow_adds_cylinder_groups_and_still_verifies() {
        let mut img = blank(4);
        put(&mut img, "keep", b"payload that must survive");
        let before = fs_size_of(&img);
        img.resize(16 * 1024 * 1024, 0);
        resize(&mut img, 16 * 1024 * 1024).unwrap();
        let after = fs_size_of(&img);
        assert!(after > before, "{before} -> {after}");

        let mut fs = EfsV1Filesystem::open(Cursor::new(img), 0).unwrap();
        assert!(super::super::efs_v1_fsck::fsck_efs_v1(&mut fs)
            .unwrap()
            .is_clean());
        let root = fs.root().unwrap();
        let e = fs.list_directory(&root).unwrap().remove(0);
        assert_eq!(
            fs.read_file(&e, usize::MAX).unwrap(),
            b"payload that must survive"
        );
    }

    #[test]
    fn shrink_drops_trailing_groups_and_still_verifies() {
        let mut img = blank(16);
        put(&mut img, "keep", b"still here");
        let before = fs_size_of(&img);
        resize(&mut img, 8 * 1024 * 1024).unwrap();
        let after = fs_size_of(&img);
        assert!(after < before, "{before} -> {after}");

        let mut fs = EfsV1Filesystem::open(Cursor::new(img), 0).unwrap();
        assert!(super::super::efs_v1_fsck::fsck_efs_v1(&mut fs)
            .unwrap()
            .is_clean());
        let root = fs.root().unwrap();
        let e = fs.list_directory(&root).unwrap().remove(0);
        assert_eq!(fs.read_file(&e, usize::MAX).unwrap(), b"still here");
    }

    #[test]
    fn a_shrink_that_would_cut_live_data_is_refused() {
        let mut img = blank(16);
        // Fill enough that allocation reaches past the halfway mark.
        for i in 0..24 {
            put(&mut img, &format!("f{i:02}"), &vec![0xEEu8; 200 * 1024]);
        }
        let before = img.clone();
        let err = resize(&mut img, 2 * 1024 * 1024).unwrap_err();
        assert!(
            format!("{err}").contains("shrink refused"),
            "unexpected error: {err}"
        );
        assert_eq!(img, before, "a refused shrink must not write");
    }

    #[test]
    fn a_size_below_one_cylinder_group_is_refused() {
        let mut img = blank(8);
        let before = img.clone();
        assert!(resize(&mut img, 64 * 1024).is_err());
        assert_eq!(img, before);
    }

    #[test]
    fn geometry_rounds_down_to_whole_cylinder_groups() {
        let f = fit(8, 100, 1008).unwrap();
        assert_eq!(f.ncg, 10);
        assert_eq!(f.fs_size, 1008);
        // Eight blocks short of the next group: round down, do not overreach.
        let f = fit(8, 100, 1100).unwrap();
        assert_eq!(f.ncg, 10);
        assert_eq!(f.fs_size, 1008);
    }
}
