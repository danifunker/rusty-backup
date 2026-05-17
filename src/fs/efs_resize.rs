//! In-place EFS grow and shrink.
//!
//! See `docs/efs_resize_and_edit.md` Phase 3 (grow) and Phase 4
//! (conservative shrink) for the design rationale. EFS lacks btrees,
//! checksums, packed inode numbers, and a journal, so in-place is the
//! right approach — the volume-header story is a single 92-byte
//! superblock + replica.
//!
//! Both operations end with a Phase-2 fsck pass before the primary
//! superblock is sealed. A crash before the primary write leaves the
//! volume readable at its old size; only the replica may be stale,
//! and IRIX's mount path falls back to the primary first.

use std::io::{Read, Seek, Write};

#[cfg(test)]
use byteorder::ByteOrder;

use super::efs::{EfsFilesystem, EFS_DIRECTEXTENTS_MAX};
use super::filesystem::{EditableFilesystem, FilesystemError};

/// Grow an EFS volume in place to `new_size_blocks` (512-byte blocks).
/// Returns the new size in blocks on success. Errors when:
///   - `new_size_blocks <= old_size` (no growth requested).
///   - The growth would require the bitmap to be relocated. This is
///     the most common rejection on real-world IRIX images; the
///     bitmap is sized exactly for the original volume and we'd need
///     to evacuate any allocated blocks living at the future bitmap
///     location. Bitmap relocation is in the doc as a future-work item
///     but not in this initial slice.
///   - Post-grow fsck reports any errors.
///
/// The replica superblock's position scales with `fs_size`: if the
/// original layout had `replsb = fs_size + N` for some small N, we
/// preserve N and write the new replica at `new_size_blocks + N`.
/// The old replica block becomes orphaned but harmless — it's outside
/// the new bitmap region by construction.
pub fn grow_efs<R: Read + Write + Seek + Send>(
    reader: R,
    partition_offset: u64,
    new_size_blocks: u32,
    log: &mut impl FnMut(&str),
) -> Result<u32, FilesystemError> {
    let mut fs = EfsFilesystem::open(reader, partition_offset)?;
    let sb = fs.sb_clone();
    let old_size = sb.fs_size;
    if new_size_blocks <= old_size {
        return Err(FilesystemError::InvalidData(format!(
            "EFS grow: new size {new_size_blocks} blocks is not larger than old size {old_size}"
        )));
    }
    let delta = new_size_blocks - old_size;
    log(&format!(
        "EFS grow: {} -> {} blocks (+{} blocks, +{} KiB)",
        old_size,
        new_size_blocks,
        delta,
        delta as u64 / 2
    ));

    // Bitmap capacity check: every block in the volume must be
    // addressable from the bitmap. If the existing bitmap has spare
    // bits, we can grow within that. Otherwise refuse — bitmap
    // relocation is future work (see doc Phase 3a step 3).
    let bitmap_bits = sb.bmsize as u64 * 8;
    if (new_size_blocks as u64) > bitmap_bits {
        return Err(FilesystemError::Unsupported(format!(
            "EFS grow: new size {} blocks exceeds bitmap capacity ({} bits); \
             bitmap relocation is not implemented in this slice. \
             Try a smaller increase (max {} additional blocks).",
            new_size_blocks,
            bitmap_bits,
            bitmap_bits.saturating_sub(old_size as u64)
        )));
    }

    // Mark the new tail blocks as free in the bitmap. Per the doc,
    // any stale bits left over from prior shrink rounds get cleared.
    {
        let bm = fs.staged_bitmap_mut()?;
        for blk in old_size..new_size_blocks {
            let by = (blk / 8) as usize;
            if by >= bm.len() {
                break;
            }
            let bb = 7 - (blk % 8);
            bm[by] &= !(1u8 << bb);
        }
    }

    // Adjust the superblock. ncg is bumped only if at least one full
    // new CG fits at the tail; the doc's heuristic. We don't actually
    // initialize the new CG's inode table here in this slice —
    // doing so requires choosing a layout for those blocks that the
    // existing inode-byte-offset math agrees with. For the small-grow
    // case (delta < cgfsize) ncg stays put and we just extend the
    // last CG's data region.
    let mut new_sb = sb.clone();
    let added_blocks = delta;
    let added_cgs = added_blocks / sb.cgfsize;
    if added_cgs > 0 {
        // Append-new-CGs path (doc Phase 3b) — deferred.
        return Err(FilesystemError::Unsupported(format!(
            "EFS grow: new size would add {added_cgs} new cylinder group(s); \
             append-new-CGs path is deferred to a future slice. \
             Use a smaller increase (< {} blocks) for the extend-last-CG path.",
            sb.cgfsize
        )));
    }

    // Preserve the relative position of the replica past fs_size.
    let trailing_zone = sb.replsb.saturating_sub(old_size);
    new_sb.fs_size = new_size_blocks;
    new_sb.replsb = new_size_blocks.saturating_add(trailing_zone);
    new_sb.tfree = new_sb.tfree.saturating_add(added_blocks);

    log(&format!(
        "EFS grow: replica moves {} -> {} (trailing zone {})",
        sb.replsb, new_sb.replsb, trailing_zone
    ));

    fs.set_sb(new_sb);

    // Post-mutation fsck: ensures we didn't violate any invariants
    // BEFORE writing the new superblock. A failure here leaves the
    // on-disk state intact because we haven't called sync_metadata
    // yet — the staged bitmap and sb_dirty flag are dropped when
    // `fs` goes out of scope.
    let result = super::efs_fsck::fsck_efs(&mut fs)?;
    if !result.errors.is_empty() {
        let codes: Vec<String> = result.errors.iter().map(|e| e.code.clone()).collect();
        return Err(FilesystemError::InvalidData(format!(
            "EFS grow: post-grow fsck reported {} error(s): [{}]",
            result.errors.len(),
            codes.join(", ")
        )));
    }
    log(&format!(
        "EFS grow: fsck clean ({} files, {} dirs checked)",
        result.stats.files_checked, result.stats.directories_checked
    ));

    fs.sync_metadata()?;
    log("EFS grow: committed new superblock pair");
    Ok(new_size_blocks)
}

/// Shrink an EFS volume in-place to `new_size_blocks` (conservative:
/// every cylinder group that contains any in-use inode is preserved
/// in full; only trailing free data blocks are dropped). Returns the
/// new size in blocks on success.
///
/// Pre-checks compute the conservative floor by walking the inode
/// table. If `new_size_blocks` is below the floor, errors with the
/// floor in the message so the caller can surface a useful dialog.
///
/// This is the resize counterpart most useful during restore: pick a
/// smaller partition, EFS shrinks itself to the floor or beyond if
/// the user picked a custom size at or above the floor.
pub fn shrink_efs_conservative<R: Read + Write + Seek + Send>(
    reader: R,
    partition_offset: u64,
    new_size_blocks: u32,
    log: &mut impl FnMut(&str),
) -> Result<u32, FilesystemError> {
    let mut fs = EfsFilesystem::open(reader, partition_offset)?;
    let sb = fs.sb_clone();
    let old_size = sb.fs_size;
    if new_size_blocks >= old_size {
        return Err(FilesystemError::InvalidData(format!(
            "EFS shrink: new size {new_size_blocks} blocks is not smaller than old size {old_size}"
        )));
    }

    // Conservative floor: every block in [0..new_size_blocks) must
    // not be claimed by any in-use inode. Build the set of allocated
    // blocks and find the highest one — that's the floor.
    let floor = compute_conservative_floor(&mut fs)?;
    log(&format!(
        "EFS shrink: conservative floor = {floor} blocks ({} KiB)",
        floor as u64 / 2
    ));

    if new_size_blocks < floor {
        return Err(FilesystemError::InvalidData(format!(
            "EFS shrink: requested size {new_size_blocks} below conservative floor {floor}. \
             Pick a size >= {floor} blocks ({} KiB).",
            floor as u64 / 2
        )));
    }

    // Clear the bits for blocks [new_size_blocks..old_size) in the
    // bitmap (they're now outside the volume). The bitmap itself
    // stays at its current location and keeps its current size — the
    // surplus bits past new_size_blocks are simply unused.
    {
        let bm = fs.staged_bitmap_mut()?;
        for blk in new_size_blocks..old_size {
            let by = (blk / 8) as usize;
            if by >= bm.len() {
                break;
            }
            let bb = 7 - (blk % 8);
            bm[by] &= !(1u8 << bb);
        }
    }

    // Replica scales with fs_size — preserve the trailing-zone offset.
    let mut new_sb = sb.clone();
    let trailing_zone = sb.replsb.saturating_sub(old_size);
    new_sb.fs_size = new_size_blocks;
    new_sb.replsb = new_size_blocks.saturating_add(trailing_zone);
    let removed_blocks = old_size - new_size_blocks;
    new_sb.tfree = new_sb.tfree.saturating_sub(removed_blocks);
    log(&format!(
        "EFS shrink: replica moves {} -> {} (trailing zone {})",
        sb.replsb, new_sb.replsb, trailing_zone
    ));

    fs.set_sb(new_sb);

    let result = super::efs_fsck::fsck_efs(&mut fs)?;
    if !result.errors.is_empty() {
        let codes: Vec<String> = result.errors.iter().map(|e| e.code.clone()).collect();
        return Err(FilesystemError::InvalidData(format!(
            "EFS shrink: post-shrink fsck reported {} error(s): [{}]",
            result.errors.len(),
            codes.join(", ")
        )));
    }
    log(&format!(
        "EFS shrink: fsck clean ({} files, {} dirs)",
        result.stats.files_checked, result.stats.directories_checked
    ));

    fs.sync_metadata()?;
    log("EFS shrink: committed new superblock pair");
    Ok(new_size_blocks)
}

/// Compute the conservative shrink floor: 1 + highest block claimed
/// by any in-use inode. This is the smallest `new_size_blocks` value
/// that preserves every allocated extent without relocation.
pub fn compute_conservative_floor<R: Read + Seek + Send>(
    fs: &mut EfsFilesystem<R>,
) -> Result<u32, FilesystemError> {
    let total = fs.total_inodes_readonly();
    let mut highest: u32 = 0;
    for inum in 2..total {
        let ino = fs.read_inode_readonly(inum)?;
        if ino.mode == 0 {
            continue;
        }
        for i in 0..(ino.numextents as usize).min(EFS_DIRECTEXTENTS_MAX) {
            let ext = ino.extents[i];
            if ext.length == 0 {
                continue;
            }
            let end = ext.bn.saturating_add(ext.length as u32);
            if end > highest {
                highest = end;
            }
        }
    }
    // The replica block must also stay inside the new volume — but
    // the replica position moves with `fs_size` in our resize, so
    // the floor only needs to cover inode data.
    Ok(highest)
}

#[cfg(test)]
mod tests {
    use super::super::efs::{EfsFilesystem, EfsSuperblock, EFS_SUPERBLOCK_SIZE};
    use super::super::filesystem::{CreateFileOptions, EditableFilesystem, Filesystem};
    use super::*;
    use std::io::Cursor;

    /// Build a writable EFS volume populated with a small file so the
    /// shrink path has something below the floor to preserve.
    fn build_test_volume() -> Vec<u8> {
        // 512 blocks total, with one file at the start of the data
        // region and lots of free tail. Lets us shrink down without
        // hitting the floor.
        let mut img = vec![0u8; 512 * 512];
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
            magic: 0x0007_2959, // EFS_MAGIC_OLD
            fname: *b"resize",
            fpack: *b"resize",
            // Bigger-than-fs_size bitmap so grow has room to extend
            // without relocation (the bitmap covers 32 * 8 = 256
            // bits; fs_size is 512. Insufficient! Make it 96 bytes =
            // 768 bits, covering up to 768 blocks.)
            bmsize: 96,
            tfree: 0,
            tinode: 0,
            bmblock: 2,
            replsb: total_blocks + 4,
            lastialloc: 2,
            checksum: 0,
        };
        sb.write_into(&mut img[sb_off..sb_off + EFS_SUPERBLOCK_SIZE]);

        // Bitmap: mark reserved-region bits.
        let bm_off = 2 * 512;
        for b in [0u32, 1, 2, 3, 4, 18, 19, 25] {
            let by = (b / 8) as usize;
            let bb = 7 - (b % 8);
            img[bm_off + by] |= 1 << bb;
        }
        // Root inode (2) — same shape as the efs.rs synthetic tests.
        let ino2_off = 18 * 512 + 2 * 128;
        let root = super::super::efs::EfsInode {
            inum: 2,
            mode: 0o040755,
            nlink: 2,
            uid: 0,
            gid: 0,
            size: 512,
            atime: 0,
            mtime: 0,
            ctime: 0,
            gen: 0,
            numextents: 1,
            version: 0,
            extents: {
                let mut e = [super::super::efs::EfsExtent {
                    magic: 0,
                    bn: 0,
                    length: 0,
                    offset: 0,
                }; EFS_DIRECTEXTENTS_MAX];
                e[0] = super::super::efs::EfsExtent {
                    magic: 0,
                    bn: 25,
                    length: 1,
                    offset: 0,
                };
                e
            },
        };
        let mut slot = [0u8; 128];
        root.write_into(&mut slot);
        img[ino2_off..ino2_off + 128].copy_from_slice(&slot);

        // Empty root dirblock at block 25.
        let mut dirblk = [0u8; 512];
        byteorder::BigEndian::write_u16(&mut dirblk[0..2], 0xBEEF);
        img[25 * 512..26 * 512].copy_from_slice(&dirblk);

        // Mirror replica.
        let rep_off = (sb.replsb as usize) * 512;
        if rep_off + EFS_SUPERBLOCK_SIZE <= img.len() {
            let mut copy = [0u8; EFS_SUPERBLOCK_SIZE];
            copy.copy_from_slice(&img[sb_off..sb_off + EFS_SUPERBLOCK_SIZE]);
            img[rep_off..rep_off + EFS_SUPERBLOCK_SIZE].copy_from_slice(&copy);
        }
        img
    }

    #[test]
    fn grow_extends_within_bitmap_capacity() {
        // Volume is 512 blocks, cgfsize=64. Stay below the
        // append-new-CGs threshold by growing 32 blocks → 544.
        let img = build_test_volume();
        let cur = Cursor::new(img);
        let mut log_lines: Vec<String> = Vec::new();
        let new_size = grow_efs(cur, 0, 544, &mut |s| log_lines.push(s.into())).expect("grow");
        assert_eq!(new_size, 544);
        assert!(log_lines.iter().any(|l| l.contains("committed")));
    }

    #[test]
    fn grow_rejects_append_new_cgs() {
        // 640 - 512 = 128 = 2 * cgfsize → triggers the deferred path.
        let img = build_test_volume();
        let cur = Cursor::new(img);
        let err = grow_efs(cur, 0, 640, &mut |_| {}).expect_err("deferred");
        assert!(
            matches!(err, FilesystemError::Unsupported(_)),
            "got {err:?}"
        );
    }

    #[test]
    fn grow_rejects_when_beyond_bitmap_capacity() {
        // Volume 512, bitmap 768 bits. Try 1000 → must refuse.
        let img = build_test_volume();
        let cur = Cursor::new(img);
        let err = grow_efs(cur, 0, 1000, &mut |_| {}).expect_err("too big");
        assert!(
            matches!(err, FilesystemError::Unsupported(_)),
            "got {err:?}"
        );
    }

    #[test]
    fn grow_rejects_no_growth() {
        let img = build_test_volume();
        let cur = Cursor::new(img);
        let err = grow_efs(cur, 0, 512, &mut |_| {}).expect_err("not growing");
        assert!(matches!(err, FilesystemError::InvalidData(_)));
    }

    #[test]
    fn shrink_respects_conservative_floor() {
        // Build a volume, add a file that lives at high blocks, then
        // try to shrink past it.
        let img = build_test_volume();
        let mut fs = EfsFilesystem::open(Cursor::new(img), 0).expect("open");
        // Bump tfree so the create_file path has room.
        let mut sb = fs.sb_clone();
        sb.tfree = 200;
        sb.tinode = 4;
        fs.set_sb(sb);
        let root = fs.root().expect("root");
        let payload = vec![0xAB; 256]; // one-block file
        let mut cur = Cursor::new(payload.clone());
        let _entry = fs
            .create_file(
                &root,
                "a.bin",
                &mut cur,
                payload.len() as u64,
                &CreateFileOptions::default(),
            )
            .expect("create");
        EditableFilesystem::sync_metadata(&mut fs).expect("sync");

        let bytes = fs.reader_into_inner().into_inner();

        // First-fit allocator places the file at block 26 (next after
        // root dir at 25 + reserved-region bits 0..19). So the
        // floor should be 27. Try to shrink to 20 → rejected.
        let err = shrink_efs_conservative(Cursor::new(bytes.clone()), 0, 20, &mut |_| {})
            .expect_err("below floor");
        assert!(matches!(err, FilesystemError::InvalidData(_)));

        // Shrinking to 100 (well above floor) works.
        let mut log_lines: Vec<String> = Vec::new();
        let new_size = shrink_efs_conservative(Cursor::new(bytes), 0, 100, &mut |s| {
            log_lines.push(s.into())
        })
        .expect("shrink");
        assert_eq!(new_size, 100);
    }

    #[test]
    fn shrink_rejects_no_shrink() {
        let img = build_test_volume();
        let cur = Cursor::new(img);
        let err = shrink_efs_conservative(cur, 0, 512, &mut |_| {}).expect_err("not shrinking");
        assert!(matches!(err, FilesystemError::InvalidData(_)));
    }
}
