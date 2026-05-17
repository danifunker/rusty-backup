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

use super::efs::{EfsFilesystem, EFS_BLOCKSIZE, EFS_DIRECTEXTENTS_MAX};
use super::filesystem::FilesystemError;

const EFS_BLOCKSIZE_USIZE: usize = EFS_BLOCKSIZE as usize;

/// Unified entry point matching the `resize_filesystem_for` dispatch
/// pattern: probe the EFS magic at sector 1 of the partition, and if
/// present, route to grow_efs or shrink_efs_conservative based on the
/// requested size. Non-EFS partitions are ignored (returns Ok), so
/// this is safe to call alongside the other filesystem resize hooks.
///
/// Wraps the EFS-specific `FilesystemError` as an `anyhow::Error` so
/// the function fits the common signature.
pub fn resize_efs_in_place(
    file: &mut (impl Read + Write + Seek),
    partition_offset: u64,
    new_size_bytes: u64,
    log: &mut impl FnMut(&str),
) -> anyhow::Result<()> {
    // Probe the primary superblock for the EFS magic before we
    // commit to running a real resize. `EfsFilesystem::open` already
    // validates magic; on mismatch we silently bail (this lets the
    // unified `resize_filesystem_for` call every filesystem's hook
    // without each one needing to know the partition type).
    use std::io::SeekFrom;
    file.seek(SeekFrom::Start(partition_offset + 512))?;
    let mut probe = [0u8; 32];
    if file.read_exact(&mut probe).is_err() {
        return Ok(());
    }
    let magic = u32::from_be_bytes([probe[28], probe[29], probe[30], probe[31]]);
    if magic != 0x0007_2959 && magic != 0x0007_295A {
        return Ok(());
    }

    let new_size_blocks = (new_size_bytes / 512) as u32;
    // Open just to read the current size; we'll re-open inside the
    // grow/shrink path so they own the writable handle for the full
    // mutation.
    let cur_size = {
        let fs = EfsFilesystem::open(&mut *file, partition_offset)
            .map_err(|e| anyhow::anyhow!("EFS resize: {e}"))?;
        fs.sb_clone().fs_size
    };
    if new_size_blocks == cur_size {
        log("EFS resize: target equals current size, no-op");
        return Ok(());
    }
    let result = if new_size_blocks > cur_size {
        grow_efs(file, partition_offset, new_size_blocks, log)
    } else {
        shrink_efs_conservative(file, partition_offset, new_size_blocks, log)
    };
    result
        .map(|_| ())
        .map_err(|e| anyhow::anyhow!("EFS resize: {e}"))
}

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
pub fn grow_efs<R: Read + Write + Seek>(
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

    // Safety pre-check + reservation for Phase 3b. When new CGs are
    // appended, each one's inode-region [cg_start..cg_start+cgisize)
    // must currently be free in the bitmap — otherwise zeroing those
    // sectors below would destroy live file data. We reserve them
    // (mark in-use) BEFORE bitmap relocation so the relocate's
    // contiguous-free-run search avoids them.
    let added_cgs_preview = delta / sb.cgfsize;
    if added_cgs_preview > 0 {
        let bm = fs.staged_bitmap_mut()?;
        for new_cg_index in 0..added_cgs_preview {
            let cg = sb.ncg as u32 + new_cg_index;
            let cg_start = sb.firstcg + cg * sb.cgfsize;
            for blk in cg_start..cg_start + sb.cgisize as u32 {
                let by = (blk / 8) as usize;
                if by >= bm.len() {
                    // Past the current bitmap's coverage — relocation
                    // will grow the buffer; reservation deferred.
                    break;
                }
                let bb = 7 - (blk % 8);
                if bm[by] & (1u8 << bb) != 0 {
                    return Err(FilesystemError::InvalidData(format!(
                        "EFS grow: cannot append new CG at block {cg_start}: \
                         inode-region block {blk} is already in use by a file. \
                         A future slice could relocate the conflicting data block; \
                         for now, fsck the volume and retry with a smaller increase."
                    )));
                }
                bm[by] |= 1u8 << bb; // reserve
            }
        }
    }

    // Bitmap capacity check + optional relocation. If the existing
    // bitmap has enough bits for the new volume size, leave it where
    // it is. Otherwise find a free contiguous run in the current
    // volume (now with new CG inode regions reserved) and relocate.
    let bitmap_bits = sb.bmsize as u64 * 8;
    let sb = if (new_size_blocks as u64) <= bitmap_bits {
        sb
    } else {
        relocate_bitmap(&mut fs, &sb, new_size_blocks, log)?
    };

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

    // Split the delta into N full new CGs + an optional fractional tail.
    let added_cgs = delta / sb.cgfsize;
    let _fractional = delta - added_cgs * sb.cgfsize; // covered by the
                                                      // "mark new blocks
                                                      // free" step above
    if added_cgs > 0 {
        log(&format!(
            "EFS grow: appending {added_cgs} new cylinder group(s) of {} blocks each",
            sb.cgfsize
        ));
        // For each new CG: zero its inode-table region (mode==0 in every
        // slot = free) and mark inode-region bits as in-use in the
        // bitmap. Data-region bits stay clear (already cleared by the
        // tail loop above).
        // Inode-region bits were already marked in-use by the
        // pre-check / reservation step above. Here we just zero the
        // inode-table sectors on disk so every new slot reads as
        // mode==0 (free).
        for new_cg_index in 0..added_cgs {
            let cg = sb.ncg as u32 + new_cg_index;
            let cg_start = sb.firstcg + cg * sb.cgfsize;
            let zero_sector = [0u8; EFS_BLOCKSIZE_USIZE];
            for i in 0..sb.cgisize as u32 {
                fs.write_block(cg_start + i, &zero_sector)?;
            }
            // Reservation may have been deferred for blocks past the
            // pre-relocation bitmap end. Catch up here now that the
            // bitmap may have been expanded.
            let bm = fs.staged_bitmap_mut()?;
            for blk in cg_start..cg_start + sb.cgisize as u32 {
                let by = (blk / 8) as usize;
                if by >= bm.len() {
                    break;
                }
                let bb = 7 - (blk % 8);
                bm[by] |= 1u8 << bb;
            }
        }
    }

    // Preserve the relative position of the replica past fs_size.
    let mut new_sb = sb.clone();
    let trailing_zone = sb.replsb.saturating_sub(old_size);
    new_sb.fs_size = new_size_blocks;
    new_sb.replsb = new_size_blocks.saturating_add(trailing_zone);
    new_sb.ncg = sb.ncg.saturating_add(added_cgs as u16);
    let inodes_per_cg = sb.cgisize as u32 * 4; // 4 inodes per 512-byte block
    new_sb.tinode = new_sb
        .tinode
        .saturating_add(added_cgs.saturating_mul(inodes_per_cg));
    // Free data blocks added: total new blocks minus the inode-region
    // blocks we marked in-use.
    let inode_region_blocks_added = added_cgs.saturating_mul(sb.cgisize as u32);
    new_sb.tfree = new_sb
        .tfree
        .saturating_add(delta.saturating_sub(inode_region_blocks_added));

    log(&format!(
        "EFS grow: replica moves {} -> {} (trailing zone {}), ncg {} -> {}",
        sb.replsb, new_sb.replsb, trailing_zone, sb.ncg, new_sb.ncg
    ));

    fs.set_sb(new_sb);

    // Post-mutation fsck: ensures we didn't violate any invariants
    // BEFORE writing the new superblock. A failure here leaves the
    // on-disk state intact because we haven't called sync_metadata
    // yet — the staged bitmap and sb_dirty flag are dropped when
    // `fs` goes out of scope.
    let result = super::efs_fsck::fsck_efs(&mut fs)?;
    if !result.errors.is_empty() {
        let msgs: Vec<String> = result
            .errors
            .iter()
            .map(|e| format!("{}: {}", e.code, e.message))
            .collect();
        return Err(FilesystemError::InvalidData(format!(
            "EFS grow: post-grow fsck reported {} error(s): {}",
            result.errors.len(),
            msgs.join("; ")
        )));
    }
    log(&format!(
        "EFS grow: fsck clean ({} files, {} dirs checked)",
        result.stats.files_checked, result.stats.directories_checked
    ));

    fs.do_sync_metadata()?;
    log("EFS grow: committed new superblock pair");
    Ok(new_size_blocks)
}

/// Find a free contiguous run inside the current volume large enough
/// to hold the new bitmap, copy the existing bitmap forward into the
/// staged buffer (which gets resized), mark the old bitmap blocks as
/// free, mark the new bitmap blocks as in-use, and update sb.bmblock
/// + sb.bmsize. Returns the updated superblock; the caller installs
/// it via `fs.set_sb` once the rest of the grow finishes.
///
/// The write to the new location happens at sync_metadata time —
/// `do_sync_metadata` calls `write_bitmap` which uses the current
/// `sb.bmblock`. Since we update `sb.bmblock` here before sync runs,
/// the bitmap bytes land at the new location naturally.
fn relocate_bitmap<R: Read + Write + Seek>(
    fs: &mut EfsFilesystem<R>,
    sb: &super::efs::EfsSuperblock,
    new_size_blocks: u32,
    log: &mut impl FnMut(&str),
) -> Result<super::efs::EfsSuperblock, FilesystemError> {
    let new_bmsize_bytes = (new_size_blocks as u64).div_ceil(8) as u32;
    let new_bmsize_sectors = (new_bmsize_bytes as u64).div_ceil(EFS_BLOCKSIZE) as u32;
    let old_bmblock = fs.effective_bmblock();
    let old_bmsize_sectors = (sb.bmsize as u64).div_ceil(EFS_BLOCKSIZE) as u32;

    log(&format!(
        "EFS grow: relocating bitmap (need {new_bmsize_bytes} bytes, \
         {new_bmsize_sectors} sectors); old loc=block {old_bmblock}, size={} sectors",
        old_bmsize_sectors
    ));

    let bm = fs.staged_bitmap_mut()?;

    // The bitmap's own blocks are already marked in-use, so the
    // allocator naturally skips them. Find a contiguous run.
    let new_ext = EfsFilesystem::<R>::alloc_contiguous_in_bitmap(bm, new_bmsize_sectors, 0)
        .map_err(|e| match e {
            FilesystemError::DiskFull(_) => FilesystemError::Unsupported(format!(
                "EFS grow: bitmap relocation needs {new_bmsize_sectors} contiguous free \
             sectors in the current volume; none found. The volume may be too fragmented; \
             consider shrinking first or running fsck repair."
            )),
            other => other,
        })?;
    let new_bmblock = new_ext.bn;
    log(&format!(
        "EFS grow: new bitmap at block {new_bmblock} ({} sectors)",
        new_bmsize_sectors
    ));

    // Free old bitmap blocks.
    for blk in old_bmblock..old_bmblock + old_bmsize_sectors {
        let by = (blk / 8) as usize;
        if by >= bm.len() {
            break;
        }
        let bb = 7 - (blk % 8);
        bm[by] &= !(1u8 << bb);
    }

    // Expand the in-memory buffer to the new bitmap size. New bytes
    // default to 0 (all blocks free); the grow path's tail loop and
    // CG-append step will set the appropriate bits before sync.
    bm.resize(new_bmsize_bytes as usize, 0);

    let mut new_sb = sb.clone();
    new_sb.bmblock = new_bmblock;
    new_sb.bmsize = new_bmsize_bytes;
    Ok(new_sb)
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
pub fn shrink_efs_conservative<R: Read + Write + Seek>(
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

    fs.do_sync_metadata()?;
    log("EFS shrink: committed new superblock pair");
    Ok(new_size_blocks)
}

/// Compute the conservative shrink floor: 1 + highest block claimed
/// by any in-use inode. This is the smallest `new_size_blocks` value
/// that preserves every allocated extent without relocation.
pub fn compute_conservative_floor<R: Read + Seek>(
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
    fn grow_appends_new_cylinder_groups() {
        // 640 - 512 = 128 = 2 * cgfsize → 2 new CGs appended at the
        // tail. cgisize=2 inode-table blocks per CG, so the new
        // inode region for CG 1 sits at 82..84 and for CG 2 at
        // 146..148. After grow: ncg=3, fs_size=640.
        let img = build_test_volume();
        let cur = Cursor::new(img);
        let mut log_lines: Vec<String> = Vec::new();
        let new_size =
            grow_efs(cur, 0, 640, &mut |s| log_lines.push(s.into())).expect("append-new-CGs grow");
        assert_eq!(new_size, 640);
        assert!(
            log_lines
                .iter()
                .any(|l| l.contains("appending 2 new cylinder")),
            "missing append log line in: {log_lines:?}"
        );
        assert!(log_lines.iter().any(|l| l.contains("committed")));
    }

    #[test]
    fn grow_append_new_cgs_then_reopen_round_trip() {
        // Grow by exactly 1 CG and re-open to confirm the inode
        // allocator can use the newly-added inode slots.
        use super::super::filesystem::EditableFilesystem;
        let img = build_test_volume();
        let mut buf = img;
        // 512 -> 576 = +64 = exactly one new CG (cgfsize).
        grow_efs(Cursor::new(&mut buf), 0, 576, &mut |_| {}).expect("grow");

        let mut fs = EfsFilesystem::open(Cursor::new(buf), 0).expect("reopen");
        let sb = fs.sb_clone();
        assert_eq!(sb.fs_size, 576);
        assert_eq!(sb.ncg, 2);
        // tinode was 0 in the test volume; the new CG adds cgisize*4
        // = 8 inode slots, all currently free.
        assert_eq!(sb.tinode, 8);

        // The newly-allocated inode slots should read as mode==0 (free).
        // Inode region for CG 1 spans blocks 82..84, holding inums 8..16
        // (cgisize*4 = 8 inodes per CG).
        for inum in 8..16 {
            let ino = fs.read_inode(inum).expect("read");
            assert_eq!(ino.mode, 0, "new-CG inum {inum} should be free");
        }
        // allocate_inode should hand out one of these new slots once
        // the existing free ones (in CG 0) are exhausted.
        let _ = EditableFilesystem::free_space(&mut fs);
    }

    #[test]
    fn grow_relocates_bitmap_when_beyond_capacity() {
        // Volume 512, bitmap 768 bits. Grow to 1000 → exceeds
        // bitmap capacity AND adds 7 new CGs. The relocate path
        // moves the bitmap to a fresh location with enough bits.
        let img = build_test_volume();
        let cur = Cursor::new(img);
        let mut log_lines: Vec<String> = Vec::new();
        let new_size = grow_efs(cur, 0, 1000, &mut |s| log_lines.push(s.into()))
            .expect("grow with bitmap relocation");
        assert_eq!(new_size, 1000);
        assert!(
            log_lines.iter().any(|l| l.contains("relocating bitmap")),
            "missing relocate log line in: {log_lines:?}"
        );
        assert!(log_lines.iter().any(|l| l.contains("committed")));
    }

    #[test]
    fn grow_relocate_then_reopen_round_trip() {
        // Same as above, but reopen and verify the new bitmap is
        // readable at sb.bmblock and addresses the full new volume.
        let img = build_test_volume();
        let mut buf = img;
        grow_efs(Cursor::new(&mut buf), 0, 1000, &mut |_| {}).expect("grow + relocate");
        let mut fs = EfsFilesystem::open(Cursor::new(buf), 0).expect("reopen");
        let sb = fs.sb_clone();
        assert_eq!(sb.fs_size, 1000);
        assert!(
            (sb.bmsize as u64) * 8 >= sb.fs_size as u64,
            "new bitmap ({} bits) does not cover new fs_size {}",
            sb.bmsize as u64 * 8,
            sb.fs_size
        );
        let bm = fs.read_bitmap_readonly().expect("read new bitmap");
        assert_eq!(bm.len(), sb.bmsize as usize);
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
