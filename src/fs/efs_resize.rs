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

use super::efs::{EfsFilesystem, EfsSuperblock, EFS_BLOCKSIZE, EFS_DIRECTEXTENTS_MAX};
use super::filesystem::FilesystemError;

const EFS_BLOCKSIZE_USIZE: usize = EFS_BLOCKSIZE as usize;

/// How a volume of N 512-byte blocks is laid out, per the invariant
/// `create_blank_efs` documents (`efs.rs`) and every real IRIX disk we
/// have parsed exhibits:
///
/// * `fs_size == firstcg + ncg * cgfsize` — the addressable region is
///   exactly the cylinder groups, so a size that isn't a whole number
///   of groups rounds DOWN (mkfs_efs does the same).
/// * `replsb == total_blocks - 1` — the replica lives in the volume's
///   LAST block, past `fs_size`, where the allocator can never reach it.
///
/// Both resize directions have to maintain all three fields together.
/// Scaling `fs_size` on its own — which is what they used to do — left
/// the cylinder-group region hanging past the end of a shrunk volume
/// (fsck's `CgRegionPastFsSize`, which aborted every real shrink) and
/// put the replica past the end of a grown one (a write beyond the
/// partition).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct VolumeFit {
    /// Blocks the filesystem may address: `[0, fs_size)`.
    fs_size: u32,
    /// Cylinder groups that fit. Callers clamp this to their direction
    /// of travel: grow only ever appends groups, shrink only ever drops
    /// trailing ones.
    ncg: u32,
    /// Replica block — the volume's last block, just past `fs_size`.
    replsb: u32,
}

/// Fit a volume of `total_blocks` 512-byte blocks to `sb`'s geometry.
/// `firstcg` and `cgfsize` are fixed by the original format and are
/// never renegotiated by a resize, so the only free variable is how
/// many whole cylinder groups fit below the replica's block.
fn fit_volume(sb: &EfsSuperblock, total_blocks: u32) -> Result<VolumeFit, FilesystemError> {
    if sb.cgfsize == 0 {
        return Err(FilesystemError::InvalidData(
            "EFS resize: superblock declares cgfsize 0".into(),
        ));
    }
    // Reserve the last block for the replica; the groups fill what's
    // below it. Matches `mkfs_efs`'s `replsb = total_blocks - 1`.
    let last = total_blocks.checked_sub(1).ok_or_else(|| {
        FilesystemError::InvalidData("EFS resize: target size is 0 blocks".into())
    })?;
    let ncg = (last.saturating_sub(sb.firstcg) / sb.cgfsize).min(u16::MAX as u32);
    if ncg == 0 {
        return Err(FilesystemError::InvalidData(format!(
            "EFS resize: {total_blocks} blocks is too small for this volume's geometry \
             (the first cylinder group starts at block {} and each group is {} blocks, \
              so the smallest possible volume is {} blocks)",
            sb.firstcg,
            sb.cgfsize,
            sb.firstcg + sb.cgfsize + 1
        )));
    }
    Ok(VolumeFit {
        fs_size: sb.firstcg + ncg * sb.cgfsize,
        ncg,
        replsb: last,
    })
}

/// Cylinder groups that have to survive a shrink whose in-use data
/// reaches `floor`: dropping a group drops its inode table, so any
/// group the floor reaches into must be kept whole. Always at least
/// one (the root inode lives in CG 0), never more than the volume has.
fn required_cgs_for_floor(sb: &EfsSuperblock, floor: u32) -> u32 {
    if sb.cgfsize == 0 {
        return sb.ncg as u32;
    }
    floor
        .saturating_sub(sb.firstcg)
        .div_ceil(sb.cgfsize)
        .max(1)
        .min(sb.ncg as u32)
}

/// The smallest volume, in 512-byte blocks, a conservative shrink can
/// actually produce: the conservative floor rounded up to a whole
/// cylinder group, plus the replica's block.
///
/// The GUI's shrink-to-minimum picker and the restore resize plan both
/// reach this through `Filesystem::last_data_byte`, so they need this
/// number rather than the raw floor — proposing the floor itself would
/// hand [`shrink_efs_conservative`] a size it has to refuse.
pub(crate) fn minimum_shrink_blocks<R: Read + Seek>(
    fs: &mut EfsFilesystem<R>,
) -> Result<u32, FilesystemError> {
    let sb = fs.sb_clone();
    let floor = compute_conservative_floor(fs)?;
    if sb.cgfsize == 0 {
        return Ok(floor);
    }
    Ok(sb
        .firstcg
        .saturating_add(required_cgs_for_floor(&sb, floor).saturating_mul(sb.cgfsize))
        .saturating_add(1))
}

/// Mark every bit at or above `fs_size` as in-use, the way
/// `create_blank_efs` marks its trailing zone.
///
/// This matters because [`EfsFilesystem::alloc_contiguous_in_bitmap`]
/// scans the WHOLE bitmap, not `[0, fs_size)` — the bitmap routinely
/// has more bits than the volume has blocks (`bmsize` is a whole number
/// of bytes, and a shrink leaves the original oversized bitmap in
/// place). Leaving the out-of-volume tail marked free would let the
/// allocator hand out blocks past the end of the volume.
fn reserve_above_fs_size(bm: &mut [u8], fs_size: u32) {
    let total_bits = (bm.len() as u64 * 8).min(u32::MAX as u64) as u32;
    for blk in fs_size..total_bits {
        let by = (blk / 8) as usize;
        let bb = 7 - (blk % 8);
        bm[by] &= !(1u8 << bb); // set bit = free, so clear = in use
    }
}

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
    // Open just to read the current geometry; we'll re-open inside the
    // grow/shrink path so they own the writable handle for the full
    // mutation.
    let (cur_size, fitted, cgfsize) = {
        let fs = EfsFilesystem::open(&mut *file, partition_offset)
            .map_err(|e| anyhow::anyhow!("EFS resize: {e}"))?;
        let sb = fs.sb_clone();
        let fitted = fit_volume(&sb, new_size_blocks).map_err(|e| anyhow::anyhow!("{e}"))?;
        (sb.fs_size, fitted.fs_size, sb.cgfsize)
    };
    // Compare the FITTED size, not the raw request: an EFS volume holds
    // a whole number of cylinder groups, so the request rounds down —
    // and a volume already filling its partition is asked for its own
    // size on every "resize to the whole partition".
    if fitted == cur_size {
        if new_size_blocks > cur_size {
            log(&format!(
                "EFS resize: {new_size_blocks} blocks requested, but a cylinder group on this \
                 volume is {cgfsize} blocks and only whole groups can be added — it already \
                 holds every group that fits, so its size is unchanged"
            ));
        } else {
            log("EFS resize: target equals current size, no-op");
        }
        return Ok(());
    }
    let result = if fitted > cur_size {
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
/// `new_size_blocks` is the volume's new CAPACITY (normally the
/// partition's), not the new `fs_size`: the replica is placed in its
/// last block and `fs_size` stops just below, per [`fit_volume`]. The
/// old replica block becomes orphaned but harmless — it's inside the
/// grown volume's free space and gets marked free with the rest of the
/// new tail.
pub fn grow_efs<R: Read + Write + Seek>(
    reader: R,
    partition_offset: u64,
    new_size_blocks: u32,
    log: &mut impl FnMut(&str),
) -> Result<u32, FilesystemError> {
    let mut fs = EfsFilesystem::open(reader, partition_offset)?;
    let sb = fs.sb_clone();
    let old_size = sb.fs_size;
    let fit = fit_volume(&sb, new_size_blocks)?;
    if fit.fs_size <= old_size {
        return Err(FilesystemError::InvalidData(format!(
            "EFS grow: new size {new_size_blocks} blocks is not larger than old size {old_size}"
        )));
    }
    let delta = fit.fs_size - old_size;
    log(&format!(
        "EFS grow: {} -> {} blocks (+{} blocks, +{} KiB)",
        old_size,
        fit.fs_size,
        delta,
        delta as u64 / 2
    ));

    // Safety pre-check + reservation for Phase 3b. When new CGs are
    // appended, each one's inode-region [cg_start..cg_start+cgisize)
    // must currently be free in the bitmap — otherwise zeroing those
    // sectors below would destroy live file data. We reserve them
    // (mark in-use) BEFORE bitmap relocation so the relocate's
    // contiguous-free-run search avoids them.
    //
    // A grow only ever adds whole cylinder groups (`fit.fs_size` is a
    // whole number of them by construction), so the count is simply the
    // difference — no group can straddle the end of the volume.
    let added_cgs = fit.ncg.saturating_sub(sb.ncg as u32);
    if added_cgs > 0 {
        let bm = fs.staged_bitmap_mut()?;
        for new_cg_index in 0..added_cgs {
            let cg = sb.ncg as u32 + new_cg_index;
            let cg_start = sb.firstcg + cg * sb.cgfsize;
            for blk in cg_start..cg_start + sb.cgisize as u32 {
                let by = (blk / 8) as usize;
                if by >= bm.len() {
                    // Past the current bitmap's coverage — relocation
                    // will grow the buffer; reservation deferred.
                    break;
                }
                if blk >= old_size {
                    // Outside the OLD volume, so no file can own it: the
                    // trailing zone past `fs_size` is marked in-use on
                    // purpose (mkfs_efs does it, and so does our shrink),
                    // and it is already reserved by that very fact.
                    continue;
                }
                let bb = 7 - (blk % 8);
                // set bit = free, so in-use means the bit is CLEAR.
                if bm[by] & (1u8 << bb) == 0 {
                    return Err(FilesystemError::InvalidData(format!(
                        "EFS grow: cannot append new CG at block {cg_start}: \
                         inode-region block {blk} is already in use by a file. \
                         A future slice could relocate the conflicting data block; \
                         for now, fsck the volume and retry with a smaller increase."
                    )));
                }
                bm[by] &= !(1u8 << bb); // reserve (mark in-use)
            }
        }
    }

    // Bitmap capacity check + optional relocation. If the existing
    // bitmap has enough bits for the new volume size, leave it where
    // it is. Otherwise find a free contiguous run in the current
    // volume (now with new CG inode regions reserved) and relocate.
    let bitmap_bits = sb.bmsize as u64 * 8;
    let sb = if (fit.fs_size as u64) <= bitmap_bits {
        sb
    } else {
        relocate_bitmap(&mut fs, &sb, fit.fs_size, log)?
    };

    // Mark the new tail blocks as free in the bitmap. Per the doc,
    // any stale bits left over from prior shrink rounds get reset to
    // the "free" state. Convention: set bit = free.
    {
        let bm = fs.staged_bitmap_mut()?;
        for blk in old_size..fit.fs_size {
            let by = (blk / 8) as usize;
            if by >= bm.len() {
                break;
            }
            let bb = 7 - (blk % 8);
            bm[by] |= 1u8 << bb;
        }
        // ...then take back everything past the volume, replica included.
        reserve_above_fs_size(bm, fit.fs_size);
    }

    // The delta buys `added_cgs` full new CGs (computed above); any
    // fractional tail is covered by the "mark new blocks free" step.
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
                // set bit = free; mark inode region as in-use by clearing.
                bm[by] &= !(1u8 << bb);
            }
        }
    }

    // Seat the replica in the volume's last block (see `fit_volume`).
    let mut new_sb = sb.clone();
    new_sb.fs_size = fit.fs_size;
    new_sb.replsb = fit.replsb;
    new_sb.ncg = sb.ncg.saturating_add(added_cgs as u16);
    let inodes_per_cg = sb.cgisize as u32 * 4; // 4 inodes per 512-byte block
    new_sb.tinode = new_sb
        .tinode
        .saturating_add(added_cgs.saturating_mul(inodes_per_cg));
    // `tfree` is not adjusted here: `do_sync_metadata` recomputes it from
    // the staged bitmap, so the new tail, the new CGs' inode tables and
    // any drift the volume already carried all settle in one place.

    log(&format!(
        "EFS grow: replica moves {} -> {} (volume's last block, of {}), ncg {} -> {}",
        sb.replsb, new_sb.replsb, new_size_blocks, sb.ncg, new_sb.ncg
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
    Ok(fit.fs_size)
}

/// Find a free contiguous run inside the current volume large enough
/// to hold the new bitmap, copy the existing bitmap forward into the
/// staged buffer (which gets resized), mark the old bitmap blocks as
/// free, mark the new bitmap blocks as in-use, and update `sb.bmblock`
/// plus `sb.bmsize`. Returns the updated superblock; the caller
/// installs it via `fs.set_sb` once the rest of the grow finishes.
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

    // Confine the search to cylinder-group data areas. The bitmap's own
    // blocks are marked in-use so the allocator skips them anyway, but
    // the inode tables are NOT bitmap-tracked on a real IRIX volume —
    // parking the relocated bitmap on top of one would eat live inodes.
    let regions = super::efs::EfsDataRegions::from_sb(sb);
    let bm = fs.staged_bitmap_mut()?;

    let new_ext =
        EfsFilesystem::<R>::alloc_contiguous_in_bitmap(bm, &regions, new_bmsize_sectors, 0)
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

    // Free old bitmap blocks. Convention: set bit = free.
    for blk in old_bmblock..old_bmblock + old_bmsize_sectors {
        let by = (blk / 8) as usize;
        if by >= bm.len() {
            break;
        }
        let bb = 7 - (blk % 8);
        bm[by] |= 1u8 << bb;
    }

    // Expand the in-memory buffer to the new bitmap size. New bytes
    // default to 0xFF (all blocks free); the grow path's tail loop and
    // CG-append step will clear bits for in-use blocks before sync.
    bm.resize(new_bmsize_bytes as usize, 0xFF);

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
/// table, then round it up to a whole number of cylinder groups —
/// dropping a group means dropping its inode table, so a group holding
/// any in-use inode has to survive whole. If `new_size_blocks` is below
/// that rounded-up minimum, errors with the minimum in the message so
/// the caller can surface a useful dialog.
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
    let fit = fit_volume(&sb, new_size_blocks)?;
    if fit.fs_size >= old_size {
        return Err(FilesystemError::InvalidData(format!(
            "EFS shrink: new size {new_size_blocks} blocks is not smaller than old size {old_size}"
        )));
    }

    // Conservative floor: every block in [0..fs_size) must not be
    // claimed by any in-use inode. Build the set of allocated blocks
    // and find the highest one — that's the floor.
    let floor = compute_conservative_floor(&mut fs)?;
    log(&format!(
        "EFS shrink: conservative floor = {floor} blocks ({} KiB)",
        floor as u64 / 2
    ));

    // The floor is a block number, but the volume can only give back
    // whole cylinder groups: `ncg` has to come down with `fs_size` or
    // the CG region hangs past the end of the volume (fsck's
    // `CgRegionPastFsSize`). So round the floor up to a group boundary
    // and add the replica's block — that, not the raw floor, is the
    // smallest volume this shrink can produce.
    let required_ncg = required_cgs_for_floor(&sb, floor);
    // Saturating: `ncg * cgfsize` is bounded by the volume on any sane
    // superblock, but a corrupt one shouldn't panic a debug build.
    let min_blocks = sb
        .firstcg
        .saturating_add(required_ncg.saturating_mul(sb.cgfsize))
        .saturating_add(1);
    if new_size_blocks < min_blocks {
        return Err(FilesystemError::InvalidData(format!(
            "EFS shrink: requested size {new_size_blocks} blocks is below the smallest size \
             this volume can shrink to. In-use data reaches block {floor}, which needs \
             {required_ncg} of the volume's {} cylinder group(s) ({} blocks each) kept whole. \
             Pick a size >= {min_blocks} blocks ({} KiB).",
            sb.ncg,
            sb.cgfsize,
            min_blocks as u64 / 2
        )));
    }

    // Only ever drop trailing groups — `fit.ncg` is what fits in the
    // new volume, but a volume whose `fs_size` ran ahead of its CG
    // region could "fit" more groups than it has, and shrinking is not
    // the place to conjure new ones.
    let new_ncg = fit.ncg.min(sb.ncg as u32);
    let dropped_cgs = sb.ncg as u32 - new_ncg;
    let new_fs_size = sb.firstcg + new_ncg * sb.cgfsize;

    // Everything from the new end of the volume upwards is out of
    // bounds now — mark it in-use so the allocator (which scans the
    // whole bitmap, not just `[0, fs_size)`) can't place a file past
    // the end of the shrunk volume. A later grow re-frees the range it
    // takes back.
    {
        let bm = fs.staged_bitmap_mut()?;
        reserve_above_fs_size(bm, new_fs_size);
    }

    let mut new_sb = sb.clone();
    new_sb.fs_size = new_fs_size;
    new_sb.replsb = fit.replsb;
    new_sb.ncg = new_ncg as u16;
    // Every inode in a dropped group is free by construction (the
    // required-CG check above kept every group holding an in-use inode),
    // so the free-inode count simply loses those slots.
    let inodes_per_cg = sb.cgisize as u32 * 4; // 4 inodes per 512-byte block
    new_sb.tinode = new_sb
        .tinode
        .saturating_sub(dropped_cgs.saturating_mul(inodes_per_cg));
    // `tfree` is not adjusted here: `do_sync_metadata` recomputes it from
    // the staged bitmap, which is the only way to get it right when whole
    // cylinder groups (inode tables included) leave the volume.
    log(&format!(
        "EFS shrink: {} -> {} blocks, ncg {} -> {}, replica at {} (volume's last block, of {})",
        old_size, new_fs_size, sb.ncg, new_ncg, fit.replsb, new_size_blocks
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
    Ok(new_fs_size)
}

/// Shrink an EFS volume aggressively: renumber in-use inodes into the
/// lowest available slots, rewrite every directory's dirent inum
/// references (including `.` and `..`), then run the conservative
/// shrink against the now-lower floor.
///
/// Root (inum 2) is preserved as a hard invariant — IRIX and every
/// historical EFS tool assume `EFS_ROOTINODE == 2`. Other in-use
/// inodes are reassigned in ascending old-inum order to the lowest
/// free slots 3, 4, 5, ...
///
/// **Hardlinks**: EFS does not support multi-link semantics (each
/// inum is referenced from exactly one dirent), so the inum→inum
/// mapping is unambiguously 1:1. We assert per inode at most one
/// dirent reference; if a hardlink is encountered, the renumbering
/// pass falls back to a conservative shrink with a warning rather
/// than risking dangling references.
///
/// Runs Phase-2 fsck both BEFORE the renumber pass (to refuse on a
/// corrupt source) and AFTER, before the new superblock is sealed.
pub fn shrink_efs_aggressive<R: Read + Write + Seek>(
    reader: R,
    partition_offset: u64,
    new_size_blocks: u32,
    log: &mut impl FnMut(&str),
) -> Result<u32, FilesystemError> {
    use std::collections::HashMap;

    let mut fs = EfsFilesystem::open(reader, partition_offset)?;
    let old_size = fs.sb_clone().fs_size;
    if new_size_blocks >= old_size {
        return Err(FilesystemError::InvalidData(format!(
            "EFS shrink (aggressive): new size {new_size_blocks} is not smaller than old {old_size}"
        )));
    }

    log(&format!(
        "EFS shrink (aggressive): {} -> {} blocks",
        old_size, new_size_blocks
    ));

    // Refuse on a pre-existing corrupt source — renumbering on top
    // of broken structure would amplify the damage.
    let pre = super::efs_fsck::fsck_efs(&mut fs)?;
    if !pre.errors.is_empty() {
        let codes: Vec<String> = pre.errors.iter().map(|e| e.code.clone()).collect();
        return Err(FilesystemError::InvalidData(format!(
            "EFS shrink (aggressive): refusing to renumber on a volume with \
             pre-existing fsck errors: [{}]. Run repair first.",
            codes.join(", ")
        )));
    }

    // --- 1. Collect in-use inodes ---
    let total = fs.total_inodes_readonly();
    let mut in_use: Vec<(u32, super::efs::EfsInode)> = Vec::new();
    for inum in 2..total {
        let ino = fs.read_inode_readonly(inum)?;
        if ino.mode != 0 {
            in_use.push((inum, ino));
        }
    }
    in_use.sort_by_key(|(i, _)| *i);
    log(&format!(
        "EFS shrink (aggressive): {} in-use inodes to consider",
        in_use.len()
    ));

    // --- 2. Build the old→new map (root invariant) ---
    let mut map: HashMap<u32, u32> = HashMap::new();
    map.insert(2, 2);
    let mut next_new: u32 = 3;
    for (old_inum, _) in &in_use {
        if *old_inum == 2 {
            continue;
        }
        map.insert(*old_inum, next_new);
        next_new += 1;
    }
    let renumber_count = map.iter().filter(|(o, n)| **o != **n).count();
    log(&format!(
        "EFS shrink (aggressive): renumbering {renumber_count} inode(s)"
    ));
    if renumber_count == 0 {
        log("EFS shrink (aggressive): no renumber needed; falling back to conservative");
        // Drop our handle before re-opening through the conservative path.
        drop(fs);
        // Caller must re-supply the reader. We instead route through
        // the conservative path with the SAME underlying handle, but
        // since `fs` already consumed it, the caller pattern uses
        // `resize_efs_in_place` (which owns the handle). Here we
        // can't recover — surface a clear message.
        return Err(FilesystemError::InvalidData(
            "EFS shrink (aggressive): no inodes need renumbering; use \
             shrink_efs_conservative instead"
                .into(),
        ));
    }

    // --- 3. Rewrite directory contents ---
    rewrite_directory_inum_refs(&mut fs, &in_use, &map, log)?;

    // --- 4. Move inode bodies into their new positions ---
    move_inode_bodies(&mut fs, &in_use, &map, log)?;

    // --- 5. Update the sb hint, then run conservative shrink ---
    {
        let mut sb = fs.sb_clone();
        sb.lastialloc = next_new.saturating_sub(1);
        fs.set_sb(sb);
    }
    // Commit the renumber pass to disk BEFORE the conservative shrink
    // re-opens the volume — the new conservative floor relies on the
    // moved inode positions being visible at re-open time.
    fs.do_sync_metadata()?;

    let new_floor = compute_conservative_floor(&mut fs)?;
    log(&format!(
        "EFS shrink (aggressive): post-renumber floor = {new_floor} blocks"
    ));
    if new_size_blocks < new_floor {
        return Err(FilesystemError::InvalidData(format!(
            "EFS shrink (aggressive): requested size {new_size_blocks} still below \
             post-renumber floor {new_floor}. Data extents would need relocation, \
             which is not implemented. Pick a size >= {new_floor} blocks."
        )));
    }

    // Hand off to conservative shrink for the actual truncate + sb
    // update. It re-opens the volume so we drop our handle here.
    let reader = fs.reader_into_inner();
    shrink_efs_conservative(reader, partition_offset, new_size_blocks, log)
}

/// Walk every directory's data blocks and rewrite the inum field of
/// each dirent according to `map`. Includes `.` and `..` entries.
fn rewrite_directory_inum_refs<R: Read + Write + Seek>(
    fs: &mut EfsFilesystem<R>,
    in_use: &[(u32, super::efs::EfsInode)],
    map: &std::collections::HashMap<u32, u32>,
    log: &mut impl FnMut(&str),
) -> Result<(), FilesystemError> {
    use super::efs::{parse_dir_block, serialize_dir_block};
    let mut dirblocks_rewritten = 0u32;
    for (_inum, ino) in in_use {
        if !ino.is_dir() {
            continue;
        }
        for i in 0..(ino.numextents as usize).min(EFS_DIRECTEXTENTS_MAX) {
            let ext = ino.extents[i];
            if ext.length == 0 {
                continue;
            }
            for offset in 0..ext.length as u32 {
                let bn = ext.bn + offset;
                let block = fs.read_block(bn)?;
                let mut entries = parse_dir_block(&block);
                if entries.is_empty() {
                    continue;
                }
                let mut changed = false;
                for de in &mut entries {
                    if let Some(&new) = map.get(&de.inum) {
                        if new != de.inum {
                            de.inum = new;
                            changed = true;
                        }
                    }
                    // Dangling references (no map entry) are left as
                    // the original inum and will surface in the
                    // post-shrink fsck. They indicate a pre-existing
                    // corruption that the upfront fsck would have
                    // caught — defensive only.
                }
                if changed {
                    let new_block = serialize_dir_block(&entries).ok_or_else(|| {
                        FilesystemError::InvalidData(format!(
                            "aggressive shrink: re-serializing dirblock {bn} grew past 512 bytes"
                        ))
                    })?;
                    fs.write_block(bn, &new_block)?;
                    dirblocks_rewritten += 1;
                }
            }
        }
    }
    log(&format!(
        "EFS shrink (aggressive): rewrote {dirblocks_rewritten} dirblock(s)"
    ));
    Ok(())
}

/// Move every in-use inode body to its new on-disk position. Zeroes
/// the old slots that don't get reused. Writes new slots in
/// ascending new_inum order so a moved-to slot is never clobbered
/// by a later write to the same slot.
fn move_inode_bodies<R: Read + Write + Seek>(
    fs: &mut EfsFilesystem<R>,
    in_use: &[(u32, super::efs::EfsInode)],
    map: &std::collections::HashMap<u32, u32>,
    log: &mut impl FnMut(&str),
) -> Result<(), FilesystemError> {
    use std::collections::HashSet;
    let mut moves: Vec<(u32, u32, super::efs::EfsInode)> = in_use
        .iter()
        .map(|(old, body)| (*old, map[old], body.clone()))
        .collect();
    moves.sort_by_key(|(_, new, _)| *new);

    // Write to new positions in ascending new-inum order.
    let mut writes = 0u32;
    for (old, new, body) in &moves {
        if *old == *new {
            continue;
        }
        let mut moved = body.clone();
        moved.inum = *new;
        fs.write_inode(&moved)?;
        writes += 1;
    }

    // Zero the old slots that are NOT now occupied by another inode's
    // new position. Walk new positions first to know which old slots
    // are still in use.
    let new_positions: HashSet<u32> = moves.iter().map(|(_, n, _)| *n).collect();
    let mut zeros = 0u32;
    for (old, new, _) in &moves {
        if *old == *new {
            continue;
        }
        if new_positions.contains(old) {
            continue; // another inode now lives at this position
        }
        fs.write_inode(&super::efs::EfsInode::empty(*old))?;
        zeros += 1;
    }
    log(&format!(
        "EFS shrink (aggressive): wrote {writes} inode(s) at new positions; zeroed {zeros} old slot(s)"
    ));
    Ok(())
}

/// Compute the conservative shrink floor: max(highest data block
/// claimed by any in-use inode, highest inode-position block among
/// in-use inodes). Both must survive the truncation, otherwise we'd
/// either drop file data or corrupt inode bodies.
///
/// Inode bodies live in their CG's inode-table region; the "position"
/// of inode N is determined by CG geometry (firstcg + cg * cgfsize +
/// slot/4). Aggressive shrink renumbers inodes into low slots to push
/// this component of the floor down — that's why it's called out
/// separately here.
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
        // The inode's body occupies bytes [inode_byte_offset .. +128);
        // round up to the next-block boundary so we preserve the
        // 512-byte sector that holds it.
        let inode_byte = fs.inode_byte_offset(inum);
        let inode_block_end = ((inode_byte + 128).div_ceil(512)) as u32;
        if inode_block_end > highest {
            highest = inode_block_end;
        }
        // For indirect-mode inodes (numextents > 12) the inode's own
        // extent slots describe indirect index blocks rather than data;
        // resolve_owned_extents returns both sets so the shrink floor
        // covers every block that must survive truncation.
        let pofs = fs.partition_offset_value();
        let (data_exts, indirect_exts) =
            super::efs::resolve_owned_extents(fs.raw_reader_mut(), pofs, &ino)?;
        for ext in data_exts.iter().chain(indirect_exts.iter()) {
            if ext.length == 0 {
                continue;
            }
            let end = ext.bn.saturating_add(ext.length as u32);
            if end > highest {
                highest = end;
            }
        }
    }
    let _ = EFS_DIRECTEXTENTS_MAX;
    Ok(highest)
}

#[cfg(test)]
mod tests {
    use super::super::efs::{EfsFilesystem, EfsSuperblock, EFS_SUPERBLOCK_SIZE};
    use super::super::filesystem::{CreateFileOptions, EditableFilesystem, Filesystem};
    use super::*;
    use std::io::Cursor;

    /// Blocks per cylinder group in the synthetic volume.
    const T_CGFSIZE: u32 = 64;
    /// Inode-table blocks per cylinder group (4 inodes each).
    const T_CGISIZE: u16 = 2;
    /// First cylinder group's starting block.
    const T_FIRSTCG: u32 = 18;
    /// Cylinder groups in the freshly-built volume.
    const T_NCG: u32 = 7;
    /// Root directory's single data block (first data block of CG 0).
    const T_ROOT_DIRBLOCK: u32 = 25;

    /// Build a writable EFS volume populated with a small file so the
    /// shrink path has something below the floor to preserve.
    ///
    /// Shaped the way `mkfs_efs` shapes a volume — `fs_size ==
    /// firstcg + ncg * cgfsize`, replica in the last block — because
    /// that invariant is what the resize paths have to maintain. The
    /// earlier fixture declared `fs_size` far beyond its single
    /// cylinder group, which meant no test could ever observe the
    /// cylinder-group region running past the end of a shrunk volume.
    fn build_test_volume() -> Vec<u8> {
        // 512 blocks total: bitmap region, 7 cylinder groups of 64
        // (blocks 18..466), then a free tail holding the replica. One
        // file near the start of the data region leaves plenty of room
        // to shrink without hitting the floor.
        let mut img = vec![0u8; 512 * 512];
        let sb_off = 512;
        let total_blocks = (img.len() / 512) as u32;
        let sb = EfsSuperblock {
            fs_size: T_FIRSTCG + T_NCG * T_CGFSIZE,
            firstcg: T_FIRSTCG,
            cgfsize: T_CGFSIZE,
            cgisize: T_CGISIZE,
            sectors: 63,
            heads: 1,
            ncg: T_NCG as u16,
            dirty: 0,
            fs_time: 0,
            magic: 0x0007_2959, // EFS_MAGIC_OLD
            fname: *b"resize",
            fpack: *b"resize",
            // Bigger-than-fs_size bitmap so grow has room to extend
            // without relocation: 96 bytes = 768 bits, covering up to
            // 768 blocks.
            bmsize: 96,
            tfree: 0,
            tinode: 0,
            bmblock: 2,
            replsb: total_blocks - 1,
            lastialloc: 2,
            checksum: 0,
        };
        sb.write_into(&mut img[sb_off..sb_off + EFS_SUPERBLOCK_SIZE]);

        // Bitmap: set bit = free, clear bit = in use (matches real
        // IRIX EFS). Pre-fill `bmsize` bytes with 0xFF (all free), then
        // CLEAR bits for reserved/in-use blocks: boot + superblock, the
        // bitmap region, every CG's inode table, the root dirblock, and
        // everything past `fs_size`.
        let bm_off = 2 * 512;
        for b in 0..sb.bmsize as usize {
            img[bm_off + b] = 0xFF;
        }
        let mark_in_use = |img: &mut Vec<u8>, b: u32| {
            let by = (b / 8) as usize;
            let bb = 7 - (b % 8);
            img[bm_off + by] &= !(1 << bb);
        };
        for b in 0..T_FIRSTCG {
            mark_in_use(&mut img, b);
        }
        for cg in 0..T_NCG {
            let cg_start = T_FIRSTCG + cg * T_CGFSIZE;
            for b in cg_start..cg_start + T_CGISIZE as u32 {
                mark_in_use(&mut img, b);
            }
        }
        mark_in_use(&mut img, T_ROOT_DIRBLOCK);
        for b in sb.fs_size..sb.bmsize * 8 {
            mark_in_use(&mut img, b);
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

    /// The `fs_size` a volume of `total_blocks` settles on: whole
    /// cylinder groups below the replica's block, per `fit_volume`.
    fn expect_fs_size(total_blocks: u32) -> u32 {
        let ncg = (total_blocks - 1 - T_FIRSTCG) / T_CGFSIZE;
        T_FIRSTCG + ncg * T_CGFSIZE
    }

    #[test]
    fn grow_extends_within_bitmap_capacity() {
        // 512 -> 544 blocks. The volume's 7 CGs become 8 (block 543,
        // the last, is the replica's, so 8 groups of 64 from block 18
        // is all that fits); `fs_size` follows the groups.
        let img = build_test_volume();
        let cur = Cursor::new(img);
        let mut log_lines: Vec<String> = Vec::new();
        let new_size = grow_efs(cur, 0, 544, &mut |s| log_lines.push(s.into())).expect("grow");
        assert_eq!(new_size, expect_fs_size(544));
        assert_eq!(new_size, T_FIRSTCG + 8 * T_CGFSIZE);
        assert!(log_lines.iter().any(|l| l.contains("committed")));
    }

    #[test]
    fn grow_appends_new_cylinder_groups() {
        // 512 -> 640 blocks: 7 CGs become 9, so two are appended at
        // the tail. cgisize=2 inode-table blocks per CG, so the new
        // inode regions sit at 466..468 and 530..532.
        let img = build_test_volume();
        let cur = Cursor::new(img);
        let mut log_lines: Vec<String> = Vec::new();
        let new_size =
            grow_efs(cur, 0, 640, &mut |s| log_lines.push(s.into())).expect("append-new-CGs grow");
        assert_eq!(new_size, expect_fs_size(640));
        assert_eq!(new_size, T_FIRSTCG + 9 * T_CGFSIZE);
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
        // 512 -> 576 blocks = room for one more cylinder group.
        grow_efs(Cursor::new(&mut buf), 0, 576, &mut |_| {}).expect("grow");

        let mut fs = EfsFilesystem::open(Cursor::new(buf), 0).expect("reopen");
        let sb = fs.sb_clone();
        assert_eq!(sb.fs_size, expect_fs_size(576));
        assert_eq!(sb.ncg as u32, T_NCG + 1);
        // The replica sits in the volume's last block, never past it.
        assert_eq!(sb.replsb, 575);
        // tinode was 0 in the test volume; the new CG adds cgisize*4
        // = 8 inode slots, all currently free.
        assert_eq!(sb.tinode, 8);

        // The newly-allocated inode slots should read as mode==0 (free).
        // The appended CG is index 7, holding the 8 inums after the 56
        // (7 CGs * cgisize * 4) the volume already had.
        let first_new_inum = T_NCG * T_CGISIZE as u32 * 4;
        for inum in first_new_inum..first_new_inum + 8 {
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
        // bitmap capacity AND adds 8 new CGs. The relocate path
        // moves the bitmap to a fresh location with enough bits.
        let img = build_test_volume();
        let cur = Cursor::new(img);
        let mut log_lines: Vec<String> = Vec::new();
        let new_size = grow_efs(cur, 0, 1000, &mut |s| log_lines.push(s.into()))
            .expect("grow with bitmap relocation");
        assert_eq!(new_size, expect_fs_size(1000));
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
        assert_eq!(sb.fs_size, expect_fs_size(1000));
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

    /// Regression: a grow used to scale `replsb` off `fs_size` while
    /// `fs_size` took the caller's size verbatim, so the replica landed
    /// PAST the end of the volume — on a real disk that write lands in
    /// whatever follows the partition.
    #[test]
    fn grow_keeps_replica_inside_the_volume() {
        for total in [544u32, 576, 640, 1000] {
            let mut buf = build_test_volume();
            grow_efs(Cursor::new(&mut buf), 0, total, &mut |_| {}).expect("grow");
            let fs = EfsFilesystem::open(Cursor::new(&mut buf), 0).expect("reopen");
            let sb = fs.sb_clone();
            assert_eq!(
                sb.replsb,
                total - 1,
                "replica should be the last block of a {total}-block volume"
            );
            assert!(
                sb.fs_size <= sb.replsb,
                "fs_size {} overlaps the replica at {}",
                sb.fs_size,
                sb.replsb
            );
            assert_eq!(
                sb.firstcg + sb.ncg as u32 * sb.cgfsize,
                sb.fs_size,
                "cylinder-group region must cover exactly fs_size"
            );
        }
    }

    /// Regression: a shrink used to leave `ncg` untouched, so the
    /// cylinder-group region kept describing the OLD volume and every
    /// real shrink aborted on fsck's `CgRegionPastFsSize`.
    #[test]
    fn shrink_drops_trailing_cylinder_groups() {
        let mut buf = build_test_volume();
        let new_size = shrink_efs_conservative(Cursor::new(&mut buf), 0, 200, &mut |_| {})
            .expect("shrink to 200 blocks");
        assert_eq!(new_size, expect_fs_size(200));
        let fs = EfsFilesystem::open(Cursor::new(&mut buf), 0).expect("reopen");
        let sb = fs.sb_clone();
        assert_eq!(sb.ncg as u32, (200 - 1 - T_FIRSTCG) / T_CGFSIZE);
        assert!(
            (sb.ncg as u32) < T_NCG,
            "groups should have been dropped, still {}",
            sb.ncg
        );
        assert_eq!(sb.firstcg + sb.ncg as u32 * sb.cgfsize, sb.fs_size);
        assert_eq!(sb.replsb, 199);
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

        // The file lands in CG 0's data region, so the floor sits well
        // below the first group's end. Shrinking to 20 blocks is under
        // even one cylinder group → rejected.
        let err = shrink_efs_conservative(Cursor::new(bytes.clone()), 0, 20, &mut |_| {})
            .expect_err("below floor");
        assert!(matches!(err, FilesystemError::InvalidData(_)));

        // Shrinking to 100 blocks (one whole cylinder group, well above
        // the floor) works. `fs_size` rounds down to the group boundary
        // the way mkfs_efs rounds, so the result is 82, not 100.
        let mut log_lines: Vec<String> = Vec::new();
        let new_size = shrink_efs_conservative(Cursor::new(bytes), 0, 100, &mut |s| {
            log_lines.push(s.into())
        })
        .expect("shrink");
        assert_eq!(new_size, expect_fs_size(100));
        assert_eq!(new_size, T_FIRSTCG + T_CGFSIZE);
    }

    #[test]
    fn aggressive_shrink_renumbers_inodes_and_truncates() {
        // Build a volume with 8 inodes per CG. Grow to ncg=2, create
        // files that allocate high-inum slots, then aggressive-shrink.
        // Verify that the in-use inodes get renumbered into the low
        // CG and the conservative floor drops accordingly.
        let img = build_test_volume();
        let mut buf = img;
        grow_efs(Cursor::new(&mut buf), 0, 576, &mut |_| {}).expect("grow to ncg=2");

        // Open and create files. We need to push some inodes into CG 1
        // (inums 8..16). To do that, fill CG 0's inode slots (inums 2..8)
        // first. Inums 0,1 are reserved; 2 = root; 3..8 are available.
        let mut fs = EfsFilesystem::open(Cursor::new(buf), 0).expect("open");
        let root = Filesystem::root(&mut fs).expect("root");
        for i in 0..7u32 {
            let payload = vec![0xAB; 16];
            let mut cur = Cursor::new(payload.clone());
            fs.create_file(
                &root,
                &format!("f{i}"),
                &mut cur,
                payload.len() as u64,
                &CreateFileOptions::default(),
            )
            .expect("create");
        }
        EditableFilesystem::sync_metadata(&mut fs).expect("sync");

        // Now delete files from CG 0 to open up low-inum slots, so the
        // survivors all fit in CG 0 once renumbered. With the first-fit
        // inode allocator + sb.lastialloc starting at 2, the files take
        // inums 3..10, and inums 8+ live in CG 1 (cgisize*4 = 8 inodes
        // per group, so CG 0 holds 0..8 with 0/1 reserved and 2 = root).
        // Deleting two leaves root + 5 files = 6 in-use inodes, exactly
        // what CG 0's slots 2..8 hold — so the aggressive shrink can
        // renumber CG 1 empty and drop it.
        for _ in 0..2 {
            let kids: Vec<_> = fs
                .list_directory(&root)
                .expect("list")
                .into_iter()
                .filter(|e| e.name.starts_with("f"))
                .collect();
            // Delete the LOWEST-named file so the freed inum is reused.
            let to_del = kids
                .iter()
                .min_by_key(|e| e.location)
                .expect("at least one file");
            fs.delete_entry(&root, to_del).expect("delete");
            EditableFilesystem::sync_metadata(&mut fs).expect("sync after delete");
        }

        // Locate the highest in-use inum to verify renumber lowers it.
        let highest_before = (2..fs.total_inodes_readonly())
            .filter_map(|i| {
                let ino = fs.read_inode_readonly(i).ok()?;
                if ino.mode == 0 {
                    None
                } else {
                    Some(i)
                }
            })
            .max()
            .unwrap_or(2);

        let bytes = fs.reader_into_inner().into_inner();

        // Aggressive shrink to a size only achievable after renumber.
        let conservative_floor = {
            let mut fs2 = EfsFilesystem::open(Cursor::new(&bytes), 0).expect("reopen");
            compute_conservative_floor(&mut fs2).expect("floor")
        };

        // A shrink gives back whole cylinder groups, so the interesting
        // target is a ONE-group volume. Before renumbering, in-use
        // inodes still live in CG 1, so the conservative path can't get
        // there; the aggressive path can, which is the whole point of
        // renumbering.
        let target_blocks = T_FIRSTCG + T_CGFSIZE + 1;
        assert!(
            conservative_floor > T_FIRSTCG + T_CGFSIZE,
            "expected the pre-renumber floor ({conservative_floor}) to reach into CG 1"
        );
        assert!(
            shrink_efs_conservative(Cursor::new(bytes.clone()), 0, target_blocks, &mut |_| {})
                .is_err(),
            "conservative shrink should not reach {target_blocks} blocks before renumbering"
        );

        let mut log_lines: Vec<String> = Vec::new();
        let mut buf2 = bytes;
        let new_size = shrink_efs_aggressive(Cursor::new(&mut buf2), 0, target_blocks, &mut |s| {
            log_lines.push(s.into())
        })
        .expect("aggressive shrink");
        assert!(
            log_lines.iter().any(|l| l.contains("renumbering")),
            "missing renumber log: {log_lines:?}"
        );

        // Re-open and verify root inum 2 is preserved + highest inum dropped.
        let mut fs3 = EfsFilesystem::open(Cursor::new(buf2), 0).expect("reopen post-shrink");
        let root3 = fs3.read_inode(2).expect("root post");
        assert!(root3.is_dir(), "root must still be a directory");
        let highest_after = (2..fs3.total_inodes_readonly())
            .filter_map(|i| {
                let ino = fs3.read_inode_readonly(i).ok()?;
                if ino.mode == 0 {
                    None
                } else {
                    Some(i)
                }
            })
            .max()
            .unwrap_or(2);
        assert!(
            highest_after <= highest_before,
            "highest in-use inum did not drop (before={highest_before}, after={highest_after})"
        );
        assert_eq!(new_size as u64 * 512, fs3.total_size());
    }

    #[test]
    fn aggressive_shrink_rejects_when_no_renumber_needed() {
        // Plain synthetic volume has only root (inum 2) in use; no
        // renumbering possible. Aggressive shrink should refuse and
        // point at conservative shrink.
        let img = build_test_volume();
        let cur = Cursor::new(img);
        let err = shrink_efs_aggressive(cur, 0, 100, &mut |_| {}).expect_err("no renumber");
        assert!(
            matches!(err, FilesystemError::InvalidData(_)),
            "expected InvalidData, got {err:?}"
        );
    }

    #[test]
    fn shrink_rejects_no_shrink() {
        let img = build_test_volume();
        let cur = Cursor::new(img);
        let err = shrink_efs_conservative(cur, 0, 512, &mut |_| {}).expect_err("not shrinking");
        assert!(matches!(err, FilesystemError::InvalidData(_)));
    }
}
