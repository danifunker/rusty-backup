//! ext2/3/4 integrity check (fsck) and repair.
//!
//! Promotes the old `validate` gate to a real Pass-5-style reconciliation, in
//! the same `analyze()`-shared-by-check-and-repair shape as `fat_fsck` /
//! `exfat_fsck`. The central structures are ext's **block bitmap** and **inode
//! bitmap** (one per block group).
//!
//! ## What it checks
//!
//! The in-use set is computed independently of the on-disk bitmaps:
//! - **Metadata blocks** — from the group descriptors (exact locations): the
//!   superblock + primary GDT in each group that carries a backup (per
//!   `sparse_super`), plus every group's block bitmap, inode bitmap, and inode
//!   table. Reserved-GDT and journal blocks are owned by the reserved inodes and
//!   picked up by the inode walk below.
//! - **Inode-owned blocks** — every in-use inode (reserved inode `< s_first_ino`,
//!   or `mode != 0`) is traced through its extents or indirect blocks, counting
//!   both the data blocks and the metadata blocks that describe them.
//!
//! From that it flags block-bitmap and inode-bitmap divergence, wrong free-block
//! / free-inode counts (superblock and per group), and multiply-claimed blocks.
//!
//! ## What it repairs
//!
//! Rewrites the block + inode bitmaps and the free counts from the computed
//! state — Pass 5's job. On **`metadata_csum`** volumes it additionally verifies
//! and recomputes the crc32c on every structure it touches (superblock,
//! descriptors, bitmap checksums) via a reseal pass driven by `ext_csum` — so
//! ext4 is now check + repair. The legacy
//! **`gdt_csum`/`uninit_bg`** regime (crc16, no bitmap/inode/dir checksums) is
//! still withheld. Multiply-claimed blocks are surfaced only (they need the
//! editor to relocate data).

use std::io::{Read, Seek, Write};

use super::ext::{ExtFilesystem, ExtGeom, ExtGroup};
use super::filesystem::FilesystemError;
use super::fsck::{FsckIssue, FsckResult, FsckStats};

/// A block bitmap indexed by absolute block number.
///
/// Shared with the `ext_format` blank-volume formatter, which reuses the same
/// metadata-marking + bitmap-building helpers so a freshly formatted volume is
/// byte-for-byte what this fsck expects to find.
pub(crate) struct BlockMap {
    bits: Vec<u8>,
}

impl BlockMap {
    pub(crate) fn new(total_blocks: u64) -> Self {
        BlockMap {
            bits: vec![0u8; (total_blocks as usize).div_ceil(8)],
        }
    }
    pub(crate) fn get(&self, block: u64) -> bool {
        let (byte, off) = ((block / 8) as usize, (block % 8) as u8);
        byte < self.bits.len() && self.bits[byte] & (1 << off) != 0
    }
    pub(crate) fn set(&mut self, block: u64) {
        let (byte, off) = ((block / 8) as usize, (block % 8) as u8);
        if byte < self.bits.len() {
            self.bits[byte] |= 1 << off;
        }
    }
}

struct Analysis {
    geom: ExtGeom,
    errors: Vec<FsckIssue>,
    warnings: Vec<FsckIssue>,
    unrepairable: usize,
    fixes: Vec<Fix>,
    files_checked: u32,
    dirs_checked: u32,
    used_blocks: u64,
    used_inodes: u32,
}

/// A concrete repair action.
enum Fix {
    /// Rewrite group `g`'s block bitmap block from the computed bytes.
    BlockBitmap { group: usize, bytes: Vec<u8> },
    /// Rewrite group `g`'s inode bitmap block from the computed bytes.
    InodeBitmap { group: usize, bytes: Vec<u8> },
    /// Patch the superblock free-block / free-inode counts.
    SuperCounts { free_blocks: u64, free_inodes: u32 },
    /// Patch group `g`'s descriptor free-block / free-inode counts.
    GroupCounts {
        group: usize,
        free_blocks: u32,
        free_inodes: u32,
    },
    /// (metadata_csum) Recompute group `g`'s descriptor crc32c: its
    /// block/inode-bitmap checksums (over the on-disk bitmaps) and `bg_checksum`.
    /// Emitted for any group we rewrite, plus any group whose stored checksums are
    /// stale.
    GroupChecksum { group: usize },
    /// (metadata_csum) Recompute the superblock `s_checksum`.
    SuperChecksum,
}

impl Analysis {
    fn err(&mut self, code: &str, message: String, repairable: bool) {
        if !repairable {
            self.unrepairable += 1;
        }
        self.errors.push(FsckIssue {
            code: code.into(),
            message,
            repairable,
            debug: false,
        });
    }
}

pub(crate) fn is_power_of(mut n: u32, base: u32) -> bool {
    if n == 0 {
        return false;
    }
    while n.is_multiple_of(base) {
        n /= base;
    }
    n == 1
}

/// True when group `g` carries a superblock + GDT backup.
pub(crate) fn has_super_backup(g: u32, sparse: bool) -> bool {
    if !sparse {
        return true;
    }
    g == 0 || is_power_of(g, 3) || is_power_of(g, 5) || is_power_of(g, 7)
}

/// Mark the fixed filesystem metadata (super + GDT + per-group bitmaps + inode
/// tables) into the computed block map.
pub(crate) fn mark_metadata(g: &ExtGeom, groups: &[ExtGroup], map: &mut BlockMap) {
    let gdt_blocks = (g.group_count as u64 * g.desc_size as u64).div_ceil(g.block_size);
    let inode_blocks_per_group =
        (g.inodes_per_group as u64 * g.inode_size as u64).div_ceil(g.block_size);

    for (gi, grp) in groups.iter().enumerate() {
        let group_start = g.first_data_block as u64 + gi as u64 * g.blocks_per_group as u64;
        if has_super_backup(gi as u32, g.sparse_super) {
            // Superblock block + the primary GDT blocks that follow it. (Reserved
            // GDT blocks belong to the resize inode and are marked via the inode
            // walk.)
            map.set(group_start);
            for b in (group_start + 1)..(group_start + 1 + gdt_blocks) {
                map.set(b);
            }
        }
        map.set(grp.block_bitmap);
        map.set(grp.inode_bitmap);
        for b in grp.inode_table..(grp.inode_table + inode_blocks_per_group) {
            map.set(b);
        }
    }
}

/// Build the computed group block-bitmap block: one bit per block in the group,
/// bits past the group's real blocks padded to 1 (the ext convention).
pub(crate) fn build_block_bitmap(g: &ExtGeom, group: usize, computed: &BlockMap) -> Vec<u8> {
    let block_size = g.block_size as usize;
    let mut out = vec![0u8; block_size];
    let group_start = g.first_data_block as u64 + group as u64 * g.blocks_per_group as u64;
    let blocks_in_group = (g.total_blocks - group_start).min(g.blocks_per_group as u64);
    for idx in 0..(block_size * 8) {
        let used = if (idx as u64) < blocks_in_group {
            computed.get(group_start + idx as u64)
        } else {
            true // padding past the group's real blocks is marked used
        };
        if used {
            out[idx / 8] |= 1 << (idx % 8);
        }
    }
    out
}

/// Build the computed group inode-bitmap block.
pub(crate) fn build_inode_bitmap(g: &ExtGeom, group: usize, inode_used: &[bool]) -> Vec<u8> {
    let block_size = g.block_size as usize;
    let mut out = vec![0u8; block_size];
    for idx in 0..(block_size * 8) {
        let used = if (idx as u32) < g.inodes_per_group {
            let inum = group as u32 * g.inodes_per_group + idx as u32 + 1;
            (inum as usize) < inode_used.len() && inode_used[inum as usize]
        } else {
            true // padding
        };
        if used {
            out[idx / 8] |= 1 << (idx % 8);
        }
    }
    out
}

/// The shared analysis pass. Read-only.
fn analyze<R: Read + Seek + Send>(fs: &mut ExtFilesystem<R>) -> Result<Analysis, FilesystemError> {
    let (geom, groups) = fs.fsck_geometry()?;
    // metadata_csum volumes are now repairable — we recompute crc32c on every
    // structure we rewrite (see the reseal pass in `repair_ext`). The legacy
    // gdt_csum-only regime (crc16, no bitmap/inode/dir csums) is still withheld.
    let repairable = !geom.checksummed || geom.metadata_csum;
    let mut a = Analysis {
        geom,
        errors: Vec::new(),
        warnings: Vec::new(),
        unrepairable: 0,
        fixes: Vec::new(),
        files_checked: 0,
        dirs_checked: 0,
        used_blocks: 0,
        used_inodes: 0,
    };
    let g = geom;

    if geom.checksummed && !geom.metadata_csum {
        a.warnings.push(FsckIssue {
            code: "MetadataChecksum".into(),
            message: "volume uses the legacy gdt_csum/uninit_bg checksums (crc16); issues are \
                      reported but repair is withheld for this legacy format"
                .into(),
            repairable: false,
            debug: false,
        });
    }

    // ---- Compute the in-use block + inode sets. ----
    let mut computed = BlockMap::new(g.total_blocks);
    mark_metadata(&g, &groups, &mut computed);

    // inode_used[inum] = true when inode is allocated. Index 0 unused.
    let mut inode_used = vec![false; g.inodes_count as usize + 1];
    // Track blocks already claimed to surface multiply-claimed corruption.
    let mut multiply_claimed = 0u64;

    for inum in 1..=g.inodes_count {
        let ino = match fs.fsck_read_inode(inum) {
            Ok(i) => i,
            Err(_) => continue,
        };
        let reserved = inum < g.first_ino;
        let in_use = reserved || ino.mode != 0;
        if !in_use {
            continue;
        }
        inode_used[inum as usize] = true;
        a.used_inodes += 1;
        if ino.mode != 0 {
            if ino.mode & 0xF000 == 0x4000 {
                a.dirs_checked += 1;
            } else {
                a.files_checked += 1;
            }
        }

        let blocks = fs.fsck_owned_blocks(&ino)?;
        for b in blocks {
            if computed.get(b) {
                multiply_claimed += 1;
            } else {
                computed.set(b);
            }
        }
    }

    if multiply_claimed > 0 {
        a.err(
            "MultiplyClaimedBlocks",
            format!("{multiply_claimed} block(s) are claimed by more than one inode / metadata"),
            false,
        );
    }

    // ---- Reconcile block bitmaps + count free blocks. ----
    let mut computed_free_blocks = 0u64;
    for (gi, grp) in groups.iter().enumerate() {
        let group_start = g.first_data_block as u64 + gi as u64 * g.blocks_per_group as u64;
        let blocks_in_group = (g.total_blocks - group_start).min(g.blocks_per_group as u64);
        let on_disk = fs.read_block_bitmap(gi)?;
        let mut group_free = 0u32;
        let mut diff = 0u64;
        for idx in 0..blocks_in_group {
            let block = group_start + idx;
            let byte = (idx / 8) as usize;
            let disk = byte < on_disk.len() && on_disk[byte] & (1 << (idx % 8)) != 0;
            let want = computed.get(block);
            if disk != want {
                diff += 1;
            }
            if !want {
                group_free += 1;
                computed_free_blocks += 1;
            } else {
                a.used_blocks += 1;
            }
        }
        if diff > 0 {
            a.err(
                "BlockBitmapDifference",
                format!(
                    "group {gi}: {diff} block-bitmap bit(s) disagree with the computed allocation"
                ),
                repairable,
            );
            if repairable {
                a.fixes.push(Fix::BlockBitmap {
                    group: gi,
                    bytes: build_block_bitmap(&g, gi, &computed),
                });
            }
        }
        if grp.free_blocks != group_free {
            a.err(
                "GroupFreeBlocksWrong",
                format!(
                    "group {gi}: descriptor free-blocks is {}, computed {group_free}",
                    grp.free_blocks
                ),
                repairable,
            );
            if repairable {
                a.fixes.push(Fix::GroupCounts {
                    group: gi,
                    free_blocks: group_free,
                    free_inodes: grp.free_inodes,
                });
            }
        }
    }

    // ---- Reconcile inode bitmaps + count free inodes. ----
    let mut computed_free_inodes = 0u32;
    for (gi, grp) in groups.iter().enumerate() {
        let on_disk = fs.read_inode_bitmap(gi)?;
        let mut group_free = 0u32;
        let mut diff = 0u64;
        for idx in 0..g.inodes_per_group {
            let inum = gi as u32 * g.inodes_per_group + idx + 1;
            let byte = (idx / 8) as usize;
            let disk = byte < on_disk.len() && on_disk[byte] & (1 << (idx % 8)) != 0;
            let want = (inum as usize) < inode_used.len() && inode_used[inum as usize];
            if disk != want {
                diff += 1;
            }
            if !want {
                group_free += 1;
                computed_free_inodes += 1;
            }
        }
        if diff > 0 {
            a.err(
                "InodeBitmapDifference",
                format!(
                    "group {gi}: {diff} inode-bitmap bit(s) disagree with the computed allocation"
                ),
                repairable,
            );
            if repairable {
                a.fixes.push(Fix::InodeBitmap {
                    group: gi,
                    bytes: build_inode_bitmap(&g, gi, &inode_used),
                });
            }
        }
        // Fold the corrected inode free count into the (already-queued) group
        // descriptor fix, or add one if only the inode side is wrong.
        if grp.free_inodes != group_free {
            let block_free = grp.free_blocks; // may be superseded below
                                              // If a GroupCounts fix already exists for this group, update it.
            if let Some(Fix::GroupCounts { free_inodes, .. }) = a
                .fixes
                .iter_mut()
                .find(|f| matches!(f, Fix::GroupCounts { group, .. } if *group == gi))
            {
                *free_inodes = group_free;
            } else {
                a.err(
                    "GroupFreeInodesWrong",
                    format!(
                        "group {gi}: descriptor free-inodes is {}, computed {group_free}",
                        grp.free_inodes
                    ),
                    repairable,
                );
                if repairable {
                    a.fixes.push(Fix::GroupCounts {
                        group: gi,
                        free_blocks: block_free,
                        free_inodes: group_free,
                    });
                }
            }
        }
    }

    // ---- Superblock free counts. ----
    if g.free_blocks != computed_free_blocks {
        a.err(
            "FreeBlocksCountWrong",
            format!(
                "superblock free-blocks is {}, computed {computed_free_blocks}",
                g.free_blocks
            ),
            repairable,
        );
    }
    if g.free_inodes != computed_free_inodes {
        a.err(
            "FreeInodesCountWrong",
            format!(
                "superblock free-inodes is {}, computed {computed_free_inodes}",
                g.free_inodes
            ),
            repairable,
        );
    }
    if repairable
        && (g.free_blocks != computed_free_blocks || g.free_inodes != computed_free_inodes)
    {
        a.fixes.push(Fix::SuperCounts {
            free_blocks: computed_free_blocks,
            free_inodes: computed_free_inodes,
        });
    }

    // ---- metadata_csum: verify checksums and queue reseals. ----
    if g.metadata_csum {
        csum_audit(fs, &g, &groups, &mut a)?;
    }

    Ok(a)
}

/// For a `metadata_csum` volume: queue a checksum reseal for every group/super we
/// already produced a content fix for, and additionally verify the stored
/// superblock / descriptor / bitmap checksums, queuing a reseal (with a reported
/// issue) for any that are stale. Read-only — the reseal `Fix`es are applied by
/// `repair_ext` after the content fixes.
fn csum_audit<R: Read + Seek + Send>(
    fs: &mut ExtFilesystem<R>,
    g: &ExtGeom,
    groups: &[ExtGroup],
    a: &mut Analysis,
) -> Result<(), FilesystemError> {
    use std::collections::BTreeSet;
    let seed = g.csum_seed;

    let mut reseal_groups: BTreeSet<usize> = BTreeSet::new();
    let mut reseal_super = false;
    for f in &a.fixes {
        match f {
            Fix::BlockBitmap { group, .. }
            | Fix::InodeBitmap { group, .. }
            | Fix::GroupCounts { group, .. } => {
                reseal_groups.insert(*group);
            }
            Fix::SuperCounts { .. } => reseal_super = true,
            _ => {}
        }
    }

    // Superblock checksum.
    let sb_off = g.partition_offset + 1024;
    let sb = fs.read_raw(sb_off, 1024)?;
    let stored = u32::from_le_bytes(
        sb[super::ext_csum::SB_CSUM_OFF..super::ext_csum::SB_CSUM_OFF + 4]
            .try_into()
            .unwrap(),
    );
    if super::ext_csum::superblock_csum(&sb) != stored && !reseal_super {
        a.err(
            "SuperblockChecksumWrong",
            "superblock crc32c is stale".into(),
            true,
        );
        reseal_super = true;
    }

    // Per-group descriptor + bitmap checksums. A cloned descriptor re-stamped over
    // its current content differs from the original ONLY in the checksum fields,
    // so an inequality is exactly a stale checksum.
    let gdt_start = (g.first_data_block as u64 + 1) * g.block_size + g.partition_offset;
    let ibm_len = (g.inodes_per_group / 8) as usize;
    for (gi, grp) in groups.iter().enumerate() {
        if reseal_groups.contains(&gi) {
            continue;
        }
        let desc = fs.read_raw(
            gdt_start + gi as u64 * g.desc_size as u64,
            g.desc_size as usize,
        )?;
        let bbm = fs.read_raw(
            g.partition_offset + grp.block_bitmap * g.block_size,
            g.block_size as usize,
        )?;
        let ibm = fs.read_raw(
            g.partition_offset + grp.inode_bitmap * g.block_size,
            ibm_len,
        )?;
        let mut expected = desc.clone();
        super::ext_csum::stamp_block_bitmap_csum(seed, &mut expected, &bbm);
        super::ext_csum::stamp_inode_bitmap_csum(seed, &mut expected, &ibm);
        super::ext_csum::stamp_group_desc(seed, gi as u32, &mut expected);
        if expected != desc {
            a.err(
                "GroupChecksumWrong",
                format!("group {gi} descriptor/bitmap crc32c is stale"),
                true,
            );
            reseal_groups.insert(gi);
        }
    }

    for gi in reseal_groups {
        a.fixes.push(Fix::GroupChecksum { group: gi });
    }
    if reseal_super {
        a.fixes.push(Fix::SuperChecksum);
    }
    Ok(())
}

fn stats(a: &Analysis) -> FsckStats {
    FsckStats {
        files_checked: a.files_checked,
        directories_checked: a.dirs_checked,
        extra: vec![
            ("block_size".into(), a.geom.block_size.to_string()),
            ("blocks_total".into(), a.geom.total_blocks.to_string()),
            ("blocks_used".into(), a.used_blocks.to_string()),
            ("inodes_used".into(), a.used_inodes.to_string()),
        ],
    }
}

/// Run the ext integrity check (read-only).
pub fn fsck_ext<R: Read + Seek + Send>(
    fs: &mut ExtFilesystem<R>,
) -> Result<FsckResult, FilesystemError> {
    let a = analyze(fs)?;
    let repairable = a.errors.iter().any(|e| e.repairable);
    let stats = stats(&a);
    Ok(FsckResult {
        errors: a.errors,
        warnings: a.warnings,
        stats,
        repairable,
        orphaned_entries: Vec::new(),
    })
}

/// Apply the block/inode-bitmap and free-count fixes `analyze` produced.
pub fn repair_ext<R: Read + Write + Seek + Send>(
    fs: &mut ExtFilesystem<R>,
) -> Result<super::fsck::RepairReport, FilesystemError> {
    let a = analyze(fs)?;
    let g = a.geom;

    // Legacy gdt_csum/uninit_bg (crc16) is still withheld; metadata_csum is
    // repaired with a crc32c reseal pass (the GroupChecksum/SuperChecksum fixes
    // `analyze` queued after the content fixes).
    if g.checksummed && !g.metadata_csum {
        return Ok(super::fsck::RepairReport {
            fixes_applied: Vec::new(),
            fixes_failed: Vec::new(),
            unrepairable_count: a.errors.len(),
        });
    }

    // The GDT begins in the block after the primary superblock block.
    let gdt_start = (g.first_data_block as u64 + 1) * g.block_size + g.partition_offset;
    let is_64bit_desc = g.desc_size >= 64;

    let mut applied: Vec<String> = Vec::new();
    let mut failed: Vec<String> = Vec::new();

    for fix in &a.fixes {
        let r: Result<String, FilesystemError> = match fix {
            Fix::BlockBitmap { group, bytes } => fs
                .write_block_bitmap(*group, bytes)
                .map(|_| format!("rewrote block bitmap for group {group}")),
            Fix::InodeBitmap { group, bytes } => fs
                .write_inode_bitmap(*group, bytes)
                .map(|_| format!("rewrote inode bitmap for group {group}")),
            Fix::SuperCounts {
                free_blocks,
                free_inodes,
            } => {
                let sb_off = g.partition_offset + 1024;
                (|| {
                    fs.write_raw(sb_off + 0x0C, &(*free_blocks as u32).to_le_bytes())?;
                    fs.write_raw(sb_off + 0x10, &free_inodes.to_le_bytes())?;
                    fs.write_raw(sb_off + 0x158, &((*free_blocks >> 32) as u32).to_le_bytes())?;
                    Ok(format!(
                        "set superblock free counts ({free_blocks} blocks, {free_inodes} inodes)"
                    ))
                })()
            }
            Fix::GroupCounts {
                group,
                free_blocks,
                free_inodes,
            } => {
                let desc_off = gdt_start + *group as u64 * g.desc_size as u64;
                (|| {
                    fs.write_raw(desc_off + 0x0C, &(*free_blocks as u16).to_le_bytes())?;
                    fs.write_raw(desc_off + 0x0E, &(*free_inodes as u16).to_le_bytes())?;
                    if is_64bit_desc {
                        fs.write_raw(
                            desc_off + 0x2C,
                            &((*free_blocks >> 16) as u16).to_le_bytes(),
                        )?;
                        fs.write_raw(
                            desc_off + 0x2E,
                            &((*free_inodes >> 16) as u16).to_le_bytes(),
                        )?;
                    }
                    Ok(format!(
                        "set group {group} free counts ({free_blocks} blocks, {free_inodes} inodes)"
                    ))
                })()
            }
            // Reseal fixes run after the content fixes above, so they re-stamp over
            // already-corrected bitmaps / counts.
            Fix::GroupChecksum { group } => reseal_group_csum(fs, &g, gdt_start, *group)
                .map(|_| format!("resealed group {group} checksums")),
            Fix::SuperChecksum => reseal_super_csum(fs, g.partition_offset)
                .map(|_| "resealed superblock checksum".into()),
        };
        match r {
            Ok(desc) => applied.push(desc),
            Err(e) => failed.push(format!("{e}")),
        }
    }

    if !applied.is_empty() {
        fs.flush_writer()?;
    }

    Ok(super::fsck::RepairReport {
        fixes_applied: applied,
        fixes_failed: failed,
        unrepairable_count: a.unrepairable,
    })
}

/// Re-stamp group `group`'s descriptor crc32c: its block/inode-bitmap checksums
/// (computed over the on-disk bitmaps) and `bg_checksum`. The bitmap block numbers
/// come from the descriptor itself.
fn reseal_group_csum<R: Read + Write + Seek + Send>(
    fs: &mut ExtFilesystem<R>,
    g: &ExtGeom,
    gdt_start: u64,
    group: usize,
) -> Result<(), FilesystemError> {
    let seed = g.csum_seed;
    let is64 = g.desc_size >= 64;
    let desc_off = gdt_start + group as u64 * g.desc_size as u64;
    let mut desc = fs.read_raw(desc_off, g.desc_size as usize)?;
    let read_block_no = |d: &[u8], lo: usize, hi: usize| -> u64 {
        let mut v = u32::from_le_bytes(d[lo..lo + 4].try_into().unwrap()) as u64;
        if is64 {
            v |= (u32::from_le_bytes(d[hi..hi + 4].try_into().unwrap()) as u64) << 32;
        }
        v
    };
    let bb = read_block_no(&desc, 0x00, 0x20); // bg_block_bitmap
    let ib = read_block_no(&desc, 0x04, 0x24); // bg_inode_bitmap
    let bbm = fs.read_raw(
        g.partition_offset + bb * g.block_size,
        g.block_size as usize,
    )?;
    let ibm = fs.read_raw(
        g.partition_offset + ib * g.block_size,
        (g.inodes_per_group / 8) as usize,
    )?;
    // Stamp bitmap csums before bg_checksum — bg_checksum covers them.
    super::ext_csum::stamp_block_bitmap_csum(seed, &mut desc, &bbm);
    super::ext_csum::stamp_inode_bitmap_csum(seed, &mut desc, &ibm);
    super::ext_csum::stamp_group_desc(seed, group as u32, &mut desc);
    fs.write_raw(desc_off, &desc)
}

/// Re-stamp the superblock crc32c.
fn reseal_super_csum<R: Read + Write + Seek + Send>(
    fs: &mut ExtFilesystem<R>,
    partition_offset: u64,
) -> Result<(), FilesystemError> {
    let sb_off = partition_offset + 1024;
    let mut sb = fs.read_raw(sb_off, 1024)?;
    super::ext_csum::stamp_superblock(&mut sb);
    fs.write_raw(sb_off, &sb)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::fs::ext::ExtFilesystem;
    use std::io::Cursor;

    type Img = Cursor<Vec<u8>>;

    fn load(name: &str) -> Img {
        let path = format!("tests/fixtures/{name}");
        let compressed = std::fs::read(&path).unwrap_or_else(|e| panic!("read {path}: {e}"));
        let mut dec = zstd::stream::read::Decoder::new(Cursor::new(compressed)).expect("zstd");
        let mut out = Vec::new();
        dec.read_to_end(&mut out).expect("decompress");
        Cursor::new(out)
    }

    fn open(img: &mut Img) -> ExtFilesystem<&mut Img> {
        ExtFilesystem::open(img, 0).expect("open ext")
    }

    fn run_fsck(img: &mut Img) -> FsckResult {
        let mut fs = open(img);
        fsck_ext(&mut fs).expect("fsck runs")
    }

    fn run_repair(img: &mut Img) -> super::super::fsck::RepairReport {
        let mut fs = open(img);
        repair_ext(&mut fs).expect("repair runs")
    }

    fn codes(r: &FsckResult) -> Vec<String> {
        r.errors.iter().map(|e| e.code.clone()).collect()
    }

    /// Corrupt the superblock free-block count to a wrong value.
    fn corrupt_super_free_blocks(img: &mut Img, wrong: u32) {
        let mut fs = open(img);
        fs.write_raw(1024 + 0x0C, &wrong.to_le_bytes()).unwrap();
        fs.flush_writer().unwrap();
    }

    #[test]
    fn clean_ext2_and_ext4_report_no_errors() {
        // ext2 (no checksums) and ext4 (metadata_csum) must both reconcile with
        // no errors — the accounting exactly matches a known-clean image.
        for name in ["test_ext2.img.zst", "test_ext4.img.zst"] {
            let mut img = load(name);
            let r = run_fsck(&mut img);
            assert!(r.is_clean(), "{name}: reported errors {:?}", r.errors);
        }
    }

    #[test]
    fn ext2_detects_and_repairs_free_count_and_bitmap() {
        let mut img = load("test_ext2.img.zst");
        // Wrong superblock free count + a leaked block-bitmap bit.
        corrupt_super_free_blocks(&mut img, 999);
        {
            let mut fs = open(&mut img);
            let mut bm = fs.read_block_bitmap(0).unwrap();
            // Find a free block bit (>= block 100 to stay clear of metadata) and
            // mark it used — a bitmap-vs-allocation disagreement.
            let idx = (100u64..fs.fsck_geometry().unwrap().0.total_blocks)
                .find(|&b| bm[(b / 8) as usize] & (1 << (b % 8)) == 0)
                .expect("a free block");
            bm[(idx / 8) as usize] |= 1 << (idx % 8);
            fs.write_block_bitmap(0, &bm).unwrap();
            fs.flush_writer().unwrap();
        }

        let r = run_fsck(&mut img);
        assert!(
            codes(&r).contains(&"FreeBlocksCountWrong".to_string()),
            "{:?}",
            r.errors
        );
        assert!(
            codes(&r).contains(&"BlockBitmapDifference".to_string()),
            "{:?}",
            r.errors
        );

        let rep = run_repair(&mut img);
        assert!(rep.fixes_failed.is_empty(), "{:?}", rep.fixes_failed);
        assert!(run_fsck(&mut img).is_clean(), "not clean after repair");
    }

    #[test]
    fn ext4_metadata_csum_detects_and_repairs() {
        let mut img = load("test_ext4.img.zst");
        // Patching the free count via write_raw invalidates the value AND leaves
        // the superblock crc32c stale.
        corrupt_super_free_blocks(&mut img, 12345);

        let r = run_fsck(&mut img);
        assert!(
            codes(&r).contains(&"FreeBlocksCountWrong".to_string()),
            "{:?}",
            r.errors
        );
        // metadata_csum is now repairable — we recompute crc32c (the content fix
        // implies the superblock reseal, so it isn't reported as a separate csum
        // issue).
        assert!(r.repairable, "metadata_csum should now be repairable");

        let rep = run_repair(&mut img);
        assert!(rep.fixes_failed.is_empty(), "{:?}", rep.fixes_failed);
        let after = run_fsck(&mut img);
        assert!(
            after.is_clean(),
            "not clean after repair: {:?}",
            after.errors
        );
    }

    #[test]
    fn ext4_detects_and_repairs_stale_superblock_checksum() {
        // A pure checksum corruption: valid content, wrong s_checksum.
        let mut img = load("test_ext4.img.zst");
        {
            let mut fs = open(&mut img);
            fs.write_raw(1024 + 0x3FC, &0xDEAD_BEEFu32.to_le_bytes())
                .unwrap();
            fs.flush_writer().unwrap();
        }
        let r = run_fsck(&mut img);
        assert!(
            codes(&r).contains(&"SuperblockChecksumWrong".to_string()),
            "{:?}",
            r.errors
        );
        assert!(
            !codes(&r).contains(&"FreeBlocksCountWrong".to_string()),
            "content must be untouched: {:?}",
            r.errors
        );
        assert!(r.repairable);
        let rep = run_repair(&mut img);
        assert!(rep.fixes_failed.is_empty(), "{:?}", rep.fixes_failed);
        let after = run_fsck(&mut img);
        assert!(
            after.is_clean(),
            "not clean after repair: {:?}",
            after.errors
        );
    }
}
