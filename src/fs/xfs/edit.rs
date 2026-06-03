//! XFS write primitives (§3 of `docs/xfs_edit_and_repair.md`) — the shared
//! foundation for Track-B editing (`create_directory` here) and R7 orphan
//! reconnection. Writes straight through to the device (no in-memory
//! staging), like the repair path.
//!
//! **Format coverage**: v4 (no CRC) is the original target. v5 (CRC) is
//! being landed structure-by-structure under §2.1 hole (E):
//!   - **E.2 (this commit)** wires v3 inode core stamping
//!     ([`v5_crc::stamp_inode_v3`]) into [`write_inode_region`] and into the
//!     fresh-chunk initializer ([`init_free_inode_chunk`]). Upstream callers
//!     still gate on v5 — `alloc_inode_slot` and `alloc_new_inode_chunk`
//!     because the inobt leaf splice / AGI / AGFL / superblock writes don't
//!     yet stamp CRCs (E.4 / E.5).
//!   - The dir3 / bmbt / sblock-CRC / AGF / AGI / SB stamp work lives in
//!     later slices; until then the v5 gate at each alloc entry point keeps
//!     the volume internally consistent (no write paths active).
//!
//! Primitives, smallest first:
//!
//!   * **Inode-slot allocation** ([`alloc_inode_slot`]) / freeing
//!     ([`free_inode`]): claim/release an inode in an existing inode-btree
//!     chunk (flip `ir_free`, adjust the record freecount + AGI freecount +
//!     `sb_ifree`). When every existing chunk is full, `alloc_new_inode_chunk`
//!     carves a fresh 64-inode chunk via [`alloc_blocks_aligned`], writes 64
//!     free dinodes, then splices a new record into the AG's inobt and bumps
//!     the AGI + superblock inode counters; the slot search then runs again
//!     and claims slot 0 of the new chunk. Two splice strategies pick
//!     themselves based on tree shape: single-leaf splice when the AGI root
//!     is a single leaf with room, multi-level grow (via
//!     [`build_sblock_btree`]) when the existing tree is multi-level or its
//!     only leaf is full — the latter reclaims the old tree blocks in the
//!     same freespace rebuild that allocates the new ones.
//!   * **Block allocation** ([`alloc_blocks`]) / freeing ([`free_blocks`]):
//!     carve/return contiguous blocks by rebuilding the AG free-space btrees
//!     (reusing the R2 rebuild) + resyncing `sb_fdblocks`.
//!   * **Directory insert** ([`dir_insert_entry`]) / remove ([`sf_remove_entry`]):
//!     short-form inline add/remove, automatic short-form→single-block
//!     conversion on overflow ([`build_block_dir`] writes the data entries,
//!     `bestfree`, and the sorted hash leaf index), and rebuild-in-place inserts
//!     for an already single-block directory. Leaf/node (multi-block)
//!     directories are still `Unsupported`.
//!
//! On top of these: `create_directory`, `create_file` (single contiguous
//! extent), and `delete_entry` (empty short-form dirs / single-or-no-extent
//! files). Porting reference: `xfs_repair` @ v3.1.11 + `libxfs/xfs_dir2_sf.c`,
//! `libxfs/xfs_dir2_block.c`, `libxfs/xfs_ialloc.c`.

use std::io::{Read, Seek, Write};

use byteorder::{BigEndian, ByteOrder};

use super::ag::{XfsAgf, XfsAgi};
use super::bmap::{encode_extent, fsblock_to_partition_byte, NULLFSBLOCK, XFS_BMAP_MAGIC};
use super::btree_build::{blocks_needed, blocks_needed_for, build_sblock_btree, FreeExtent};
use super::dir2::XFS_DIR2_DATA_HDR_LEN;
use super::freespace_rebuild::{carve_aligned_from_largest, carve_from_largest, coalesce};
use super::types::{
    XFS_ABTB_MAGIC, XFS_ABTC_MAGIC, XFS_DINODE_MAGIC, XFS_IBT_CRC_MAGIC, XFS_IBT_MAGIC,
    XFS_INODES_PER_CHUNK,
};
use super::{read_at_aligned, XfsFilesystem};
use crate::fs::filesystem::{Filesystem, FilesystemError};
use crate::fs::fsck::RepairReport;

/// Largest single-extent file we write: the on-disk blockcount field is 21 bits.
const MAX_SINGLE_EXTENT_BLOCKS: u64 = (1 << 21) - 1;

/// One bmap extent record (`xfs_bmbt_rec`) on disk.
const BMBT_REC_SIZE: usize = 16;

/// A directory's child entries as `(name, inode, ftype)`, excluding `.`/`..`.
type DirEntries = Vec<(String, u64, u8)>;

/// dir2 filetype values (only the two we emit).
const XFS_DIR3_FT_REG_FILE: u8 = 1;
const XFS_DIR3_FT_DIR: u8 = 2;

/// NULL agino sentinel stored in `di_next_unlinked` for a not-unlinked inode.
const NULLAGINO: u32 = 0xFFFF_FFFF;

/// inobt leaf record layout: startino(4) freecount(4) free(8) — same on v4
/// and v5.
const INOBT_REC_SIZE: usize = 16;
/// inobt key is the leading 4-byte `startino` field; same width as a btree
/// pointer.
const INOBT_KEY_SIZE: usize = 4;

/// Stamp the v5 sblock-crc header tuple (`bb_blkno` / `bb_lsn` / `bb_uuid` /
/// `bb_owner` / `bb_crc`) on an inobt / bnobt / cntbt block buffer, given
/// the host AG, the block's AG-relative number, and the superblock. v4
/// callers must guard with `sb.is_v5()` — this helper unconditionally
/// stamps. Mirrors `stamp_v5_sblock` in `btree_build` but exposed for the
/// edit-path leaf rewrites (splice / grow callers reach for it directly).
fn stamp_v5_sblock_for_ag(buf: &mut [u8], sb: &super::sb::XfsSuperblock, agno: u64, agbno: u32) {
    let fsblock = (agno << sb.agblklog) | agbno as u64;
    let blkno = super::v5_crc::fsblock_to_daddr(fsblock, sb);
    super::v5_crc::stamp_sblock_crc_header(buf, blkno, agno as u32, sb);
}

/// AGI / superblock counter offsets.
const AGI_COUNT_OFF: usize = 16;
const AGI_ROOT_OFF: usize = 20;
const AGI_LEVEL_OFF: usize = 24;
const AGI_FREECOUNT_OFF: usize = 28;
const SB_ICOUNT_OFF: usize = 128;
const SB_IFREE_OFF: usize = 136;

/// `di_next_unlinked` lives 96 bytes into every dinode.
const DI_NEXT_UNLINKED_OFF: usize = 96;

/// `xfs_dir2_data_entsize`: the space one entry occupies in the notional data
/// block — `inumber(8) + namelen(1) + name + [ftype] + tag(2)`, rounded up to 8.
/// The short-form *offset cookie* of each entry is its byte offset in that
/// block, so this is how we advance the cookie when appending.
fn data_entsize(namelen: usize, has_ftype: bool) -> usize {
    let raw = 11 + namelen + usize::from(has_ftype);
    raw.div_ceil(8) * 8
}

impl<R: Read + Write + Seek + Send> XfsFilesystem<R> {
    /// Allocate one inode. Tries existing chunks first; when every chunk in
    /// every AG is full, allocates a fresh 64-inode chunk via
    /// [`alloc_new_inode_chunk`] and re-runs the slot search against it.
    /// Returns the new inode number. v4 only.
    pub(crate) fn alloc_inode_slot(
        &mut self,
        sb: &super::sb::XfsSuperblock,
    ) -> Result<u64, FilesystemError> {
        // v5 lifted in §2.1 (E.5b): inobt-leaf rewrites + AGI / SB writes are
        // CRC-aware; new-chunk allocation routes through `init_free_inode_chunk`
        // which stamps each v3 inode core (E.2). The grow / single-leaf splice
        // paths both stamp the rewritten leaf's sblock-crc header before write.
        if let Some(ino) = self.try_claim_slot_from_existing_chunks(sb)? {
            return Ok(ino);
        }
        // No free slots anywhere: extend an AG's inobt with a brand-new chunk,
        // then claim slot 0 of it.
        self.alloc_new_inode_chunk(sb)?;
        self.try_claim_slot_from_existing_chunks(sb)?
            .ok_or_else(|| {
                FilesystemError::DiskFull(
                    "alloc_new_inode_chunk left no claimable slot (internal inconsistency)".into(),
                )
            })
    }

    /// First-pass slot search: walk every AG's existing inobt, claim the
    /// lowest free slot in the first record that has one. Returns `Ok(None)`
    /// when every chunk is full (the trigger for `alloc_new_inode_chunk`).
    /// Lifted from the old `alloc_inode_slot` body.
    fn try_claim_slot_from_existing_chunks(
        &mut self,
        sb: &super::sb::XfsSuperblock,
    ) -> Result<Option<u64>, FilesystemError> {
        let sectsize = sb.sectsize as u64;
        let agblocks = sb.agblocks as u64;
        let blocksize = sb.blocksize as u64;
        let bs = sb.blocksize as usize;
        let ino_shift = sb.agblklog + sb.inopblog;

        for agno in 0..sb.agcount as u64 {
            let agi_byte = self.partition_offset + agno * agblocks * blocksize + 2 * sectsize;
            let mut agi_sec = vec![0u8; sectsize as usize];
            if read_at_aligned(&mut self.reader, agi_byte, sectsize, &mut agi_sec).is_err() {
                continue;
            }
            let agi = match XfsAgi::parse(&agi_sec) {
                Ok(a) => a,
                Err(_) => continue,
            };
            if agi.freecount == 0 {
                continue;
            }
            let leaves = match self.collect_inobt_leaf_blocks(sb, agno, agi.root) {
                Ok(l) => l,
                Err(_) => continue,
            };
            let hdr_len = sb.sblock_hdr_len();
            for leaf_agbno in leaves {
                let fsblock = (agno << sb.agblklog) | leaf_agbno as u64;
                let mut block = vec![0u8; bs];
                if self.read_fsblock(fsblock, &mut block).is_err() {
                    continue;
                }
                let numrecs = BigEndian::read_u16(&block[6..8]) as usize;
                let max_leaf = (bs - hdr_len) / INOBT_REC_SIZE;
                if numrecs > max_leaf {
                    continue;
                }
                for r in 0..numrecs {
                    let off = hdr_len + r * INOBT_REC_SIZE;
                    let start_agino = BigEndian::read_u32(&block[off..off + 4]) as u64;
                    let freecount = BigEndian::read_u32(&block[off + 4..off + 8]);
                    let free = BigEndian::read_u64(&block[off + 8..off + 16]);
                    if freecount == 0 || free == 0 {
                        continue;
                    }
                    let slot = free.trailing_zeros() as u64; // lowest free slot
                    let ino = (agno << ino_shift) | (start_agino + slot);

                    // Mark allocated: clear the bit, decrement record freecount.
                    let new_free = free & !(1u64 << slot);
                    BigEndian::write_u32(&mut block[off + 4..off + 8], freecount - 1);
                    BigEndian::write_u64(&mut block[off + 8..off + 16], new_free);
                    // v5: re-stamp the leaf's sblock-crc tuple before writing.
                    if sb.is_v5() {
                        stamp_v5_sblock_for_ag(&mut block, sb, agno, leaf_agbno);
                    }
                    self.write_fsblock(sb, fsblock, &block)?;

                    // AGI freecount-- and sb_ifree--.
                    BigEndian::write_u32(
                        &mut agi_sec[AGI_FREECOUNT_OFF..AGI_FREECOUNT_OFF + 4],
                        agi.freecount - 1,
                    );
                    self.write_agi_sector(sb, agi_byte, &mut agi_sec)?;
                    self.bump_sb_ifree(sb, -1)?;
                    return Ok(Some(ino));
                }
            }
        }
        Ok(None)
    }

    /// Extend an AG's inobt with a brand-new 64-inode chunk. Carves
    /// `blocks_per_chunk` aligned contiguous blocks via
    /// [`alloc_blocks_aligned`], writes 64 free dinodes there, then splices a
    /// fresh inobt record (all 64 bits set ⇒ all free) into the AG's inobt.
    /// Updates AGI `count`/`freecount` and superblock `sb_icount`/`sb_ifree`
    /// by `+64` each — the caller's slot claim later rebalances `freecount`/
    /// `sb_ifree` by `-1`.
    ///
    /// Two record-splice strategies, picked by the existing tree's shape:
    ///   * **Single-leaf splice** ([`splice_inobt_single_leaf_record`]) — when
    ///     `agi.level == 1` and the root leaf has at least one open slot, the
    ///     new record drops in alongside the existing ones with no allocation
    ///     and no AGI root/level change.
    ///   * **Multi-level grow** ([`grow_inobt_with_new_record`]) — when the
    ///     existing tree is already multi-level OR its only leaf is full, gather
    ///     every existing record + the new one, allocate fresh contiguous tree
    ///     blocks (and reclaim the old tree blocks in the same freespace
    ///     rebuild), then rebuild a balanced tree via [`build_sblock_btree`]
    ///     and relocate the AGI root + level.
    ///
    /// v4 only.
    pub(crate) fn alloc_new_inode_chunk(
        &mut self,
        sb: &super::sb::XfsSuperblock,
    ) -> Result<(), FilesystemError> {
        // §2.1 (E.5b): v5 lifted. `init_free_inode_chunk` stamps each v3
        // inode core (E.2), `alloc_blocks_aligned` rebuilds the host AG's
        // freespace btrees through the v5-aware `build_alloc_btree` (E.4),
        // and the splice / grow paths both stamp the AGI sector + the
        // inobt leaf or fresh tree blocks on v5 (this slice + E.4).
        let bs = sb.blocksize as u64;
        let bs_usize = bs as usize;
        let inodesize = sb.inodesize as u64;
        let blocks_per_chunk = ((XFS_INODES_PER_CHUNK as u64) * inodesize).div_ceil(bs) as u32;
        // sb_inoalignmt is the on-disk chunk alignment; fall back to the
        // chunk's own block count when it's unset (older v4 images without
        // the ALIGN feature). The fallback gives natural chunk-stride
        // alignment, which is the minimum every walker assumes.
        let alignment = if sb.inoalignmt > 0 {
            sb.inoalignmt
        } else {
            blocks_per_chunk
        };
        let max_leaf = (bs_usize - sb.sblock_hdr_len()) / INOBT_REC_SIZE;

        // di_version: every inode in a v4 volume shares the version byte the
        // root inode carries. Reading it once gives the right value for both
        // v1 (16-bit nlink, di_onlink) and v2 (32-bit nlink, di_nlink) inode
        // formats without having to read sb_features2 directly.
        let (_root_core, root_buf) = self.read_inode_buf(sb.rootino)?;
        let di_version = root_buf[4];

        // Carve the chunk's contiguous aligned blocks. alloc_blocks_aligned
        // also rebuilds the host AG's freespace btrees + resyncs sb_fdblocks,
        // so on success the volume's free-space accounting is already correct
        // for the consumed run.
        let chunk_fsblock = self.alloc_blocks_aligned(sb, blocks_per_chunk, alignment)?;
        let agno = chunk_fsblock >> sb.agblklog;
        let chunk_agbno = (chunk_fsblock & ((1u64 << sb.agblklog) - 1)) as u32;
        let start_agino = ((chunk_agbno as u64) << sb.inopblog) as u32;

        // Initialize the chunk: 64 free dinodes (magic + version +
        // di_next_unlinked = NULLAGINO, mode 0 == free).
        self.init_free_inode_chunk(sb, chunk_fsblock, di_version, blocks_per_chunk as usize)?;

        // Re-read the AGI of the chosen AG so we can pick a splice strategy.
        let sectsize = sb.sectsize as u64;
        let agblocks = sb.agblocks as u64;
        let agi_byte = self.partition_offset + agno * agblocks * bs + 2 * sectsize;
        let mut agi_sec = vec![0u8; sectsize as usize];
        read_at_aligned(&mut self.reader, agi_byte, sectsize, &mut agi_sec)?;
        let agi = XfsAgi::parse(&agi_sec)?;

        // Pick the splice strategy. Single-leaf splice when (a) the tree is
        // single-level, (b) its only leaf is the AGI root, and (c) the leaf
        // has at least one open slot. Anything else → multi-level grow.
        let single_leaf_fits = if agi.level == 1 {
            let leaves = self.collect_inobt_leaf_blocks(sb, agno, agi.root)?;
            if leaves.as_slice() == [agi.root] {
                let leaf_fsblock = (agno << sb.agblklog) | agi.root as u64;
                let mut leaf = vec![0u8; bs_usize];
                self.read_fsblock(leaf_fsblock, &mut leaf)?;
                let numrecs = BigEndian::read_u16(&leaf[6..8]) as usize;
                numrecs < max_leaf
            } else {
                false
            }
        } else {
            false
        };

        if single_leaf_fits {
            self.splice_inobt_single_leaf_record(sb, agno, agi.root, start_agino, max_leaf)?;
        } else {
            self.grow_inobt_with_new_record(sb, agno, agi.root, start_agino)?;
            // grow_inobt_with_new_record rewrote AGI root + level; refresh.
            read_at_aligned(&mut self.reader, agi_byte, sectsize, &mut agi_sec)?;
        }

        // AGI count += 64, freecount += 64. The single-leaf path leaves AGI
        // root/level untouched; the multi-level grow already wrote new values
        // but left count/freecount alone, so this final step is unconditional.
        let new_count = agi
            .count
            .checked_add(XFS_INODES_PER_CHUNK as u32)
            .ok_or_else(|| FilesystemError::Parse("AGI count overflow".into()))?;
        let new_freecount = agi
            .freecount
            .checked_add(XFS_INODES_PER_CHUNK as u32)
            .ok_or_else(|| FilesystemError::Parse("AGI freecount overflow".into()))?;
        BigEndian::write_u32(&mut agi_sec[AGI_COUNT_OFF..AGI_COUNT_OFF + 4], new_count);
        BigEndian::write_u32(
            &mut agi_sec[AGI_FREECOUNT_OFF..AGI_FREECOUNT_OFF + 4],
            new_freecount,
        );
        self.write_agi_sector(sb, agi_byte, &mut agi_sec)?;

        // sb_icount += 64, sb_ifree += 64.
        self.bump_sb_inode_counters(sb, XFS_INODES_PER_CHUNK as i64, XFS_INODES_PER_CHUNK as i64)
    }

    /// Splice a fresh all-free chunk record `(start_agino, freecount=64,
    /// free=u64::MAX)` into the existing single-leaf inobt at `root_agbno`.
    /// Reads the leaf, appends the record, sorts by `start_agino`, and rewrites
    /// the leaf in place. Bumps only the leaf's `numrecs`; the leaf header
    /// (level, sibling pointers) is left untouched. Trailing-byte zero fill
    /// keeps a future bump from reading stale data.
    fn splice_inobt_single_leaf_record(
        &mut self,
        sb: &super::sb::XfsSuperblock,
        agno: u64,
        root_agbno: u32,
        new_start_agino: u32,
        max_leaf: usize,
    ) -> Result<(), FilesystemError> {
        let bs_usize = sb.blocksize as usize;
        let hdr_len = sb.sblock_hdr_len();
        let leaf_fsblock = (agno << sb.agblklog) | root_agbno as u64;
        let mut leaf = vec![0u8; bs_usize];
        self.read_fsblock(leaf_fsblock, &mut leaf)?;
        let numrecs = BigEndian::read_u16(&leaf[6..8]) as usize;

        let mut records: Vec<(u32, u32, u64)> = Vec::with_capacity(numrecs + 1);
        for r in 0..numrecs {
            let off = hdr_len + r * INOBT_REC_SIZE;
            let s = BigEndian::read_u32(&leaf[off..off + 4]);
            let fc = BigEndian::read_u32(&leaf[off + 4..off + 8]);
            let free = BigEndian::read_u64(&leaf[off + 8..off + 16]);
            if s == new_start_agino {
                return Err(FilesystemError::Parse(format!(
                    "XFS new-chunk allocation: AG {agno} already has chunk at \
                     start_agino {new_start_agino}"
                )));
            }
            records.push((s, fc, free));
        }
        records.push((new_start_agino, XFS_INODES_PER_CHUNK as u32, u64::MAX));
        records.sort_by_key(|&(s, _, _)| s);

        BigEndian::write_u16(&mut leaf[6..8], (numrecs + 1) as u16);
        for (i, &(s, fc, free)) in records.iter().enumerate() {
            let off = hdr_len + i * INOBT_REC_SIZE;
            BigEndian::write_u32(&mut leaf[off..off + 4], s);
            BigEndian::write_u32(&mut leaf[off + 4..off + 8], fc);
            BigEndian::write_u64(&mut leaf[off + 8..off + 16], free);
        }
        let used = hdr_len + records.len() * INOBT_REC_SIZE;
        let stale_end = hdr_len + (numrecs + 1).min(max_leaf) * INOBT_REC_SIZE;
        for b in &mut leaf[used..stale_end.max(used)] {
            *b = 0;
        }
        // v5 leaf carries an sblock-crc tuple — restamp after the record
        // mutation so the on-disk CRC matches the rewritten payload.
        if sb.is_v5() {
            stamp_v5_sblock_for_ag(&mut leaf, sb, agno, root_agbno);
        }
        self.write_fsblock(sb, leaf_fsblock, &leaf)?;
        Ok(())
    }

    /// Multi-level inobt growth: the existing tree can't take one more record
    /// without restructuring (root leaf full, or already multi-level). Gather
    /// every existing record, add the new all-free chunk, allocate fresh
    /// contiguous tree blocks (reclaiming the old tree blocks in the same
    /// freespace rebuild via [`swap_inobt_blocks_in_ag`]), build a balanced
    /// tree with [`build_sblock_btree`], write its blocks, and relocate the
    /// AGI root + level. AGI count/freecount are not touched here — the caller
    /// applies a single +64 delta after dispatch.
    fn grow_inobt_with_new_record(
        &mut self,
        sb: &super::sb::XfsSuperblock,
        agno: u64,
        old_root: u32,
        new_start_agino: u32,
    ) -> Result<(), FilesystemError> {
        let bs_usize = sb.blocksize as usize;
        let sectsize = sb.sectsize as u64;
        let agblocks = sb.agblocks as u64;
        let bs = sb.blocksize as u64;

        // (1) Gather every existing record from every leaf, refuse a duplicate
        // start_agino, then add the new all-free record and key-sort.
        let old_leaves = self.collect_inobt_leaf_blocks(sb, agno, old_root)?;
        let read_hdr_len = sb.sblock_hdr_len();
        let mut records: Vec<(u32, u32, u64)> = Vec::new();
        for &leaf_agbno in &old_leaves {
            let leaf_fsblock = (agno << sb.agblklog) | leaf_agbno as u64;
            let mut leaf = vec![0u8; bs_usize];
            self.read_fsblock(leaf_fsblock, &mut leaf)?;
            let nr = BigEndian::read_u16(&leaf[6..8]) as usize;
            for r in 0..nr {
                let off = read_hdr_len + r * INOBT_REC_SIZE;
                let s = BigEndian::read_u32(&leaf[off..off + 4]);
                let fc = BigEndian::read_u32(&leaf[off + 4..off + 8]);
                let free = BigEndian::read_u64(&leaf[off + 8..off + 16]);
                if s == new_start_agino {
                    return Err(FilesystemError::Parse(format!(
                        "XFS new-chunk allocation: AG {agno} already has chunk at \
                         start_agino {new_start_agino}"
                    )));
                }
                records.push((s, fc, free));
            }
        }
        records.push((new_start_agino, XFS_INODES_PER_CHUNK as u32, u64::MAX));
        records.sort_by_key(|&(s, _, _)| s);

        // (2) Pack the records into the flat byte buffer build_sblock_btree
        // wants (each entry: startino(4) freecount(4) free(8)).
        let mut packed = vec![0u8; records.len() * INOBT_REC_SIZE];
        for (i, &(s, fc, free)) in records.iter().enumerate() {
            let off = i * INOBT_REC_SIZE;
            BigEndian::write_u32(&mut packed[off..off + 4], s);
            BigEndian::write_u32(&mut packed[off + 4..off + 8], fc);
            BigEndian::write_u64(&mut packed[off + 8..off + 16], free);
        }

        // (3) Enumerate every block the OLD tree occupies (leaves + internal
        // nodes) so the freespace swap can reclaim them.
        let old_blocks = self.collect_inobt_all_blocks(sb, agno, old_root)?;

        // (4) Reserve `need` contiguous AG-local blocks for the new tree AND
        // free the old tree blocks in one freespace rebuild — the only way to
        // avoid stranding either side when the AG's freespace is tight.
        let is_v5 = sb.is_v5();
        let need = blocks_needed_for(
            records.len(),
            bs_usize,
            INOBT_REC_SIZE,
            INOBT_KEY_SIZE,
            is_v5,
        );
        let new_tree_start_agbno =
            self.swap_inobt_blocks_in_ag(sb, agno, &old_blocks, need as u32)?;

        // (5) Build the new tree on the carved block run. Magic + sb-stamp
        // arg dispatch on `is_v5`: v4 emits `XFS_IBT_MAGIC` + no CRC; v5
        // emits `XFS_IBT_CRC_MAGIC` + the standard CRC tuple via
        // `build_sblock_btree`'s `Some(sb)` path.
        let carved_agbnos: Vec<u32> = (0..need as u32).map(|i| new_tree_start_agbno + i).collect();
        let inobt_magic = if is_v5 {
            XFS_IBT_CRC_MAGIC
        } else {
            XFS_IBT_MAGIC
        };
        let sb_for_v5: Option<&super::sb::XfsSuperblock> = if is_v5 { Some(sb) } else { None };
        let tree = build_sblock_btree(
            &packed,
            INOBT_REC_SIZE,
            INOBT_KEY_SIZE,
            inobt_magic,
            bs_usize,
            agno as u32,
            &carved_agbnos,
            sb_for_v5,
        );

        // (6) Write every new tree block. Until the AGI root/level update at
        // step (7), the AGI still points at the old (now-freed) root — readers
        // would see garbage if the freed blocks were reused. Within this method
        // no concurrent access happens (we hold &mut self), and we don't
        // return until AGI is updated, so the on-disk window is closed.
        for blk in &tree.blocks {
            let fsblock = (agno << sb.agblklog) | blk.agbno as u64;
            self.write_fsblock(sb, fsblock, &blk.bytes)?;
        }

        // (7) Rewrite AGI root + level. Count/freecount are bumped by the
        // caller after dispatch, so we leave them alone here.
        let agi_byte = self.partition_offset + agno * agblocks * bs + 2 * sectsize;
        let mut agi_sec = vec![0u8; sectsize as usize];
        read_at_aligned(&mut self.reader, agi_byte, sectsize, &mut agi_sec)?;
        BigEndian::write_u32(
            &mut agi_sec[AGI_ROOT_OFF..AGI_ROOT_OFF + 4],
            tree.root_agbno,
        );
        BigEndian::write_u32(&mut agi_sec[AGI_LEVEL_OFF..AGI_LEVEL_OFF + 4], tree.levels);
        self.write_agi_sector(sb, agi_byte, &mut agi_sec)?;
        Ok(())
    }

    /// Reserve `n` contiguous AG-local blocks for the new inobt and return the
    /// freed `old_blocks` to the free set, all in one freespace rebuild. Adds
    /// the freed blocks to this AG's current full-free set, coalesces, carves
    /// `n` blocks from the largest extent of the merged set, then rebuilds
    /// bno/cnt + resyncs `sb_fdblocks`. Returns the AG-relative starting block
    /// of the carved run.
    ///
    /// Both alloc and free are AG-scoped: the inobt belongs to one specific AG
    /// (the AGI of that AG points at its root), so every new tree block has to
    /// live in that AG, and every reclaimed old block does too. A pure
    /// `alloc_blocks` would try other AGs on `DiskFull`; this stays put.
    fn swap_inobt_blocks_in_ag(
        &mut self,
        sb: &super::sb::XfsSuperblock,
        agno: u64,
        old_blocks: &[u32],
        n: u32,
    ) -> Result<u32, FilesystemError> {
        let agblocks = sb.agblocks as u64;
        let expected_len = if agno == sb.agcount as u64 - 1 {
            sb.dblocks - agno * agblocks
        } else {
            agblocks
        };
        let mut full_free = self.current_full_free(sb, agno, expected_len)?;
        // Add the old tree blocks back to the free set as single-block extents;
        // coalesce will merge them with neighbouring runs.
        for &agbno in old_blocks {
            full_free.push(FreeExtent {
                startblock: agbno,
                blockcount: 1,
            });
        }
        let merged = coalesce(full_free);
        let (carved, free_after) = carve_from_largest(&merged, n).ok_or_else(|| {
            FilesystemError::DiskFull(format!(
                "AG {agno}: no free extent large enough for {n}-block inobt growth"
            ))
        })?;
        self.rebuild_ag_freespace(sb, agno, expected_len, &free_after)?;
        self.resync_sb_fdblocks(sb)?;
        Ok(carved[0])
    }

    /// Carve `n` contiguous blocks whose start agbno is a multiple of
    /// `alignment`, returning the starting fsblock. Walks every AG, picks the
    /// first whose largest free extent admits an aligned `n`-block run, then
    /// rebuilds that AG's bno/cnt over the remainder and resyncs
    /// `sb_fdblocks` — same shape as [`alloc_blocks`].
    fn alloc_blocks_aligned(
        &mut self,
        sb: &super::sb::XfsSuperblock,
        n: u32,
        alignment: u32,
    ) -> Result<u64, FilesystemError> {
        if n == 0 || alignment == 0 {
            return Err(FilesystemError::InvalidData(
                "alloc_blocks_aligned: zero n or alignment".into(),
            ));
        }
        let agblocks = sb.agblocks as u64;
        for agno in 0..sb.agcount as u64 {
            let expected_len = if agno == sb.agcount as u64 - 1 {
                sb.dblocks - agno * agblocks
            } else {
                agblocks
            };
            let full_free = match self.current_full_free(sb, agno, expected_len) {
                Ok(f) => f,
                Err(_) => continue,
            };
            let Some((carved_start, free_after)) =
                carve_aligned_from_largest(&full_free, n, alignment)
            else {
                continue;
            };
            if self
                .rebuild_ag_freespace(sb, agno, expected_len, &free_after)
                .is_err()
            {
                continue;
            }
            self.resync_sb_fdblocks(sb)?;
            return Ok((agno << sb.agblklog) | carved_start as u64);
        }
        Err(FilesystemError::DiskFull(format!(
            "no allocation group has a free extent admitting a {n}-block run \
             aligned to {alignment}"
        )))
    }

    /// Initialize a fresh inode chunk: write 64 free dinodes at the chunk's
    /// starting fsblock. Each dinode carries `XFS_DINODE_MAGIC`, the volume's
    /// `di_version` (matching every other inode), `di_mode = 0` (the on-disk
    /// "free slot" marker), and `di_next_unlinked = NULLAGINO`. All other
    /// fields are zero — matching what `xfs_ialloc_inode_init` writes.
    ///
    /// **v5 (CRC) volumes**: each slot's v3 core also needs its CRC-header
    /// tuple stamped (`di_uuid` / `di_ino` / `di_lsn` / `di_crc`) so the
    /// inobt-walk readers and `xfs_repair` accept the chunk as a fresh
    /// 64-slot extension. The kernel's `xfs_ialloc_inode_init` does the
    /// equivalent stamp via `xfs_dinode_calc_crc` after writing the body;
    /// we do it inline. v4 callers skip the stamp (no `di_crc` field).
    fn init_free_inode_chunk(
        &mut self,
        sb: &super::sb::XfsSuperblock,
        chunk_fsblock: u64,
        di_version: u8,
        blocks_per_chunk: usize,
    ) -> Result<(), FilesystemError> {
        let inodesize = sb.inodesize as usize;
        let bs = sb.blocksize as usize;
        let span = blocks_per_chunk * bs;
        let mut buf = vec![0u8; span];
        // chunk_fsblock's starting agino = the first inode number in this chunk.
        // Each slot's di_ino = start_agino + slot, embedded as a full inumber.
        let agno = chunk_fsblock >> sb.agblklog;
        let chunk_agbno = chunk_fsblock & ((1u64 << sb.agblklog) - 1);
        let start_agino = chunk_agbno << sb.inopblog;
        let ino_shift = sb.agblklog + sb.inopblog;
        let start_ino = (agno << ino_shift) | start_agino;

        for slot in 0..XFS_INODES_PER_CHUNK {
            let base = slot * inodesize;
            BigEndian::write_u16(&mut buf[base..base + 2], XFS_DINODE_MAGIC);
            // di_mode (offset 2..4) and everything else stays zero.
            buf[base + 4] = di_version;
            BigEndian::write_u32(
                &mut buf[base + DI_NEXT_UNLINKED_OFF..base + DI_NEXT_UNLINKED_OFF + 4],
                NULLAGINO,
            );
            if sb.is_v5() {
                let slot_buf = &mut buf[base..base + inodesize];
                super::v5_crc::stamp_inode_v3(slot_buf, start_ino + slot as u64, sb);
            }
        }
        let part_byte =
            fsblock_to_partition_byte(chunk_fsblock, sb.agblocks, sb.agblklog, sb.blocksize);
        self.write_at(self.partition_offset + part_byte, &buf)
    }

    /// Bump `sb_icount` and `sb_ifree` by the given deltas in the primary
    /// superblock (a single read+write). Either delta of 0 is a skip.
    fn bump_sb_inode_counters(
        &mut self,
        sb: &super::sb::XfsSuperblock,
        icount_delta: i64,
        ifree_delta: i64,
    ) -> Result<(), FilesystemError> {
        if icount_delta == 0 && ifree_delta == 0 {
            return Ok(());
        }
        let sectsize = sb.sectsize as u64;
        let mut primary = vec![0u8; sectsize as usize];
        read_at_aligned(
            &mut self.reader,
            self.partition_offset,
            sectsize,
            &mut primary,
        )?;
        if icount_delta != 0 {
            let cur = BigEndian::read_u64(&primary[SB_ICOUNT_OFF..SB_ICOUNT_OFF + 8]);
            let new = (cur as i64 + icount_delta).max(0) as u64;
            BigEndian::write_u64(&mut primary[SB_ICOUNT_OFF..SB_ICOUNT_OFF + 8], new);
        }
        if ifree_delta != 0 {
            let cur = BigEndian::read_u64(&primary[SB_IFREE_OFF..SB_IFREE_OFF + 8]);
            let new = (cur as i64 + ifree_delta).max(0) as u64;
            BigEndian::write_u64(&mut primary[SB_IFREE_OFF..SB_IFREE_OFF + 8], new);
        }
        self.write_sb_primary(sb, &mut primary)
    }

    /// Initialize a freshly-allocated inode as an empty short-form directory
    /// whose parent is `parent_ino`. Preserves the on-disk inode version and
    /// bumps the generation; writes a clean v4 dinode core + the 6-byte inline
    /// fork (`count=0, i8count=0, parent`). `nlink` is 2 (the new directory's
    /// own `.` plus its entry in the parent); no subdirectories yet.
    pub(crate) fn init_empty_shortform_dir(
        &mut self,
        sb: &super::sb::XfsSuperblock,
        ino: u64,
        parent_ino: u64,
        mode: u16,
        uid: u32,
        gid: u32,
    ) -> Result<(), FilesystemError> {
        if parent_ino > u64::from(u32::MAX) || ino > u64::from(u32::MAX) {
            // Inode numbers beyond 32 bits need the 8-byte short-form layout
            // (i8count); our small-volume target never hits this.
            return Err(FilesystemError::Unsupported(
                "XFS short-form dir with 8-byte inode numbers not supported".into(),
            ));
        }
        let (_core, mut buf) = self.read_inode_buf(ino)?;
        let version = buf[4];
        let gen = BigEndian::read_u32(&buf[92..96]).wrapping_add(1);
        let isz = buf.len();

        // Clean v4 dinode core.
        BigEndian::write_u16(&mut buf[0..2], super::types::XFS_DINODE_MAGIC);
        BigEndian::write_u16(&mut buf[2..4], mode);
        buf[4] = version;
        buf[5] = 1; // di_format = local
                    // di_onlink (v1 nlink) / di_nlink (v2 nlink).
        if version == 1 {
            BigEndian::write_u16(&mut buf[6..8], 2);
            BigEndian::write_u32(&mut buf[16..20], 0);
        } else {
            BigEndian::write_u16(&mut buf[6..8], 0);
            BigEndian::write_u32(&mut buf[16..20], 2);
        }
        BigEndian::write_u32(&mut buf[8..12], uid);
        BigEndian::write_u32(&mut buf[12..16], gid);
        for b in buf.iter_mut().take(56).skip(20) {
            *b = 0; // projid, pad, atime, mtime, ctime
        }
        BigEndian::write_u64(&mut buf[56..64], 6); // di_size = empty sf fork len
        for b in buf.iter_mut().take(92).skip(64) {
            *b = 0; // nblocks, extsize, nextents, anextents, forkoff, aformat, dm*, flags
        }
        buf[83] = 2; // di_aformat = extents (no attr fork)
        BigEndian::write_u32(&mut buf[92..96], gen);
        BigEndian::write_u32(&mut buf[96..100], NULLAGINO); // di_next_unlinked

        // Inline fork: count=0, i8count=0, parent (4 bytes).
        let fs = sb.fork_offset();
        for b in buf.iter_mut().take(isz).skip(fs) {
            *b = 0;
        }
        buf[fs] = 0; // count
        buf[fs + 1] = 0; // i8count
        BigEndian::write_u32(&mut buf[fs + 2..fs + 6], parent_ino as u32);

        self.write_inode_region(sb, ino, &buf)
    }

    /// Append an entry `(name -> child_ino)` to short-form directory
    /// `parent_ino`, fixing `di_size`/count and bumping the parent's link count
    /// when `is_dir`. Errors with `DiskFull` when the entry won't fit the inline
    /// fork (block-form conversion is deferred) or `AlreadyExists` on a dup name.
    pub(crate) fn sf_insert_entry(
        &mut self,
        sb: &super::sb::XfsSuperblock,
        parent_ino: u64,
        name: &str,
        child_ino: u64,
        is_dir: bool,
    ) -> Result<(), FilesystemError> {
        let has_ftype = sb.has_ftype();
        let (core, mut buf) = self.read_inode_buf(parent_ino)?;
        if !core.is_dir() || core.format != super::types::DiFormat::Local {
            return Err(FilesystemError::Unsupported(
                "parent is not a short-form directory".into(),
            ));
        }
        if child_ino > u64::from(u32::MAX) {
            return Err(FilesystemError::Unsupported(
                "XFS short-form dir with 8-byte inode numbers not supported".into(),
            ));
        }
        let namelen = name.len();
        if namelen == 0 || namelen > 255 {
            return Err(FilesystemError::InvalidData("bad entry name length".into()));
        }

        let fs = sb.fork_offset();
        let fork_len = core.size as usize;
        let fork_end = fs + fork_len;
        if fork_end > buf.len() {
            return Err(FilesystemError::Parse(
                "short-form fork overflows inode".into(),
            ));
        }

        // Reject a duplicate name, then pick the append offset cookie from the
        // raw fork (so removal gaps are respected).
        let (_parent, entries) = super::dir2::parse_shortform(&buf[fs..fork_end], has_ftype)?;
        if entries.iter().any(|e| e.name == name) {
            return Err(FilesystemError::AlreadyExists(name.to_string()));
        }
        let new_off = self.sf_next_offset(&buf[fs..fork_end], has_ftype)?;

        // Build the new entry: namelen(1) offset(2) name[namelen] [ftype] ino(4).
        let ino_width = 4usize; // i8count == 0 path only
        let entry_len = 1 + 2 + namelen + usize::from(has_ftype) + ino_width;
        let new_fork_len = fork_len + entry_len;

        // Fit check against the literal area (bounded by an attr fork if any).
        let avail_end = if core.forkoff > 0 {
            fs + (core.forkoff as usize) * 8
        } else {
            buf.len()
        };
        if fs + new_fork_len > avail_end {
            return Err(FilesystemError::DiskFull(
                "short-form directory full (block-form conversion not implemented)".into(),
            ));
        }

        // Append the entry bytes after the existing fork, bump the count.
        let mut p = fork_end;
        buf[p] = namelen as u8;
        BigEndian::write_u16(&mut buf[p + 1..p + 3], new_off as u16);
        p += 3;
        buf[p..p + namelen].copy_from_slice(name.as_bytes());
        p += namelen;
        if has_ftype {
            buf[p] = if is_dir {
                XFS_DIR3_FT_DIR
            } else {
                XFS_DIR3_FT_REG_FILE
            };
            p += 1;
        }
        BigEndian::write_u32(&mut buf[p..p + ino_width], child_ino as u32);

        // Header count++ and di_size.
        buf[fs] = buf[fs]
            .checked_add(1)
            .ok_or_else(|| FilesystemError::DiskFull("short-form entry count overflow".into()))?;
        BigEndian::write_u64(&mut buf[56..64], new_fork_len as u64);

        // Parent link count: a new subdirectory adds a ".." back-link.
        if is_dir {
            let version = buf[4];
            if version == 1 {
                let n = BigEndian::read_u16(&buf[6..8]).wrapping_add(1);
                BigEndian::write_u16(&mut buf[6..8], n);
            } else {
                let n = BigEndian::read_u32(&buf[16..20]).wrapping_add(1);
                BigEndian::write_u32(&mut buf[16..20], n);
            }
        }

        self.write_inode_region(sb, parent_ino, &buf)
    }

    /// The offset cookie to assign a newly-appended short-form entry: the
    /// maximum of (each existing entry's cookie + its data-block size) and the
    /// post-`.`/`..` base. Walks the raw fork so gaps left by removals are
    /// respected.
    fn sf_next_offset(&self, fork: &[u8], has_ftype: bool) -> Result<u32, FilesystemError> {
        let mut next = (XFS_DIR2_DATA_HDR_LEN
            + data_entsize(1, has_ftype)
            + data_entsize(2, has_ftype)) as u32;
        if fork.len() < 6 {
            return Err(FilesystemError::Parse("short-form fork too small".into()));
        }
        let count = fork[0] as usize;
        let i8count = fork[1];
        let ino_width = if i8count > 0 { 8 } else { 4 };
        let mut pos = 2 + ino_width;
        for _ in 0..count {
            if pos + 3 > fork.len() {
                return Err(FilesystemError::Parse("short-form entry truncated".into()));
            }
            let namelen = fork[pos] as usize;
            let off = BigEndian::read_u16(&fork[pos + 1..pos + 3]) as u32;
            let end = off + data_entsize(namelen, has_ftype) as u32;
            next = next.max(end);
            pos += 3 + namelen + usize::from(has_ftype) + ino_width;
        }
        Ok(next)
    }

    /// Write a buffer at an absolute partition byte offset, sector-aligned.
    fn write_at(&mut self, byte_off: u64, data: &[u8]) -> Result<(), FilesystemError> {
        use std::io::SeekFrom;
        self.reader.seek(SeekFrom::Start(byte_off))?;
        self.reader.write_all(data)?;
        Ok(())
    }

    /// Stamp v5 AGI CRC tuple (uuid/lsn/crc) and write the sector. v4 just
    /// writes through. Every AGI mutation funnels through here so the CRC
    /// stays valid on v5 volumes.
    pub(crate) fn write_agi_sector(
        &mut self,
        sb: &super::sb::XfsSuperblock,
        byte_off: u64,
        sec: &mut [u8],
    ) -> Result<(), FilesystemError> {
        if sb.is_v5() {
            super::v5_crc::stamp_agi(sec, sb);
        }
        self.write_at(byte_off, sec)
    }

    /// Stamp v5 AGF CRC tuple (uuid/lsn/crc) and write the sector. v4 just
    /// writes through.
    pub(crate) fn write_agf_sector(
        &mut self,
        sb: &super::sb::XfsSuperblock,
        byte_off: u64,
        sec: &mut [u8],
    ) -> Result<(), FilesystemError> {
        if sb.is_v5() {
            super::v5_crc::stamp_agf(sec, sb);
        }
        self.write_at(byte_off, sec)
    }

    /// Stamp v5 AGFL CRC tuple (uuid/lsn/crc) and write the sector. v4 just
    /// writes through. No current edit path mutates the AGFL contents, so
    /// this helper isn't called yet — kept here so when E.5b's
    /// `alloc_blocks_aligned` path needs to touch the AGFL (e.g. when an
    /// allocation drains the AGFL reserve and a refill becomes necessary)
    /// the stamping site is already wired.
    #[allow(dead_code)]
    pub(crate) fn write_agfl_sector(
        &mut self,
        sb: &super::sb::XfsSuperblock,
        byte_off: u64,
        sec: &mut [u8],
    ) -> Result<(), FilesystemError> {
        if sb.is_v5() {
            super::v5_crc::stamp_agfl(sec, sb);
        }
        self.write_at(byte_off, sec)
    }

    /// Stamp v5 SB CRC and write the primary superblock sector. v4 just
    /// writes through.
    pub(crate) fn write_sb_primary(
        &mut self,
        sb: &super::sb::XfsSuperblock,
        sec: &mut [u8],
    ) -> Result<(), FilesystemError> {
        if sb.is_v5() {
            super::v5_crc::stamp_superblock(sec);
        }
        self.write_at(self.partition_offset, sec)
    }

    /// Adjust `sb_ifree` by `delta` in the primary superblock.
    fn bump_sb_ifree(
        &mut self,
        sb: &super::sb::XfsSuperblock,
        delta: i64,
    ) -> Result<(), FilesystemError> {
        let sectsize = sb.sectsize as u64;
        let mut primary = vec![0u8; sectsize as usize];
        read_at_aligned(
            &mut self.reader,
            self.partition_offset,
            sectsize,
            &mut primary,
        )?;
        let cur = BigEndian::read_u64(&primary[SB_IFREE_OFF..SB_IFREE_OFF + 8]);
        let new = (cur as i64 + delta).max(0) as u64;
        BigEndian::write_u64(&mut primary[SB_IFREE_OFF..SB_IFREE_OFF + 8], new);
        self.write_sb_primary(sb, &mut primary)
    }

    /// Internal entry point for `create_directory`: allocate an inode, init it
    /// as an empty short-form directory, and insert it into `parent_ino`.
    /// Returns the new directory's inode number. The fit check runs before any
    /// write, so a `DiskFull` parent never leaves a dangling allocated inode.
    pub(crate) fn do_create_directory(
        &mut self,
        parent_ino: u64,
        name: &str,
        mode: u16,
        uid: u32,
        gid: u32,
    ) -> Result<u64, FilesystemError> {
        let sb = self.superblock().clone();

        // Pre-flight (no writes): unique name + the parent can take the entry
        // (short-form, or short-form converting to a single block, or an
        // existing single-block dir) — so a failure leaves the volume unchanged.
        self.dir_can_insert(&sb, parent_ino, name, true)?;

        let ino = self.alloc_inode_slot(&sb)?;
        self.init_empty_shortform_dir(&sb, ino, parent_ino, mode, uid, gid)?;
        self.dir_insert_entry(&sb, parent_ino, name, ino, true)?;
        self.reader.flush()?;
        Ok(ino)
    }

    // ----- block allocation + create_file -----------------------------------

    /// Allocate `n` **contiguous** free blocks, returning their starting fsblock.
    /// Carves the run off the tail of an AG's largest free extent and rebuilds
    /// that AG's free-space btrees over the remainder (reusing the R2 rebuild),
    /// then resyncs `sb_fdblocks`. Errors `DiskFull` when no AG has a single
    /// free extent large enough (no multi-extent split yet). v4 only.
    pub(crate) fn alloc_blocks(
        &mut self,
        sb: &super::sb::XfsSuperblock,
        n: u32,
    ) -> Result<u64, FilesystemError> {
        if n == 0 {
            return Err(FilesystemError::InvalidData("alloc_blocks(0)".into()));
        }
        let agblocks = sb.agblocks as u64;
        for agno in 0..sb.agcount as u64 {
            let expected_len = if agno == sb.agcount as u64 - 1 {
                sb.dblocks - agno * agblocks
            } else {
                agblocks
            };
            let full_free = match self.current_full_free(sb, agno, expected_len) {
                Ok(f) => f,
                Err(_) => continue,
            };
            // Carve the data run off the largest extent's tail (contiguous,
            // extent-count preserving). Leaves room for the btree rebuild's own
            // carve from the remainder.
            let Some((carved, free_after)) = carve_from_largest(&full_free, n) else {
                continue;
            };
            // Rebuild bno/cnt over the remaining free set + rewrite the AGF.
            // Fails if the remainder can't host the btrees — try the next AG.
            if self
                .rebuild_ag_freespace(sb, agno, expected_len, &free_after)
                .is_err()
            {
                continue;
            }
            self.resync_sb_fdblocks(sb)?;
            let start_agbno = carved[0] as u64;
            return Ok((agno << sb.agblklog) | start_agbno);
        }
        Err(FilesystemError::DiskFull(
            "no allocation group has a single free extent large enough".into(),
        ))
    }

    /// The AG's complete free-block set (AG-relative extents): the bnobt's free
    /// records plus the blocks both space btrees themselves occupy (which a
    /// rebuild reclaims), coalesced — the same reconstruction R2 uses.
    fn current_full_free(
        &mut self,
        sb: &super::sb::XfsSuperblock,
        agno: u64,
        expected_len: u64,
    ) -> Result<Vec<FreeExtent>, FilesystemError> {
        let sectsize = sb.sectsize as u64;
        let agblocks = sb.agblocks as u64;
        let blocksize = sb.blocksize as u64;
        let agf_byte = self.partition_offset + agno * agblocks * blocksize + sectsize;
        let mut agf_sec = vec![0u8; sectsize as usize];
        read_at_aligned(&mut self.reader, agf_byte, sectsize, &mut agf_sec)?;
        let agf = XfsAgf::parse(&agf_sec)?;
        let (recs, bno_blocks) =
            self.walk_alloc_tree(sb, agno, agf.bno_root, expected_len, XFS_ABTB_MAGIC)?;
        let (_cnt_recs, cnt_blocks) =
            self.walk_alloc_tree(sb, agno, agf.cnt_root, expected_len, XFS_ABTC_MAGIC)?;
        let mut all = recs;
        for agbno in bno_blocks.into_iter().chain(cnt_blocks) {
            all.push(FreeExtent {
                startblock: agbno,
                blockcount: 1,
            });
        }
        Ok(coalesce(all))
    }

    /// Write `data_len` bytes from `data` into `[fsblock, fsblock+count)`,
    /// zero-padding the rest of the final block.
    fn write_file_data(
        &mut self,
        sb: &super::sb::XfsSuperblock,
        fsblock: u64,
        count: u32,
        data: &mut dyn Read,
        data_len: u64,
    ) -> Result<(), FilesystemError> {
        use std::io::SeekFrom;
        let bs = sb.blocksize as u64;
        let span = count as u64 * bs;
        let mut buf = vec![0u8; span as usize];
        let want = data_len.min(span) as usize;
        let mut filled = 0usize;
        while filled < want {
            let nr = data.read(&mut buf[filled..want])?;
            if nr == 0 {
                break;
            }
            filled += nr;
        }
        let part_byte = fsblock_to_partition_byte(fsblock, sb.agblocks, sb.agblklog, sb.blocksize);
        self.reader
            .seek(SeekFrom::Start(self.partition_offset + part_byte))?;
        self.reader.write_all(&buf)?;
        Ok(())
    }

    /// Initialize a freshly-allocated inode as a regular file. `extents` is
    /// the data fork as `(startoff, fsblock, count)` records (already in
    /// file-offset order); empty for a zero-length file. When
    /// `bmbt_leaf_fsblock` is `Some`, the inode is written in btree format
    /// (§2.1 hole (D)): the inline fork carries an `xfs_bmdr_block` root that
    /// points at the single leaf block, `di_format = 3`, and `di_nblocks`
    /// includes the leaf block. Otherwise the extents are written inline
    /// (`di_format = 2`), and the caller must have bounded the count to the
    /// inline literal area.
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn init_file_inode(
        &mut self,
        sb: &super::sb::XfsSuperblock,
        ino: u64,
        mode: u16,
        uid: u32,
        gid: u32,
        size: u64,
        extents: &[(u64, u64, u32)],
        bmbt_leaf_fsblock: Option<u64>,
    ) -> Result<(), FilesystemError> {
        let (_core, mut buf) = self.read_inode_buf(ino)?;
        let version = buf[4];
        let gen = BigEndian::read_u32(&buf[92..96]).wrapping_add(1);
        let isz = buf.len();
        let use_btree = bmbt_leaf_fsblock.is_some();

        BigEndian::write_u16(&mut buf[0..2], super::types::XFS_DINODE_MAGIC);
        BigEndian::write_u16(&mut buf[2..4], mode);
        buf[4] = version;
        buf[5] = if use_btree { 3 } else { 2 }; // di_format: btree or extents
        if version == 1 {
            BigEndian::write_u16(&mut buf[6..8], 1);
            BigEndian::write_u32(&mut buf[16..20], 0);
        } else {
            BigEndian::write_u16(&mut buf[6..8], 0);
            BigEndian::write_u32(&mut buf[16..20], 1);
        }
        BigEndian::write_u32(&mut buf[8..12], uid);
        BigEndian::write_u32(&mut buf[12..16], gid);
        for b in buf.iter_mut().take(56).skip(20) {
            *b = 0;
        }
        BigEndian::write_u64(&mut buf[56..64], size);
        // di_nblocks = data extent blocks + (bmbt leaf block, if any).
        let data_nblocks: u64 = extents.iter().map(|&(_, _, c)| c as u64).sum();
        let bmbt_nblocks: u64 = if use_btree { 1 } else { 0 };
        BigEndian::write_u64(&mut buf[64..72], data_nblocks + bmbt_nblocks);
        for b in buf.iter_mut().take(76).skip(72) {
            *b = 0; // extsize
        }
        // di_nextents: total file-offset extents (same value in both formats).
        BigEndian::write_u32(&mut buf[76..80], extents.len() as u32);
        for b in buf.iter_mut().take(92).skip(80) {
            *b = 0; // anextents, forkoff, aformat(set below), dm*, flags
        }
        buf[83] = 2; // di_aformat = extents (no attr fork)
        BigEndian::write_u32(&mut buf[92..96], gen);
        BigEndian::write_u32(&mut buf[96..100], NULLAGINO);

        // v3 inode core: di_changecount / di_lsn / di_flags2 / di_cowextsize
        // / di_pad2 / di_crtime / di_ino / di_uuid live at offsets 104..176.
        // `write_inode_region` stamps di_crc + di_uuid + di_ino + di_lsn +
        // bumps di_changecount; we zero the v3 fields here so a freshly-init
        // inode doesn't inherit a previous occupant's flags/crtime.
        if sb.is_v5() {
            for b in buf.iter_mut().take(176).skip(100) {
                *b = 0;
            }
        }

        // Fork starts at byte 100 in v4 (`fork_offset(false)`) and at byte
        // 176 in v5 (`fork_offset(true)` — the v3 inode core is 176 bytes).
        let fs = sb.fork_offset();
        for b in buf.iter_mut().take(isz).skip(fs) {
            *b = 0;
        }
        if let Some(leaf_fsblock) = bmbt_leaf_fsblock {
            // Btree format: write an in-inode root pointing at the leaf.
            let first_startoff = extents.first().map(|&(off, _, _)| off).ok_or_else(|| {
                FilesystemError::Parse("bmbt root requires at least one extent".into())
            })?;
            write_bmbt_root_to_leaf(&mut buf[fs..isz], first_startoff, leaf_fsblock);
        } else {
            // Inline extents: each record must fit the literal area.
            if fs + extents.len() * BMBT_REC_SIZE > isz {
                return Err(FilesystemError::Unsupported(
                    "too many extents for an inline data fork (bmap-btree not threaded here)"
                        .into(),
                ));
            }
            for (i, &(startoff, fsblock, count)) in extents.iter().enumerate() {
                let rec = encode_extent(false, startoff, fsblock, count as u64);
                buf[fs + i * BMBT_REC_SIZE..fs + (i + 1) * BMBT_REC_SIZE].copy_from_slice(&rec);
            }
        }
        self.write_inode_region(sb, ino, &buf)
    }

    /// Internal entry point for `create_file`: pre-flight the parent insert,
    /// allocate the data blocks (single contiguous extent OR multiple runs,
    /// possibly larger than one extent record), write the data, allocate +
    /// initialize the file inode, then insert it into the parent. When the
    /// resulting extent records overflow the inline literal area, the data
    /// fork is laid out as a single-leaf bmap btree (§2.1 hole (D)). Returns
    /// the new file's inode number.
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn do_create_file(
        &mut self,
        parent_ino: u64,
        name: &str,
        data: &mut dyn Read,
        data_len: u64,
        mode: u16,
        uid: u32,
        gid: u32,
    ) -> Result<u64, FilesystemError> {
        let sb = self.superblock().clone();
        self.dir_can_insert(&sb, parent_ino, name, false)?;

        let bs = sb.blocksize as u64;
        let nblocks = data_len.div_ceil(bs);

        // Allocate data first (so a DiskFull leaves no half-built inode). The
        // allocator returns AG-local runs; runs_to_extent_records splits any
        // run larger than `MAX_SINGLE_EXTENT_BLOCKS` into multiple consecutive
        // extent records (the on-disk blockcount field is only 21 bits).
        let runs = if nblocks > 0 {
            self.alloc_extents(&sb, nblocks)?
        } else {
            Vec::new()
        };
        let extents = runs_to_extent_records(&runs);

        // Pick between inline and btree data-fork format. v1 supports single-
        // leaf bmbt only: leaves beyond the leaf cap force a clear error so
        // the partial allocation is reclaimed before the operation aborts.
        let max_inline = (sb.inodesize as usize - sb.fork_offset()) / BMBT_REC_SIZE;
        let leaf_max = bmbt_leaf_max(&sb);
        if extents.len() > leaf_max {
            // Roll back the data allocation.
            self.free_blocks(&sb, &runs)?;
            return Err(FilesystemError::Unsupported(format!(
                "file needs {} extents; multi-leaf bmap btree not yet implemented (max {})",
                extents.len(),
                leaf_max
            )));
        }
        let bmbt_leaf_fsblock = if extents.len() > max_inline {
            // Allocate one block for the leaf; on failure, roll back the data.
            match self.alloc_blocks(&sb, 1) {
                Ok(fb) => Some(fb),
                Err(e) => {
                    self.free_blocks(&sb, &runs)?;
                    return Err(e);
                }
            }
        } else {
            None
        };

        // From here on, every failure leaves an inode-shaped on-disk
        // commitment — that's the existing contract, so don't bend the
        // ordering to try to rescue it.
        let ino = self.alloc_inode_slot(&sb)?;
        self.write_file_data_extents(&sb, &extents, data, data_len)?;
        if let Some(leaf_fsblock) = bmbt_leaf_fsblock {
            let mut leaf_bytes = build_bmbt_leaf(&extents, &sb);
            if sb.is_v5() {
                // v5 lblock-crc tuple — `bb_owner` is the file's own inode.
                let blkno = super::v5_crc::fsblock_to_daddr(leaf_fsblock, &sb);
                super::v5_crc::stamp_lblock_crc_header(&mut leaf_bytes, blkno, ino, &sb);
            }
            self.write_fsblock(&sb, leaf_fsblock, &leaf_bytes)?;
        }
        self.init_file_inode(
            &sb,
            ino,
            mode,
            uid,
            gid,
            data_len,
            &extents,
            bmbt_leaf_fsblock,
        )?;
        self.dir_insert_entry(&sb, parent_ino, name, ino, false)?;
        self.reader.flush()?;
        Ok(ino)
    }

    /// Allocate `n` total blocks as one or more contiguous runs (returns
    /// `(fsblock, count)` per run, file-offset assignment is the caller's
    /// job). Tries a single contiguous extent first via [`alloc_blocks`]; if
    /// that fails (`DiskFull`), walks AGs largest-first and carves multiple
    /// runs from one AG's free set. The run count is capped at
    /// [`bmbt_leaf_max`] — the on-disk ceiling for a single-leaf bmap btree
    /// (§2.1 hole (D)). Errors `DiskFull` when no AG can satisfy `n` within
    /// that cap. The largest individual run is left uncapped here; oversize
    /// runs are split into per-record-sized extents downstream by
    /// [`runs_to_extent_records`].
    fn alloc_extents(
        &mut self,
        sb: &super::sb::XfsSuperblock,
        n: u64,
    ) -> Result<Vec<(u64, u32)>, FilesystemError> {
        // Fast path: one contiguous run when n fits a u32 (alloc_blocks's
        // interface). Single-extent allocations larger than 2^32 blocks are
        // theoretically possible but absurd for our target volumes, so this
        // is a reasonable cap on the fast-path attempt.
        if n <= u64::from(u32::MAX) {
            match self.alloc_blocks(sb, n as u32) {
                Ok(fsblock) => return Ok(vec![(fsblock, n as u32)]),
                Err(FilesystemError::DiskFull(_)) => {}
                Err(e) => return Err(e),
            }
        }

        let bs = sb.blocksize as usize;
        let leaf_max = bmbt_leaf_max(sb);
        let agblocks = sb.agblocks as u64;
        for agno in 0..sb.agcount as u64 {
            let expected_len = if agno == sb.agcount as u64 - 1 {
                sb.dblocks - agno * agblocks
            } else {
                agblocks
            };
            let mut full = match self.current_full_free(sb, agno, expected_len) {
                Ok(f) => f,
                Err(_) => continue,
            };
            let total: u64 = full.iter().map(|e| e.blockcount as u64).sum();
            if total < n {
                continue;
            }
            // Allocate largest-first to minimise the run count; take from the
            // start of each extent, leaving its tail free.
            full.sort_by_key(|e| std::cmp::Reverse(e.blockcount));
            let mut runs: Vec<(u32, u32)> = Vec::new();
            let mut post: Vec<FreeExtent> = Vec::new();
            let mut remaining = n;
            for ext in &full {
                if remaining == 0 {
                    post.push(*ext);
                    continue;
                }
                let take = remaining.min(ext.blockcount as u64) as u32;
                runs.push((ext.startblock, take));
                remaining -= take as u64;
                if take < ext.blockcount {
                    post.push(FreeExtent {
                        startblock: ext.startblock + take,
                        blockcount: ext.blockcount - take,
                    });
                }
            }
            if remaining > 0 || runs.len() > leaf_max {
                continue;
            }
            // The rebuild carves its btree blocks from the largest remaining
            // free extent, so make sure one is big enough.
            let post_largest = post.iter().map(|e| e.blockcount).max().unwrap_or(0) as usize;
            let need_bt = 2 * blocks_needed(post.len(), bs, sb.is_v5());
            if post_largest <= need_bt {
                continue;
            }
            self.rebuild_ag_freespace(sb, agno, expected_len, &post)?;
            self.resync_sb_fdblocks(sb)?;
            return Ok(runs
                .into_iter()
                .map(|(agbno, c)| ((agno << sb.agblklog) | agbno as u64, c))
                .collect());
        }
        Err(FilesystemError::DiskFull(format!(
            "could not allocate {n} blocks as <= {leaf_max} contiguous runs"
        )))
    }

    /// Write `data_len` bytes from `data` across the file's `extents`
    /// (`(startoff, fsblock, count)`), zero-padding the final block of each run.
    fn write_file_data_extents(
        &mut self,
        sb: &super::sb::XfsSuperblock,
        extents: &[(u64, u64, u32)],
        data: &mut dyn Read,
        data_len: u64,
    ) -> Result<(), FilesystemError> {
        let bs = sb.blocksize as u64;
        for &(startoff, fsblock, count) in extents {
            let run_file_start = startoff * bs;
            let run_bytes = count as u64 * bs;
            let want = data_len.saturating_sub(run_file_start).min(run_bytes);
            self.write_file_data(sb, fsblock, count, data, want)?;
        }
        Ok(())
    }

    // ----- delete_entry ------------------------------------------------------

    /// Remove `name` from short-form directory `parent_ino`, rebuilding the
    /// inline fork without it (count--, di_size), and decrementing the parent
    /// link count when the removed child was a subdirectory. Errors `NotFound`
    /// if the name isn't present.
    fn sf_remove_entry(
        &mut self,
        sb: &super::sb::XfsSuperblock,
        parent_ino: u64,
        name: &str,
        child_was_dir: bool,
    ) -> Result<(), FilesystemError> {
        let has_ftype = sb.has_ftype();
        let (core, mut buf) = self.read_inode_buf(parent_ino)?;
        if !core.is_dir() || core.format != super::types::DiFormat::Local {
            return Err(FilesystemError::Unsupported(
                "parent is not a short-form directory".into(),
            ));
        }
        let fs = sb.fork_offset();
        let fork_len = core.size as usize;
        let fork_end = fs + fork_len;

        // Walk the entries capturing raw spans + names; collect survivors.
        let count = buf[fs] as usize;
        let i8count = buf[fs + 1];
        let ino_width = if i8count > 0 { 8 } else { 4 };
        let mut survivors: Vec<(usize, usize)> = Vec::with_capacity(count); // (start,end) in buf
        let mut removed = false;
        let mut pos = fs + 2 + ino_width;
        for _ in 0..count {
            let start = pos;
            if start + 3 > fork_end {
                return Err(FilesystemError::Parse("short-form entry truncated".into()));
            }
            let namelen = buf[start] as usize;
            let name_off = start + 3;
            let next = name_off + namelen + usize::from(has_ftype) + ino_width;
            if next > fork_end {
                return Err(FilesystemError::Parse("short-form entry overflow".into()));
            }
            let ename = String::from_utf8_lossy(&buf[name_off..name_off + namelen]);
            if !removed && ename == name {
                removed = true; // drop this one
            } else {
                survivors.push((start, next));
            }
            pos = next;
        }
        if !removed {
            return Err(FilesystemError::NotFound(name.into()));
        }

        // Rebuild: header (count', i8count, parent) + surviving spans verbatim.
        let header = buf[fs..fs + 2 + ino_width].to_vec();
        let mut new_fork = Vec::with_capacity(fork_len);
        new_fork.extend_from_slice(&header);
        for (s, e) in &survivors {
            new_fork.extend_from_slice(&buf[*s..*e]);
        }
        new_fork[0] = survivors.len() as u8; // count

        buf[fs..fs + new_fork.len()].copy_from_slice(&new_fork);
        for b in buf.iter_mut().take(fork_end).skip(fs + new_fork.len()) {
            *b = 0;
        }
        BigEndian::write_u64(&mut buf[56..64], new_fork.len() as u64);
        if child_was_dir {
            let version = buf[4];
            if version == 1 {
                let n = BigEndian::read_u16(&buf[6..8]).saturating_sub(1);
                BigEndian::write_u16(&mut buf[6..8], n);
            } else {
                let n = BigEndian::read_u32(&buf[16..20]).saturating_sub(1);
                BigEndian::write_u32(&mut buf[16..20], n);
            }
        }
        self.write_inode_region(sb, parent_ino, &buf)
    }

    /// Free a previously-allocated inode: mark its inobt slot free (set the
    /// `ir_free` bit, ++ record freecount + AGI freecount + `sb_ifree`) and zero
    /// its on-disk `di_mode` so it reads as a free inode.
    fn free_inode(
        &mut self,
        sb: &super::sb::XfsSuperblock,
        ino: u64,
    ) -> Result<(), FilesystemError> {
        let sectsize = sb.sectsize as u64;
        let agblocks = sb.agblocks as u64;
        let blocksize = sb.blocksize as u64;
        let bs = sb.blocksize as usize;
        let ino_shift = sb.agblklog + sb.inopblog;
        let agno = ino >> ino_shift;
        let agino = (ino & ((1u64 << ino_shift) - 1)) as u32;

        let agi_byte = self.partition_offset + agno * agblocks * blocksize + 2 * sectsize;
        let mut agi_sec = vec![0u8; sectsize as usize];
        read_at_aligned(&mut self.reader, agi_byte, sectsize, &mut agi_sec)?;
        let agi = XfsAgi::parse(&agi_sec)?;
        let leaves = self.collect_inobt_leaf_blocks(sb, agno, agi.root)?;
        let hdr_len = sb.sblock_hdr_len();
        for leaf_agbno in leaves {
            let fsblock = (agno << sb.agblklog) | leaf_agbno as u64;
            let mut block = vec![0u8; bs];
            self.read_fsblock(fsblock, &mut block)?;
            let numrecs = BigEndian::read_u16(&block[6..8]) as usize;
            let max_leaf = (bs - hdr_len) / INOBT_REC_SIZE;
            if numrecs > max_leaf {
                continue;
            }
            for r in 0..numrecs {
                let off = hdr_len + r * INOBT_REC_SIZE;
                let start_agino = BigEndian::read_u32(&block[off..off + 4]);
                if agino < start_agino || agino >= start_agino + XFS_INODES_PER_CHUNK as u32 {
                    continue;
                }
                let slot = (agino - start_agino) as u64;
                let freecount = BigEndian::read_u32(&block[off + 4..off + 8]);
                let free = BigEndian::read_u64(&block[off + 8..off + 16]);
                if (free >> slot) & 1 == 1 {
                    return Err(FilesystemError::InvalidData(format!(
                        "inode {ino} already free in inobt"
                    )));
                }
                BigEndian::write_u32(&mut block[off + 4..off + 8], freecount + 1);
                BigEndian::write_u64(&mut block[off + 8..off + 16], free | (1u64 << slot));
                if sb.is_v5() {
                    stamp_v5_sblock_for_ag(&mut block, sb, agno, leaf_agbno);
                }
                self.write_fsblock(sb, fsblock, &block)?;

                BigEndian::write_u32(
                    &mut agi_sec[AGI_FREECOUNT_OFF..AGI_FREECOUNT_OFF + 4],
                    agi.freecount + 1,
                );
                self.write_agi_sector(sb, agi_byte, &mut agi_sec)?;
                self.bump_sb_ifree(sb, 1)?;

                // Zero the dinode's mode so it reads as free.
                let (_c, mut ibuf) = self.read_inode_buf(ino)?;
                BigEndian::write_u16(&mut ibuf[2..4], 0);
                self.write_inode_region(sb, ino, &ibuf)?;
                return Ok(());
            }
        }
        Err(FilesystemError::NotFound(format!(
            "inode {ino} not found in any inobt chunk"
        )))
    }

    /// Return `extents` (as `(fsblock, count)`) to the free-space btrees by
    /// rebuilding each touched AG's bno/cnt over its current free set plus the
    /// freed blocks, then resyncing `sb_fdblocks`. The inverse of
    /// [`alloc_blocks`].
    fn free_blocks(
        &mut self,
        sb: &super::sb::XfsSuperblock,
        extents: &[(u64, u32)],
    ) -> Result<(), FilesystemError> {
        let agblocks = sb.agblocks as u64;
        let agmask = (1u64 << sb.agblklog) - 1;
        // Group the freed runs by AG (AG-relative).
        let mut by_ag: std::collections::BTreeMap<u64, Vec<FreeExtent>> =
            std::collections::BTreeMap::new();
        for &(fsblock, count) in extents {
            if count == 0 {
                continue;
            }
            let agno = fsblock >> sb.agblklog;
            let agbno = (fsblock & agmask) as u32;
            by_ag.entry(agno).or_default().push(FreeExtent {
                startblock: agbno,
                blockcount: count,
            });
        }
        for (agno, freed) in by_ag {
            let expected_len = if agno == sb.agcount as u64 - 1 {
                sb.dblocks - agno * agblocks
            } else {
                agblocks
            };
            let mut all = self.current_full_free(sb, agno, expected_len)?;
            all.extend(freed);
            let derived = coalesce(all);
            self.rebuild_ag_freespace(sb, agno, expected_len, &derived)?;
        }
        self.resync_sb_fdblocks(sb)?;
        Ok(())
    }

    /// Internal entry point for `delete_entry`: remove `name` (-> `child_ino`)
    /// from short-form directory `parent_ino`, free the child's data blocks, and
    /// free the child inode. Refuses a non-empty / non-short-form directory and
    /// a bmap-btree-format child (multi-extent free not implemented).
    pub(crate) fn do_delete_entry(
        &mut self,
        parent_ino: u64,
        name: &str,
        child_ino: u64,
    ) -> Result<(), FilesystemError> {
        let sb = self.superblock().clone();
        let (cc, cbuf) = self.read_inode_buf(child_ino)?;
        let child_is_dir = cc.is_dir();
        if child_is_dir {
            if cc.format != super::types::DiFormat::Local {
                return Err(FilesystemError::Unsupported(
                    "cannot delete a non-short-form directory yet".into(),
                ));
            }
            let fork = self.data_fork(&cc, &cbuf);
            if fork.first().copied().unwrap_or(0) != 0 {
                return Err(FilesystemError::InvalidData("directory not empty".into()));
            }
        }
        let extents: Vec<(u64, u32)> = match cc.format {
            super::types::DiFormat::Extents => self
                .decode_data_extents(&cc, &cbuf)?
                .iter()
                .map(|e| (e.startblock, e.blockcount as u32))
                .collect(),
            super::types::DiFormat::Btree => {
                return Err(FilesystemError::Unsupported(
                    "cannot delete a bmap-btree-format inode yet".into(),
                ));
            }
            _ => Vec::new(), // local / device: owns no blocks
        };

        self.dir_remove_entry(&sb, parent_ino, name, child_is_dir)?;
        if !extents.is_empty() {
            self.free_blocks(&sb, &extents)?;
        }
        self.free_inode(&sb, child_ino)?;
        self.reader.flush()?;
        Ok(())
    }

    // ----- format-aware directory insert (short-form OR single-block) --------

    /// Check, without writing, that `name` can be inserted into directory
    /// `parent_ino` (unique name; parent is short-form or single-block; the
    /// result fits — converting short-form→block if the inline fork overflows).
    /// Errors `AlreadyExists` / `DiskFull` / `Unsupported` accordingly.
    pub(crate) fn dir_can_insert(
        &mut self,
        sb: &super::sb::XfsSuperblock,
        parent_ino: u64,
        name: &str,
        new_is_dir: bool,
    ) -> Result<(), FilesystemError> {
        let has_ftype = sb.has_ftype();
        let (core, buf) = self.read_inode_buf(parent_ino)?;
        if !core.is_dir() {
            return Err(FilesystemError::Unsupported(
                "parent is not a directory".into(),
            ));
        }
        let (dotdot, mut entries) = self.gather_dir_entries(sb, &core, &buf, parent_ino)?;
        if entries.iter().any(|(n, _, _)| n == name) {
            return Err(FilesystemError::AlreadyExists(name.into()));
        }
        match core.format {
            super::types::DiFormat::Local => {
                // Inline fit?
                let fs = sb.fork_offset();
                let entry_len = 1 + 2 + name.len() + usize::from(has_ftype) + 4;
                let avail_end = if core.forkoff > 0 {
                    fs + (core.forkoff as usize) * 8
                } else {
                    buf.len()
                };
                if fs + core.size as usize + entry_len <= avail_end {
                    return Ok(()); // fits inline
                }
                // Would overflow → must fit one block.
                let ft = if new_is_dir {
                    XFS_DIR3_FT_DIR
                } else {
                    XFS_DIR3_FT_REG_FILE
                };
                entries.push((name.to_string(), 0, ft));
                build_block_dir(
                    &entries,
                    parent_ino,
                    dotdot,
                    sb.dirblksize() as usize,
                    has_ftype,
                    sb.is_v5(),
                )
                .map(|_| ())
            }
            super::types::DiFormat::Extents => {
                // Refuse insert into an already-leaf-form dir (multi-extent
                // data fork). v1 of hole (C) ships block→leaf conversion only;
                // leaf-form grow + lookup-by-hash isn't wired here yet. Catch
                // it now, before do_create_file allocates an inode whose
                // directory entry can't be inserted (would orphan it).
                let extents = self.decode_data_extents(&core, &buf)?;
                if extents.len() > 1 || extents.first().is_some_and(|e| e.startoff != 0) {
                    return Err(FilesystemError::Unsupported(
                        "insert into leaf/node-form directory not yet implemented".into(),
                    ));
                }
                let ft = if new_is_dir {
                    XFS_DIR3_FT_DIR
                } else {
                    XFS_DIR3_FT_REG_FILE
                };
                entries.push((name.to_string(), 0, ft));
                let dirblksize = sb.dirblksize() as usize;
                match build_block_dir(
                    &entries,
                    parent_ino,
                    dotdot,
                    dirblksize,
                    has_ftype,
                    sb.is_v5(),
                ) {
                    Ok(_) => Ok(()),
                    Err(FilesystemError::DiskFull(_)) => {
                        // Single block would overflow — would convert to
                        // leaf form. Pre-flight that the 2-data-block split
                        // would actually fit (§2.1 hole (C)).
                        let mut sorted = entries.clone();
                        sorted.sort_by_key(|(n, _, _)| dir_hashname(n.as_bytes()));
                        let dotdot_bytes =
                            dir2_data_entsize(1, has_ftype) + dir2_data_entsize(2, has_ftype);
                        let total: usize = sorted
                            .iter()
                            .map(|(n, _, _)| dir2_data_entsize(n.len(), has_ftype))
                            .sum();
                        let target = (total + dotdot_bytes).saturating_sub(dotdot_bytes) / 2;
                        let mut block0_payload = 0usize;
                        let mut split_at = 0;
                        let mut any = false;
                        for (i, (n, _, _)) in sorted.iter().enumerate() {
                            let ent = dir2_data_entsize(n.len(), has_ftype);
                            if any && block0_payload + ent > target {
                                split_at = i;
                                break;
                            }
                            block0_payload += ent;
                            any = true;
                            split_at = i + 1;
                        }
                        let block0_bytes = XFS_DIR2_DATA_HDR_LEN + dotdot_bytes + block0_payload;
                        let block1_bytes = XFS_DIR2_DATA_HDR_LEN + (total - block0_payload);
                        if block0_bytes > dirblksize || block1_bytes > dirblksize {
                            return Err(FilesystemError::DiskFull(format!(
                                "leaf-form conversion split block 0={block0_bytes} block 1={block1_bytes} \
                                 doesn't fit dirblksize {dirblksize} (deeper leaf/node form not implemented)"
                            )));
                        }
                        let _ = split_at;
                        Ok(())
                    }
                    Err(e) => Err(e),
                }
            }
            _ => Err(FilesystemError::Unsupported(
                "parent directory format not editable (btree/leaf/node)".into(),
            )),
        }
    }

    /// Insert `(name -> child_ino)` into directory `parent_ino`, handling
    /// short-form (inline, or converted to a single block on overflow) and
    /// single-block directories. Bumps the parent link count for a subdirectory.
    pub(crate) fn dir_insert_entry(
        &mut self,
        sb: &super::sb::XfsSuperblock,
        parent_ino: u64,
        name: &str,
        child_ino: u64,
        is_dir: bool,
    ) -> Result<(), FilesystemError> {
        let (core, _buf) = self.read_inode_buf(parent_ino)?;
        match core.format {
            super::types::DiFormat::Local => {
                match self.sf_insert_entry(sb, parent_ino, name, child_ino, is_dir) {
                    Err(FilesystemError::DiskFull(_)) => {
                        self.convert_sf_dir_to_block(sb, parent_ino, name, child_ino, is_dir)
                    }
                    other => other,
                }
            }
            super::types::DiFormat::Extents => {
                self.block_insert_entry(sb, parent_ino, name, child_ino, is_dir)
            }
            _ => Err(FilesystemError::Unsupported(
                "parent directory format not editable (btree/leaf/node)".into(),
            )),
        }
    }

    /// Convert an overflowing short-form directory to a single-block directory,
    /// including the new `(name -> child_ino)` entry. Allocates one directory
    /// block, writes it, and rewrites the inode to extents format.
    fn convert_sf_dir_to_block(
        &mut self,
        sb: &super::sb::XfsSuperblock,
        parent_ino: u64,
        name: &str,
        child_ino: u64,
        is_dir: bool,
    ) -> Result<(), FilesystemError> {
        let has_ftype = sb.has_ftype();
        let (core, buf) = self.read_inode_buf(parent_ino)?;
        let (dotdot, mut entries) = self.gather_dir_entries(sb, &core, &buf, parent_ino)?;
        let ft = if is_dir {
            XFS_DIR3_FT_DIR
        } else {
            XFS_DIR3_FT_REG_FILE
        };
        entries.push((name.to_string(), child_ino, ft));

        let dirblksize = sb.dirblksize() as usize;
        let mut block = build_block_dir(
            &entries,
            parent_ino,
            dotdot,
            dirblksize,
            has_ftype,
            sb.is_v5(),
        )?;
        let blocks_per_dir = (dirblksize as u64).div_ceil(sb.blocksize as u64) as u32;
        let fsblock = self.alloc_blocks(sb, blocks_per_dir)?;
        if sb.is_v5() {
            // Stamp xfs_dir3_blk_hdr (crc/blkno/lsn/uuid/owner) over the
            // fresh `XDB3` block before it goes to disk.
            let blkno = super::v5_crc::fsblock_to_daddr(fsblock, sb);
            super::v5_crc::stamp_dir3_blk_hdr(&mut block, blkno, parent_ino, sb);
        }
        self.write_dir_block(sb, fsblock, &block)?;
        self.set_dir_inode_single_extent(
            sb,
            parent_ino,
            fsblock,
            blocks_per_dir,
            dirblksize,
            is_dir,
        )
    }

    /// Rebuild an already single-block directory with the new entry appended,
    /// writing it back in place (no reallocation). When the entries no longer
    /// fit one block, convert the directory to **leaf form** (§2.1 hole (C)):
    /// two XD2D data blocks plus an XD2F leaf1 block at file offset
    /// `XFS_DIR2_LEAF_OFFSET`.
    fn block_insert_entry(
        &mut self,
        sb: &super::sb::XfsSuperblock,
        parent_ino: u64,
        name: &str,
        child_ino: u64,
        is_dir: bool,
    ) -> Result<(), FilesystemError> {
        let has_ftype = sb.has_ftype();
        let (core, buf) = self.read_inode_buf(parent_ino)?;
        let extents = self.decode_data_extents(&core, &buf)?;
        let first = extents
            .first()
            .ok_or_else(|| FilesystemError::Parse("block dir has no extent".into()))?;
        // Single-block dir: exactly one extent at file offset 0. Anything
        // else (already leaf/node form) goes through the leaf-aware insert
        // path, which is not yet implemented for further growth.
        if first.startoff != 0 {
            return Err(FilesystemError::Unsupported(
                "multi-block directory: insert into existing leaf/node form not yet supported"
                    .into(),
            ));
        }
        if extents.len() > 1 {
            return Err(FilesystemError::Unsupported(
                "leaf-form directory insert not yet implemented (only block→leaf conversion)"
                    .into(),
            ));
        }
        let dirblksize = sb.dirblksize() as usize;
        let blocks_per_dir = (dirblksize as u64).div_ceil(sb.blocksize as u64) as u32;
        let fsblock = first.startblock;

        let (dotdot, mut entries) =
            self.read_block_dir_entries(sb, fsblock, dirblksize, has_ftype)?;
        if entries.iter().any(|(n, _, _)| n == name) {
            return Err(FilesystemError::AlreadyExists(name.into()));
        }
        let ft = if is_dir {
            XFS_DIR3_FT_DIR
        } else {
            XFS_DIR3_FT_REG_FILE
        };
        entries.push((name.to_string(), child_ino, ft));

        // Try the in-place block rebuild first. If that overflows, fall
        // through to the leaf-form conversion path.
        match build_block_dir(
            &entries,
            parent_ino,
            dotdot,
            dirblksize,
            has_ftype,
            sb.is_v5(),
        ) {
            Ok(mut block) => {
                if sb.is_v5() {
                    let blkno = super::v5_crc::fsblock_to_daddr(fsblock, sb);
                    super::v5_crc::stamp_dir3_blk_hdr(&mut block, blkno, parent_ino, sb);
                }
                self.write_dir_block(sb, fsblock, &block)?;
            }
            Err(FilesystemError::DiskFull(_)) => {
                self.convert_block_dir_to_leaf_form(
                    sb,
                    parent_ino,
                    &entries,
                    dotdot,
                    fsblock,
                    blocks_per_dir,
                )?;
            }
            Err(e) => return Err(e),
        }
        if is_dir {
            self.bump_dir_nlink(sb, parent_ino, 1)?;
        }
        Ok(())
    }

    /// Convert a single-block directory to leaf form: redistribute every entry
    /// across two XD2D data blocks (entries sorted by hash, split at the byte
    /// midpoint), allocate the second data block and the XD2F leaf1 index
    /// block (at file offset `XFS_DIR2_LEAF_OFFSET` in fsblocks), build all
    /// three blocks, write them, and rewrite the inode with a 3-extent inline
    /// data fork. `existing_fsblock` is the dir's current single-block fsblock
    /// (reused as the new block 0); the second data + leaf1 blocks are freshly
    /// allocated. §2.1 hole (C) v1 — supports the block → 2-data-block leaf
    /// shape only; further growth (3+ data blocks, node form) is parked.
    fn convert_block_dir_to_leaf_form(
        &mut self,
        sb: &super::sb::XfsSuperblock,
        parent_ino: u64,
        all_entries: &[(String, u64, u8)],
        dotdot: u64,
        existing_fsblock: u64,
        blocks_per_dir: u32,
    ) -> Result<(), FilesystemError> {
        let has_ftype = sb.has_ftype();
        let dirblksize = sb.dirblksize() as usize;

        // Sort by hash so the leaf index entries are naturally contiguous,
        // then split the entries so block 0 (with `.` + `..`) and block 1
        // each end up roughly half-full by data-bytes count.
        let mut sorted = all_entries.to_vec();
        sorted.sort_by_key(|(n, _, _)| dir_hashname(n.as_bytes()));

        let dotdot_bytes = dir2_data_entsize(1, has_ftype) + dir2_data_entsize(2, has_ftype);
        let total_entry_bytes: usize = sorted
            .iter()
            .map(|(n, _, _)| dir2_data_entsize(n.len(), has_ftype))
            .sum();
        // Targets: half the entry bytes per block, after subtracting `.`/`..`
        // from block 0's budget.
        let target_block0_payload =
            (total_entry_bytes + dotdot_bytes).saturating_sub(dotdot_bytes) / 2;
        let mut block0: Vec<(String, u64, u8)> = Vec::new();
        let mut block0_payload = 0usize;
        let mut split_at = 0;
        for (i, (n, ino, ft)) in sorted.iter().enumerate() {
            let ent = dir2_data_entsize(n.len(), has_ftype);
            if !block0.is_empty() && block0_payload + ent > target_block0_payload {
                split_at = i;
                break;
            }
            block0.push((n.clone(), *ino, *ft));
            block0_payload += ent;
            split_at = i + 1;
        }
        let block1: Vec<(String, u64, u8)> = sorted[split_at..].to_vec();

        // Build both data blocks. block 0 carries the synthetic `.`/`..`.
        let is_v5 = sb.is_v5();
        let mut data0 = build_leaf_data_block(
            &block0, parent_ino, dotdot, 0, dirblksize, has_ftype, true, is_v5,
        )?;
        let mut data1 = build_leaf_data_block(
            &block1,
            parent_ino,
            dotdot,
            blocks_per_dir as u64,
            dirblksize,
            has_ftype,
            false,
            is_v5,
        )?;
        data0.leaf_index.append(&mut data1.leaf_index);

        // bestfree[0] for each data block is at a known offset inside the
        // dir2/dir3 data header — v4 packs the triple at byte 4, v5 packs it
        // at byte 48 (after the 48-byte xfs_dir3_blk_hdr). Each triple is
        // {offset(u16 BE), length(u16 BE)}; the leaf1 `bests` array wants
        // the length only.
        let bests_off = if is_v5 { 50 } else { 6 };
        let bests = [
            BigEndian::read_u16(&data0.bytes[bests_off..bests_off + 2]),
            BigEndian::read_u16(&data1.bytes[bests_off..bests_off + 2]),
        ];
        let mut leaf1_bytes = build_leaf1_block(data0.leaf_index, &bests, dirblksize, is_v5)?;

        // Allocate two new dir blocks: data block 1, then the leaf1 block.
        // On a failure between the two allocs, roll back the first one.
        let data1_fsblock = self.alloc_blocks(sb, blocks_per_dir)?;
        let leaf1_fsblock = match self.alloc_blocks(sb, blocks_per_dir) {
            Ok(fb) => fb,
            Err(e) => {
                self.free_blocks(sb, &[(data1_fsblock, blocks_per_dir)])?;
                return Err(e);
            }
        };

        // v5 CRC stamps for all three blocks now that we know their fsblocks.
        // Data blocks carry an `xfs_dir3_blk_hdr`; the leaf1 block carries an
        // `xfs_da3_blkinfo`. Owner is the parent directory's inode.
        if is_v5 {
            let blkno0 = super::v5_crc::fsblock_to_daddr(existing_fsblock, sb);
            super::v5_crc::stamp_dir3_blk_hdr(&mut data0.bytes, blkno0, parent_ino, sb);
            let blkno1 = super::v5_crc::fsblock_to_daddr(data1_fsblock, sb);
            super::v5_crc::stamp_dir3_blk_hdr(&mut data1.bytes, blkno1, parent_ino, sb);
            let blkno_l = super::v5_crc::fsblock_to_daddr(leaf1_fsblock, sb);
            super::v5_crc::stamp_da3_blkinfo(&mut leaf1_bytes, blkno_l, parent_ino, sb);
        }

        // Lay down all three blocks. Block 0 (the existing single-block dir)
        // is rewritten in place with the XD2D/XDD3 format (no inline leaf tail).
        self.write_dir_block(sb, existing_fsblock, &data0.bytes)?;
        self.write_dir_block(sb, data1_fsblock, &data1.bytes)?;
        self.write_dir_block(sb, leaf1_fsblock, &leaf1_bytes)?;

        // Update the inode: 3 inline extents, di_size = 2 * dirblksize (only
        // the data blocks count toward dir size; the leaf1 block sits above
        // XFS_DIR2_LEAF_OFFSET and isn't part of the file's logical size),
        // di_nblocks = 3 * blocks_per_dir.
        let leaf_off_fb = dir2_leaf_offset_fb(sb.blocksize);
        let extents: [(u64, u64, u32); 3] = [
            (0, existing_fsblock, blocks_per_dir),
            (blocks_per_dir as u64, data1_fsblock, blocks_per_dir),
            (leaf_off_fb, leaf1_fsblock, blocks_per_dir),
        ];
        self.write_dir_inode_multi_extent(
            sb,
            parent_ino,
            &extents,
            2 * dirblksize as u64,
            3 * blocks_per_dir as u64,
        )
    }

    /// Rewrite a directory inode whose data fork holds N inline extents (each
    /// `(startoff, fsblock, blocks)`). Sets `di_format=Extents`, `di_size`,
    /// `di_nblocks`, `di_nextents=N`; preserves the rest of the core. The
    /// inline-fork fits the literal area when `N * 16 <= inodesize -
    /// fork_offset` — bounded by the caller (e.g. the leaf-form conversion
    /// only ever stores 3 extents, well within 9 inline slots).
    fn write_dir_inode_multi_extent(
        &mut self,
        sb: &super::sb::XfsSuperblock,
        ino: u64,
        extents: &[(u64, u64, u32)],
        size: u64,
        nblocks: u64,
    ) -> Result<(), FilesystemError> {
        let (_core, mut buf) = self.read_inode_buf(ino)?;
        buf[5] = 2; // di_format = extents
        BigEndian::write_u64(&mut buf[56..64], size); // di_size
        BigEndian::write_u64(&mut buf[64..72], nblocks); // di_nblocks
        BigEndian::write_u32(&mut buf[76..80], extents.len() as u32); // di_nextents
        let fs = sb.fork_offset();
        for b in buf.iter_mut().skip(fs) {
            *b = 0;
        }
        let isz = buf.len();
        if fs + extents.len() * 16 > isz {
            return Err(FilesystemError::Unsupported(
                "directory inline fork can't hold this many extents".into(),
            ));
        }
        for (i, &(startoff, fsblock, count)) in extents.iter().enumerate() {
            let rec = encode_extent(false, startoff, fsblock, count as u64);
            buf[fs + i * 16..fs + (i + 1) * 16].copy_from_slice(&rec);
        }
        self.write_inode_region(sb, ino, &buf)
    }

    /// Remove `name` from directory `parent_ino`, dispatching by format:
    /// short-form (inline rewrite) or single-block (rebuild in place, then
    /// attempt re-compaction back to short-form when the surviving entries
    /// fit the inode literal area — §2.1 hole (B)). A single-block
    /// directory whose entries still overflow the literal area stays in
    /// block form.
    fn dir_remove_entry(
        &mut self,
        sb: &super::sb::XfsSuperblock,
        parent_ino: u64,
        name: &str,
        child_was_dir: bool,
    ) -> Result<(), FilesystemError> {
        let (core, _buf) = self.read_inode_buf(parent_ino)?;
        match core.format {
            super::types::DiFormat::Local => {
                self.sf_remove_entry(sb, parent_ino, name, child_was_dir)
            }
            super::types::DiFormat::Extents => {
                self.block_remove_entry(sb, parent_ino, name, child_was_dir)?;
                self.try_recompact_block_dir_to_shortform(sb, parent_ino)
            }
            _ => Err(FilesystemError::Unsupported(
                "parent directory format not editable (btree/leaf/node)".into(),
            )),
        }
    }

    /// After a block-form directory removal, try to re-compact it back to
    /// short-form when the surviving entries fit the inode literal area
    /// (§2.1 hole (B), the inverse of [`convert_sf_dir_to_block`]). Builds the
    /// short-form fork bytes, frees the directory's data block(s) via
    /// [`free_blocks`], and rewrites the inode (`di_format=Local`,
    /// `di_size=sf_len`, `di_nblocks=0`, `di_nextents=0`). When recompaction
    /// isn't possible — entries don't fit, an inode number needs 8 bytes
    /// (`i8count`), or the directory has grown beyond a single block — the
    /// volume is left untouched and the directory stays in block form.
    /// Mirrors `xfs_dir2_block_to_sf`. v4 only.
    fn try_recompact_block_dir_to_shortform(
        &mut self,
        sb: &super::sb::XfsSuperblock,
        parent_ino: u64,
    ) -> Result<(), FilesystemError> {
        let has_ftype = sb.has_ftype();
        let (core, buf) = self.read_inode_buf(parent_ino)?;
        if !core.is_dir() || core.format != super::types::DiFormat::Extents {
            return Ok(());
        }
        let extents = self.decode_data_extents(&core, &buf)?;
        // Only a single-block directory (one extent at file-offset 0) is in
        // scope; multi-block leaf/node dirs go through their own re-compaction
        // path once hole (C) lands.
        if extents.len() != 1 {
            return Ok(());
        }
        let first = &extents[0];
        if first.startoff != 0 {
            return Ok(());
        }
        let dirblksize = sb.dirblksize() as usize;
        let blocks_per_dir = (dirblksize as u64).div_ceil(sb.blocksize as u64) as u32;
        let fsblock = first.startblock;
        let (dotdot, entries) = self.read_block_dir_entries(sb, fsblock, dirblksize, has_ftype)?;

        // 8-byte-inode short-form (`i8count > 0`) is not implemented; if any
        // inode in scope needs 8 bytes, leave the directory in block form.
        if parent_ino > u64::from(u32::MAX)
            || dotdot > u64::from(u32::MAX)
            || entries.iter().any(|(_, ino, _)| *ino > u64::from(u32::MAX))
        {
            return Ok(());
        }

        // Short-form size: header(count(1) + i8count(1) + parent(4)) + sum
        // over entries of (namelen(1) + offset(2) + name + [ftype] + ino(4)).
        let ino_width = 4usize;
        let header = 2 + ino_width;
        let entries_size: usize = entries
            .iter()
            .map(|(n, _, _)| 1 + 2 + n.len() + usize::from(has_ftype) + ino_width)
            .sum();
        let sf_size = header + entries_size;

        // Available literal area: fork start (`fork_offset(false)`) up to
        // either the attr-fork base (when `forkoff > 0`) or the inode end.
        let fs = sb.fork_offset();
        let avail_end = if core.forkoff > 0 {
            fs + (core.forkoff as usize) * 8
        } else {
            buf.len()
        };
        if fs + sf_size > avail_end {
            return Ok(()); // doesn't fit; stay in block form
        }

        // Build the short-form fork bytes. Each entry's offset cookie is its
        // byte position in the notional dir2 data block — starting after the
        // synthetic "." and ".." and advancing by each entry's data-block
        // size, matching how `sf_insert_entry` extends the fork.
        let mut sf_fork = vec![0u8; sf_size];
        sf_fork[0] = entries.len() as u8; // count
        sf_fork[1] = 0; // i8count
        BigEndian::write_u32(&mut sf_fork[2..6], dotdot as u32);

        let mut data_off = XFS_DIR2_DATA_HDR_LEN
            + dir2_data_entsize(1, has_ftype)  // "."
            + dir2_data_entsize(2, has_ftype); // ".."
        let mut pos = header;
        for (name, ino, ft) in &entries {
            let namelen = name.len();
            sf_fork[pos] = namelen as u8;
            BigEndian::write_u16(&mut sf_fork[pos + 1..pos + 3], data_off as u16);
            pos += 3;
            sf_fork[pos..pos + namelen].copy_from_slice(name.as_bytes());
            pos += namelen;
            if has_ftype {
                sf_fork[pos] = *ft;
                pos += 1;
            }
            BigEndian::write_u32(&mut sf_fork[pos..pos + 4], *ino as u32);
            pos += 4;
            data_off += dir2_data_entsize(namelen, has_ftype);
        }
        debug_assert_eq!(pos, sf_size);

        // Return the block dir's data blocks to free space.
        self.free_blocks(sb, &[(fsblock, blocks_per_dir)])?;

        // Rewrite the inode as a short-form directory. Zero the literal area
        // first so stale block-fork bytes can't leak through.
        let (_core_now, mut ibuf) = self.read_inode_buf(parent_ino)?;
        ibuf[5] = 1; // di_format = local
        BigEndian::write_u64(&mut ibuf[56..64], sf_size as u64); // di_size
        BigEndian::write_u64(&mut ibuf[64..72], 0); // di_nblocks
        BigEndian::write_u32(&mut ibuf[76..80], 0); // di_nextents
        for b in ibuf.iter_mut().take(avail_end).skip(fs) {
            *b = 0;
        }
        ibuf[fs..fs + sf_size].copy_from_slice(&sf_fork);
        self.write_inode_region(sb, parent_ino, &ibuf)
    }

    /// Remove `name` from a single-block directory by rebuilding the block
    /// without it and writing it back in place. Leaf-form (multi-extent)
    /// directories are rejected with a clear error — remove from leaf form is
    /// a separate slice once §2.1 hole (C) ships growing-insert + recompaction.
    fn block_remove_entry(
        &mut self,
        sb: &super::sb::XfsSuperblock,
        parent_ino: u64,
        name: &str,
        child_was_dir: bool,
    ) -> Result<(), FilesystemError> {
        let has_ftype = sb.has_ftype();
        let (core, buf) = self.read_inode_buf(parent_ino)?;
        let extents = self.decode_data_extents(&core, &buf)?;
        let first = extents
            .first()
            .ok_or_else(|| FilesystemError::Parse("block dir has no extent".into()))?;
        if first.startoff != 0 {
            return Err(FilesystemError::Unsupported(
                "multi-block directory not editable".into(),
            ));
        }
        if extents.len() > 1 {
            return Err(FilesystemError::Unsupported(
                "remove from leaf-form directory not yet implemented".into(),
            ));
        }
        let dirblksize = sb.dirblksize() as usize;
        let fsblock = first.startblock;
        let (dotdot, mut entries) =
            self.read_block_dir_entries(sb, fsblock, dirblksize, has_ftype)?;
        let before = entries.len();
        entries.retain(|(n, _, _)| n != name);
        if entries.len() == before {
            return Err(FilesystemError::NotFound(name.into()));
        }
        let mut block = build_block_dir(
            &entries,
            parent_ino,
            dotdot,
            dirblksize,
            has_ftype,
            sb.is_v5(),
        )?;
        if sb.is_v5() {
            let blkno = super::v5_crc::fsblock_to_daddr(fsblock, sb);
            super::v5_crc::stamp_dir3_blk_hdr(&mut block, blkno, parent_ino, sb);
        }
        self.write_dir_block(sb, fsblock, &block)?;
        if child_was_dir {
            self.bump_dir_nlink(sb, parent_ino, -1)?;
        }
        Ok(())
    }

    /// Collect a directory's entries `(name, ino, ftype)` (excluding `.`/`..`)
    /// plus the `..` inode, from a short-form or single-block directory.
    fn gather_dir_entries(
        &mut self,
        sb: &super::sb::XfsSuperblock,
        core: &super::inode::XfsDinodeCore,
        buf: &[u8],
        parent_ino: u64,
    ) -> Result<(u64, DirEntries), FilesystemError> {
        let has_ftype = sb.has_ftype();
        match core.format {
            super::types::DiFormat::Local => {
                let fs = sb.fork_offset();
                let fork = &buf[fs..fs + core.size as usize];
                Ok(sf_entries_with_ftype(fork, has_ftype)?)
            }
            super::types::DiFormat::Extents => {
                let extents = self.decode_data_extents(core, buf)?;
                let first = extents
                    .first()
                    .ok_or_else(|| FilesystemError::Parse("block dir has no extent".into()))?;
                let dirblksize = sb.dirblksize() as usize;
                self.read_block_dir_entries(sb, first.startblock, dirblksize, has_ftype)
            }
            _ => {
                let _ = parent_ino;
                Err(FilesystemError::Unsupported(
                    "parent directory format not editable".into(),
                ))
            }
        }
    }

    /// Read a single-block directory's entries `(name, ino, ftype)` (skipping
    /// `.`/`..`) and the `..` inode.
    fn read_block_dir_entries(
        &mut self,
        sb: &super::sb::XfsSuperblock,
        fsblock: u64,
        dirblksize: usize,
        has_ftype: bool,
    ) -> Result<(u64, DirEntries), FilesystemError> {
        let mut block = vec![0u8; dirblksize];
        let bs = sb.blocksize as u64;
        for i in 0..(dirblksize as u64 / bs) {
            let part =
                fsblock_to_partition_byte(fsblock + i, sb.agblocks, sb.agblklog, sb.blocksize);
            read_at_aligned(
                &mut self.reader,
                self.partition_offset + part,
                bs,
                &mut block[(i * bs) as usize..((i + 1) * bs) as usize],
            )?;
        }
        block_entries_with_ftype(&block, has_ftype)
    }

    /// Write `block` bytes across the directory block's fsblock(s).
    fn write_dir_block(
        &mut self,
        sb: &super::sb::XfsSuperblock,
        fsblock: u64,
        block: &[u8],
    ) -> Result<(), FilesystemError> {
        use std::io::SeekFrom;
        let bs = sb.blocksize as usize;
        for (i, chunk) in block.chunks(bs).enumerate() {
            let part = fsblock_to_partition_byte(
                fsblock + i as u64,
                sb.agblocks,
                sb.agblklog,
                sb.blocksize,
            );
            self.reader
                .seek(SeekFrom::Start(self.partition_offset + part))?;
            self.reader.write_all(chunk)?;
        }
        Ok(())
    }

    /// Rewrite a directory inode to extents format with a single data extent
    /// `(fsblock, blocks)` of size `dirblksize`. Bumps nlink when `add_subdir`.
    fn set_dir_inode_single_extent(
        &mut self,
        sb: &super::sb::XfsSuperblock,
        ino: u64,
        fsblock: u64,
        blocks: u32,
        dirblksize: usize,
        add_subdir: bool,
    ) -> Result<(), FilesystemError> {
        let (_core, mut buf) = self.read_inode_buf(ino)?;
        buf[5] = 2; // di_format = extents
        BigEndian::write_u64(&mut buf[56..64], dirblksize as u64); // di_size
        BigEndian::write_u64(&mut buf[64..72], blocks as u64); // di_nblocks
        BigEndian::write_u32(&mut buf[76..80], 1); // di_nextents
        if add_subdir {
            let version = buf[4];
            if version == 1 {
                let n = BigEndian::read_u16(&buf[6..8]).wrapping_add(1);
                BigEndian::write_u16(&mut buf[6..8], n);
            } else {
                let n = BigEndian::read_u32(&buf[16..20]).wrapping_add(1);
                BigEndian::write_u32(&mut buf[16..20], n);
            }
        }
        let fs = sb.fork_offset();
        for b in buf.iter_mut().skip(fs) {
            *b = 0;
        }
        let rec = encode_extent(false, 0, fsblock, blocks as u64);
        buf[fs..fs + 16].copy_from_slice(&rec);
        self.write_inode_region(sb, ino, &buf)
    }

    /// Bump a directory inode's link count by `delta` (version-correct field).
    fn bump_dir_nlink(
        &mut self,
        sb: &super::sb::XfsSuperblock,
        ino: u64,
        delta: i32,
    ) -> Result<(), FilesystemError> {
        let (_core, mut buf) = self.read_inode_buf(ino)?;
        let version = buf[4];
        if version == 1 {
            let n = (BigEndian::read_u16(&buf[6..8]) as i32 + delta).max(0) as u16;
            BigEndian::write_u16(&mut buf[6..8], n);
        } else {
            let n = (BigEndian::read_u32(&buf[16..20]) as i32 + delta).max(0) as u32;
            BigEndian::write_u32(&mut buf[16..20], n);
        }
        self.write_inode_region(sb, ino, &buf)
    }

    // ----- R7 orphan reconnection -------------------------------------------

    /// R7 (reconnection half): link every allocated-but-unreachable inode into
    /// `/lost+found` (created under the root if absent), naming each by its
    /// inode number. A reconnected subdirectory has its `..` repointed at
    /// `lost+found`; a detached *subtree* reconnects only at its top (children
    /// that are themselves reachable from an orphan are left in place). Link
    /// counts are not adjusted here — the R7 nlink pass runs afterwards and
    /// recomputes them from the now-connected tree. v4 only.
    pub(crate) fn run_orphan_reconnect(&mut self) -> Result<RepairReport, FilesystemError> {
        let mut report = RepairReport {
            fixes_applied: Vec::new(),
            fixes_failed: Vec::new(),
            unrepairable_count: 0,
        };
        let sb = self.superblock().clone();
        // §2.1 (E.5d): v5 lifted. Reconnection creates a `/lost+found`
        // directory entry via the same `dir_insert_entry` /
        // `init_empty_shortform_dir` pipeline `do_create_directory` uses;
        // both now stamp dir3 / v3 inode core on v5.
        let root = match self.root() {
            Ok(r) => r,
            Err(_) => return Ok(report),
        };

        // Reachability BFS from the root + superblock-internal inodes. If any
        // directory can't be listed the reachable set is incomplete, so abort
        // (reconnecting then would wrongly relink genuinely-reachable inodes).
        use std::collections::HashSet;
        let mut reachable: HashSet<u64> = sb.internal_inodes().into_iter().collect();
        reachable.insert(root.location);
        let mut visited: HashSet<u64> = HashSet::new();
        let mut queue = vec![root.clone()];
        // child -> true if it's referenced by some directory we visited that is
        // itself an orphan; used to keep detached subtrees intact.
        while let Some(dir) = queue.pop() {
            if !visited.insert(dir.location) {
                continue;
            }
            let children = match self.list_directory(&dir) {
                Ok(c) => c,
                Err(_) => return Ok(report),
            };
            for child in children {
                reachable.insert(child.location);
                if child.is_directory() {
                    queue.push(child);
                }
            }
        }

        let allocated = self.enumerate_allocated_inodes(&sb);
        let mut orphans: Vec<u64> = allocated
            .into_iter()
            .filter(|i| !reachable.contains(i))
            .collect();
        orphans.sort_unstable();
        if orphans.is_empty() {
            return Ok(report);
        }
        let orphan_set: HashSet<u64> = orphans.iter().copied().collect();

        // Keep detached subtrees intact: an orphan that is a child of another
        // orphan directory reconnects via that parent, not directly.
        let mut child_of_orphan: HashSet<u64> = HashSet::new();
        for &o in &orphans {
            let core = match self.read_inode(o) {
                Ok(c) => c,
                Err(_) => continue,
            };
            if !core.is_dir() {
                continue;
            }
            let entry = match self.child_entry("/", format!("orphan_{o}"), o) {
                Ok(e) => e,
                Err(_) => continue,
            };
            if let Ok(children) = self.list_directory(&entry) {
                for c in children {
                    if orphan_set.contains(&c.location) {
                        child_of_orphan.insert(c.location);
                    }
                }
            }
        }
        let roots: Vec<u64> = orphans
            .iter()
            .copied()
            .filter(|o| !child_of_orphan.contains(o))
            .collect();
        if roots.is_empty() {
            return Ok(report);
        }

        let lf_ino = match self.ensure_lost_found(&sb, root.location) {
            Ok(i) => i,
            Err(e) => {
                report
                    .fixes_failed
                    .push(format!("could not create lost+found: {e}"));
                return Ok(report);
            }
        };

        let mut count = 0u64;
        for o in roots {
            if o == lf_ino {
                continue;
            }
            let is_dir = self.read_inode(o).map(|c| c.is_dir()).unwrap_or(false);
            let name = format!("{o}");
            match self.dir_insert_entry(&sb, lf_ino, &name, o, is_dir) {
                Ok(()) => {
                    if is_dir {
                        if let Err(e) = self.set_dotdot(&sb, o, lf_ino) {
                            report
                                .fixes_failed
                                .push(format!("inode {o}: reconnected but '..' fix failed: {e}"));
                        }
                    }
                    count += 1;
                }
                Err(e) => report
                    .fixes_failed
                    .push(format!("inode {o}: reconnect failed: {e}")),
            }
        }
        if count > 0 {
            self.reader.flush()?;
            report
                .fixes_applied
                .push(format!("reconnected {count} orphan(s) into /lost+found"));
        }
        Ok(report)
    }

    /// Find `/lost+found` under the root (must be a directory) or create it.
    fn ensure_lost_found(
        &mut self,
        sb: &super::sb::XfsSuperblock,
        root_ino: u64,
    ) -> Result<u64, FilesystemError> {
        let root = self.root()?;
        if let Ok(children) = self.list_directory(&root) {
            if let Some(e) = children.iter().find(|e| e.name == "lost+found") {
                if e.is_directory() {
                    return Ok(e.location);
                }
            }
        }
        // Create it: a fresh empty short-form directory linked into the root.
        self.dir_can_insert(sb, root_ino, "lost+found", true)?;
        let ino = self.alloc_inode_slot(sb)?;
        self.init_empty_shortform_dir(sb, ino, root_ino, 0o040755, 0, 0)?;
        self.dir_insert_entry(sb, root_ino, "lost+found", ino, true)?;
        Ok(ino)
    }

    /// Repoint a directory's `..` at `new_parent` (short-form header parent, or
    /// the `..` data entry of a single-block directory).
    fn set_dotdot(
        &mut self,
        sb: &super::sb::XfsSuperblock,
        ino: u64,
        new_parent: u64,
    ) -> Result<(), FilesystemError> {
        let (core, mut buf) = self.read_inode_buf(ino)?;
        match core.format {
            super::types::DiFormat::Local => {
                let fs = sb.fork_offset();
                if buf[fs + 1] > 0 {
                    BigEndian::write_u64(&mut buf[fs + 2..fs + 10], new_parent);
                } else {
                    BigEndian::write_u32(&mut buf[fs + 2..fs + 6], new_parent as u32);
                }
                self.write_inode_region(sb, ino, &buf)
            }
            super::types::DiFormat::Extents => {
                let extents = self.decode_data_extents(&core, &buf)?;
                let first = extents
                    .first()
                    .ok_or_else(|| FilesystemError::Parse("block dir has no extent".into()))?;
                let dirblksize = sb.dirblksize() as usize;
                let mut block = vec![0u8; dirblksize];
                let bs = sb.blocksize as u64;
                for i in 0..(dirblksize as u64 / bs) {
                    let part = fsblock_to_partition_byte(
                        first.startblock + i,
                        sb.agblocks,
                        sb.agblklog,
                        sb.blocksize,
                    );
                    read_at_aligned(
                        &mut self.reader,
                        self.partition_offset + part,
                        bs,
                        &mut block[(i * bs) as usize..((i + 1) * bs) as usize],
                    )?;
                }
                // Find the ".." data entry and rewrite its inumber.
                let has_ftype = sb.has_ftype();
                let block_end = block.len();
                let tail = block_end - 8;
                let leaf_count = BigEndian::read_u32(&block[tail..tail + 4]) as usize;
                let data_end = block_end - (leaf_count * 8 + 8);
                let mut pos = XFS_DIR2_DATA_HDR_LEN;
                let mut done = false;
                while pos + 4 <= data_end {
                    if BigEndian::read_u16(&block[pos..pos + 2]) == 0xFFFF {
                        let len = BigEndian::read_u16(&block[pos + 2..pos + 4]) as usize;
                        if len == 0 {
                            break;
                        }
                        pos += len;
                        continue;
                    }
                    let namelen = block[pos + 8] as usize;
                    if &block[pos + 9..pos + 9 + namelen] == b".." {
                        BigEndian::write_u64(&mut block[pos..pos + 8], new_parent);
                        done = true;
                        break;
                    }
                    pos += dir2_data_entsize(namelen, has_ftype);
                }
                if !done {
                    return Err(FilesystemError::Parse(
                        "'..' entry not found in block dir".into(),
                    ));
                }
                self.write_dir_block(sb, first.startblock, &block)
            }
            _ => Err(FilesystemError::Unsupported(
                "cannot repoint '..' of a btree/leaf/node directory".into(),
            )),
        }
    }

    /// Every allocated inode number on the volume (walk every AG's inode btree).
    fn enumerate_allocated_inodes(&mut self, sb: &super::sb::XfsSuperblock) -> Vec<u64> {
        let agblocks = sb.agblocks as u64;
        let blocksize = sb.blocksize as u64;
        let sectsize = sb.sectsize as u64;
        let bs = sb.blocksize as usize;
        let ino_shift = sb.agblklog + sb.inopblog;
        let mut out = Vec::new();
        for agno in 0..sb.agcount as u64 {
            let agi_byte = self.partition_offset + agno * agblocks * blocksize + 2 * sectsize;
            let mut agi_sec = vec![0u8; sectsize as usize];
            if read_at_aligned(&mut self.reader, agi_byte, sectsize, &mut agi_sec).is_err() {
                continue;
            }
            let agi = match XfsAgi::parse(&agi_sec) {
                Ok(a) => a,
                Err(_) => continue,
            };
            let leaves = match self.collect_inobt_leaf_blocks(sb, agno, agi.root) {
                Ok(l) => l,
                Err(_) => continue,
            };
            let mut block = vec![0u8; bs];
            let hdr_len = sb.sblock_hdr_len();
            for leaf_agbno in leaves {
                let fsblock = (agno << sb.agblklog) | leaf_agbno as u64;
                if self.read_fsblock(fsblock, &mut block).is_err() {
                    continue;
                }
                let numrecs = BigEndian::read_u16(&block[6..8]) as usize;
                let max_leaf = (bs - hdr_len) / INOBT_REC_SIZE;
                if numrecs > max_leaf {
                    continue;
                }
                for r in 0..numrecs {
                    let off = hdr_len + r * INOBT_REC_SIZE;
                    let start_agino = BigEndian::read_u32(&block[off..off + 4]) as u64;
                    let free = BigEndian::read_u64(&block[off + 8..off + 16]);
                    for slot in 0..XFS_INODES_PER_CHUNK as u64 {
                        if (free >> slot) & 1 == 0 {
                            out.push((agno << ino_shift) | (start_agino + slot));
                        }
                    }
                }
            }
        }
        out
    }
}

/// XFS directory name hash (`xfs_da_hashname`) — used to build the leaf hash
/// index of a single-block directory.
fn dir_hashname(name: &[u8]) -> u32 {
    let mut hash: u32 = 0;
    let mut chunks = name.chunks_exact(4);
    for c in &mut chunks {
        hash = ((c[0] as u32) << 21)
            ^ ((c[1] as u32) << 14)
            ^ ((c[2] as u32) << 7)
            ^ (c[3] as u32)
            ^ hash.rotate_left(7 * 4);
    }
    let rem = chunks.remainder();
    match rem.len() {
        3 => {
            ((rem[0] as u32) << 14)
                ^ ((rem[1] as u32) << 7)
                ^ (rem[2] as u32)
                ^ hash.rotate_left(7 * 3)
        }
        2 => ((rem[0] as u32) << 7) ^ (rem[1] as u32) ^ hash.rotate_left(7 * 2),
        1 => (rem[0] as u32) ^ hash.rotate_left(7),
        _ => hash,
    }
}

/// Data-block size one directory entry occupies: `inumber(8) + namelen(1) +
/// name + [ftype] + tag(2)`, rounded up to 8.
fn dir2_data_entsize(namelen: usize, has_ftype: bool) -> usize {
    let raw = 8 + 1 + namelen + usize::from(has_ftype) + 2;
    raw.div_ceil(8) * 8
}

/// Maximum extent records in one bmap btree leaf block — what fits after
/// the long-form `xfs_btree_block` header. v5 has 48 fewer payload bytes.
fn bmbt_leaf_max(sb: &super::sb::XfsSuperblock) -> usize {
    (sb.blocksize as usize - sb.lblock_hdr_len()) / BMBT_REC_SIZE
}

/// Convert raw `(fsblock, count)` allocation runs into bmap extent records
/// `(startoff, fsblock, count)`, splitting any run whose `count` exceeds the
/// 21-bit on-disk `blockcount` field into multiple consecutive records that
/// describe the same physical blocks. File offsets advance sequentially over
/// the runs (the data-fork layout assumed by every other write path here).
fn runs_to_extent_records(runs: &[(u64, u32)]) -> Vec<(u64, u64, u32)> {
    let mut out: Vec<(u64, u64, u32)> = Vec::with_capacity(runs.len());
    let mut file_off = 0u64;
    for &(fsblock, count) in runs {
        let mut remaining = count as u64;
        let mut start_fsblock = fsblock;
        while remaining > 0 {
            let chunk = remaining.min(MAX_SINGLE_EXTENT_BLOCKS);
            out.push((file_off, start_fsblock, chunk as u32));
            file_off += chunk;
            start_fsblock += chunk;
            remaining -= chunk;
        }
    }
    out
}

/// Build a bmap btree leaf block holding every extent record; sibling
/// pointers are `NULLFSBLOCK` since hole (D) only supports a single leaf.
/// The caller has guaranteed `extents.len() <= bmbt_leaf_max(sb)`.
///
/// v4 (`XFS_BMAP_MAGIC`): 24-byte hdr (magic + level + numrecs + leftsib +
/// rightsib), records pack from byte 24.
///
/// v5 (`XFS_BMAP_CRC_MAGIC`): 72-byte hdr — the v4 prefix at bytes 0..24,
/// then `bb_blkno`/`bb_lsn`/`bb_uuid`/`bb_owner`/`bb_crc` at bytes 24..72.
/// The caller stamps the CRC tuple (`v5_crc::stamp_lblock_crc_header`)
/// after this builder returns, once the leaf's destination fsblock is
/// known.
fn build_bmbt_leaf(extents: &[(u64, u64, u32)], sb: &super::sb::XfsSuperblock) -> Vec<u8> {
    let bs = sb.blocksize as usize;
    let mut buf = vec![0u8; bs];
    let magic = if sb.is_v5() {
        super::bmap::XFS_BMAP_CRC_MAGIC
    } else {
        XFS_BMAP_MAGIC
    };
    BigEndian::write_u32(&mut buf[0..4], magic);
    BigEndian::write_u16(&mut buf[4..6], 0); // level 0 = leaf
    BigEndian::write_u16(&mut buf[6..8], extents.len() as u16);
    BigEndian::write_u64(&mut buf[8..16], NULLFSBLOCK); // leftsib
    BigEndian::write_u64(&mut buf[16..24], NULLFSBLOCK); // rightsib
    let hdr = sb.lblock_hdr_len();
    for (i, &(off, fsblock, count)) in extents.iter().enumerate() {
        let rec = encode_extent(false, off, fsblock, count as u64);
        let pos = hdr + i * BMBT_REC_SIZE;
        buf[pos..pos + BMBT_REC_SIZE].copy_from_slice(&rec);
    }
    buf
}

/// Write the in-inode `xfs_bmdr_block` root pointing at a single leaf. Layout:
/// `level(2) + numrecs(2)` then `maxrecs` keys (each 8 bytes — `startoff`) then
/// `maxrecs` pointers (each 8 bytes — leaf fsblock). Only the level-1 root
/// case with `numrecs=1` is in scope. `maxrecs = (fork_len - 4) / 16` matches
/// the read-side `parse_bmbt_root`.
fn write_bmbt_root_to_leaf(fork: &mut [u8], first_startoff: u64, leaf_fsblock: u64) {
    BigEndian::write_u16(&mut fork[0..2], 1); // level 1 (above leaves)
    BigEndian::write_u16(&mut fork[2..4], 1); // numrecs
    let maxrecs = (fork.len().saturating_sub(4)) / 16;
    BigEndian::write_u64(&mut fork[4..12], first_startoff);
    let ptrs_off = 4 + maxrecs * 8;
    BigEndian::write_u64(&mut fork[ptrs_off..ptrs_off + 8], leaf_fsblock);
}

/// XFS dir2 leaf1 block magic (`xfs_da_blkinfo.magic` for a single-leaf
/// directory). Stored as a 16-bit big-endian value at byte 8 of the block.
const XFS_DIR2_LEAF1_MAGIC: u16 = 0xD2F1;
/// XFS dir3 leaf1 block magic (`xfs_da3_blkinfo.magic` for a v5 single-leaf
/// directory). Same byte position as v4 (offset 8..10 of the block).
const XFS_DIR3_LEAF1_MAGIC: u16 = 0x3DF1;

/// dir2 leaf address space starts at byte 32 GiB in the inode's file offset
/// stream (`XFS_DIR2_LEAF_OFFSET`). Converted to fsblocks: `2^32 / blocksize`.
fn dir2_leaf_offset_fb(blocksize: u32) -> u64 {
    (1u64 << 32) / (blocksize as u64)
}

/// A built dir2 XD2D data block paired with its leaf-index contributions:
/// the block's on-disk bytes, plus a `(hashval, address)` pair per entry the
/// leaf1 block needs to record. Address encoding: `(file_block_in_dir *
/// dirblksize + byte_off) / 8` — the standard dir2 dataptr.
struct DataBlockWithIndex {
    bytes: Vec<u8>,
    leaf_index: Vec<(u32, u32)>,
}

/// Build an XD2D (v4) / XDD3 (v5) directory data block from a sub-slice of
/// entries that belong in this data block. The block's file-offset in dir2
/// address space is `file_block_in_dir` (0 for the first data block, 1 for
/// the next, …). Each entry contributes a `(hashval, address)` pair to the
/// returned leaf index; addresses are 8-byte dataptr offsets
/// (`(file_block * dirblksize + byte_off) / 8`). `extra_dot_dotdot` adds the
/// synthetic `.` and `..` entries (only for the first data block). The free
/// space at the tail is recorded as an unused record + `bestfree[0]`;
/// `bestfree[1..3]` stay zero.
///
/// **v5 (CRC)**: header is 64 bytes (`XFS_DIR3_DATA_HDR_LEN`), magic is
/// `XFS_DIR3_DATA_MAGIC`, and `bestfree[0]` sits at offset 48..52. The CRC
/// tuple is left zero — caller stamps it via
/// [`v5_crc::stamp_dir3_blk_hdr`].
#[allow(clippy::too_many_arguments)] // 8 args: v4 builder shape + is_v5 flag
fn build_leaf_data_block(
    entries: &[(String, u64, u8)],
    self_ino: u64,
    dotdot: u64,
    file_block_in_dir: u64,
    dirblksize: usize,
    has_ftype: bool,
    extra_dot_dotdot: bool,
    is_v5: bool,
) -> Result<DataBlockWithIndex, FilesystemError> {
    let mut all: Vec<(Vec<u8>, u64, u8)> = Vec::with_capacity(entries.len() + 2);
    if extra_dot_dotdot {
        all.push((b".".to_vec(), self_ino, XFS_DIR3_FT_DIR));
        all.push((b"..".to_vec(), dotdot, XFS_DIR3_FT_DIR));
    }
    for (n, ino, ft) in entries {
        all.push((n.as_bytes().to_vec(), *ino, *ft));
    }

    let data_size: usize = all
        .iter()
        .map(|(n, _, _)| dir2_data_entsize(n.len(), has_ftype))
        .sum();
    let data_start = if is_v5 {
        super::dir2::XFS_DIR3_DATA_HDR_LEN
    } else {
        XFS_DIR2_DATA_HDR_LEN
    };
    let bestfree0_off = if is_v5 { 48 } else { 4 };
    if data_start + data_size > dirblksize {
        return Err(FilesystemError::DiskFull(format!(
            "leaf-form data block overflow: {} entries need {} bytes, block is {dirblksize}",
            all.len(),
            data_start + data_size
        )));
    }

    let mut buf = vec![0u8; dirblksize];
    let magic = if is_v5 {
        super::dir2::XFS_DIR3_DATA_MAGIC
    } else {
        super::dir2::XFS_DIR2_DATA_MAGIC
    };
    BigEndian::write_u32(&mut buf[0..4], magic);

    let dirblksize_units = (dirblksize / 8) as u32;
    let block_base_units = file_block_in_dir as u32 * dirblksize_units;
    let mut leaf: Vec<(u32, u32)> = Vec::with_capacity(all.len());
    let mut pos = data_start;
    for (name, ino, ft) in &all {
        let namelen = name.len();
        let ent = dir2_data_entsize(namelen, has_ftype);
        BigEndian::write_u64(&mut buf[pos..pos + 8], *ino);
        buf[pos + 8] = namelen as u8;
        buf[pos + 9..pos + 9 + namelen].copy_from_slice(name);
        if has_ftype {
            buf[pos + 9 + namelen] = *ft;
        }
        BigEndian::write_u16(&mut buf[pos + ent - 2..pos + ent], pos as u16);
        leaf.push((dir_hashname(name), block_base_units + (pos / 8) as u32));
        pos += ent;
    }

    // Free region at the end (if any): freetag(0xFFFF) + length(2) + tag(2).
    let gap_off = pos;
    let gap_len = dirblksize - pos;
    if gap_len >= 8 {
        BigEndian::write_u16(&mut buf[gap_off..gap_off + 2], 0xFFFF);
        BigEndian::write_u16(&mut buf[gap_off + 2..gap_off + 4], gap_len as u16);
        BigEndian::write_u16(
            &mut buf[gap_off + gap_len - 2..gap_off + gap_len],
            gap_off as u16,
        );
        // bestfree[0] = (offset, length).
        BigEndian::write_u16(&mut buf[bestfree0_off..bestfree0_off + 2], gap_off as u16);
        BigEndian::write_u16(
            &mut buf[bestfree0_off + 2..bestfree0_off + 4],
            gap_len as u16,
        );
        // bestfree[1..3] left zero.
    }

    Ok(DataBlockWithIndex {
        bytes: buf,
        leaf_index: leaf,
    })
}

/// Build a dir2 (v4 `XD2F`) / dir3 (v5 `XD3F`) leaf1 block from the
/// per-data-block leaf-index entries (already collected by
/// `build_leaf_data_block` calls) and the `bests` array (one u16 per data
/// block: the largest free-record length in that block). v4 layout:
///
/// ```text
///   da_blkinfo:  forw(4) back(4) magic(2) pad(2)        =  12 bytes
///   ldlh:        count(2) stale(2)                      =   4 bytes
///   ents[count]: hashval(4) address(4) per slot         =   8 bytes each
///   ... free space ...
///   bests[bests_count]: u16 each, at the tail of the block
/// ```
///
/// v5 layout swaps the 12-byte `xfs_da_blkinfo` for the 56-byte
/// `xfs_da3_blkinfo` — same `magic` byte position (offset 8..10), but with
/// crc(4)+blkno(8)+lsn(8)+uuid(16)+owner(8) appended before `ldlh`. The
/// CRC tuple is left zero — caller stamps it via
/// [`v5_crc::stamp_da3_blkinfo`].
///
/// Entries are sorted by `hashval` ascending so name lookups can binary-search.
fn build_leaf1_block(
    mut entries: Vec<(u32, u32)>,
    bests: &[u16],
    dirblksize: usize,
    is_v5: bool,
) -> Result<Vec<u8>, FilesystemError> {
    let bests_bytes = bests.len() * 2;
    // Header = blkinfo + ldlh. v4: 12+4=16. v5: 56+4=60.
    let blkinfo_bytes = if is_v5 { 56 } else { 12 };
    let header_bytes = blkinfo_bytes + 4;
    let used = header_bytes + entries.len() * 8 + bests_bytes;
    if used > dirblksize {
        return Err(FilesystemError::DiskFull(format!(
            "leaf1 block overflow: needed {used} bytes, have {dirblksize}"
        )));
    }
    entries.sort_by_key(|&(h, _)| h);

    let mut buf = vec![0u8; dirblksize];
    // `magic` lives at offset 8..10 in both v4 (`xfs_da_blkinfo`) and v5
    // (`xfs_da3_blkinfo`). Everything else in the blkinfo (forw/back/pad +
    // v5 crc/blkno/lsn/uuid/owner) is left zero here; the v5 CRC stamp is a
    // post-build pass.
    let magic = if is_v5 {
        XFS_DIR3_LEAF1_MAGIC
    } else {
        XFS_DIR2_LEAF1_MAGIC
    };
    BigEndian::write_u16(&mut buf[8..10], magic);
    // ldlh: count, stale — sits immediately after the blkinfo.
    let ldlh_off = blkinfo_bytes;
    BigEndian::write_u16(&mut buf[ldlh_off..ldlh_off + 2], entries.len() as u16);
    BigEndian::write_u16(&mut buf[ldlh_off + 2..ldlh_off + 4], 0);
    // Entries packed from the end of the header.
    let entries_off = header_bytes;
    for (i, (h, a)) in entries.iter().enumerate() {
        let off = entries_off + i * 8;
        BigEndian::write_u32(&mut buf[off..off + 4], *h);
        BigEndian::write_u32(&mut buf[off + 4..off + 8], *a);
    }
    // Bests array at the very end of the block, in data-block index order.
    let bests_off = dirblksize - bests_bytes;
    for (i, &b) in bests.iter().enumerate() {
        BigEndian::write_u16(&mut buf[bests_off + i * 2..bests_off + i * 2 + 2], b);
    }
    Ok(buf)
}

/// Build a single-block (`XD2B` v4 / `XDB3` v5) directory holding `.`, `..`,
/// and `entries`. `self_ino` is the directory's own inode (its `.`),
/// `dotdot` its parent. Returns `DiskFull` if it doesn't fit one `dirblksize`
/// block.
///
/// **v5 (CRC)**: header is 64 bytes (`XFS_DIR3_DATA_HDR_LEN`) instead of 16,
/// magic is `XFS_DIR3_BLOCK_MAGIC`, and `bestfree[0]` sits at offset 48..52
/// of the block (after the 48-byte `xfs_dir3_blk_hdr`). The CRC tuple
/// (crc/blkno/lsn/uuid/owner) is left zero here — the caller stamps it via
/// [`v5_crc::stamp_dir3_blk_hdr`] once the destination fsblock is known.
fn build_block_dir(
    entries: &[(String, u64, u8)],
    self_ino: u64,
    dotdot: u64,
    dirblksize: usize,
    has_ftype: bool,
    is_v5: bool,
) -> Result<Vec<u8>, FilesystemError> {
    // Full entry list with synthetic "." and "..".
    let mut all: Vec<(&[u8], u64, u8)> = Vec::with_capacity(entries.len() + 2);
    all.push((b".", self_ino, XFS_DIR3_FT_DIR));
    all.push((b"..", dotdot, XFS_DIR3_FT_DIR));
    for (n, ino, ft) in entries {
        all.push((n.as_bytes(), *ino, *ft));
    }

    let leaf_count = all.len();
    let leaf_section = leaf_count * 8 + 8; // leaf entries + tail
    let data_size: usize = all
        .iter()
        .map(|(n, _, _)| dir2_data_entsize(n.len(), has_ftype))
        .sum();
    let data_start = if is_v5 {
        super::dir2::XFS_DIR3_DATA_HDR_LEN
    } else {
        XFS_DIR2_DATA_HDR_LEN
    };
    // bestfree[0].offset / .length live inside the data header. v4 packs the
    // bestfree triple right after the magic at byte 4..16; v5 packs it after
    // the 48-byte xfs_dir3_blk_hdr (bytes 48..60) with a 4-byte pad after.
    let bestfree0_off = if is_v5 { 48 } else { 4 };
    let leaf_start = dirblksize
        .checked_sub(leaf_section)
        .ok_or_else(|| FilesystemError::DiskFull("dir block too small".into()))?;
    if data_start + data_size > leaf_start {
        return Err(FilesystemError::DiskFull(
            "directory entries exceed one block (leaf/node format not implemented)".into(),
        ));
    }

    let mut buf = vec![0u8; dirblksize];
    let magic = if is_v5 {
        super::dir2::XFS_DIR3_BLOCK_MAGIC
    } else {
        super::dir2::XFS_DIR2_BLOCK_MAGIC
    };
    BigEndian::write_u32(&mut buf[0..4], magic);

    // Data entries from data_start; collect (hashval, address) for the leaf.
    let mut leaf: Vec<(u32, u32)> = Vec::with_capacity(leaf_count);
    let mut pos = data_start;
    for (name, ino, ft) in &all {
        let namelen = name.len();
        let ent = dir2_data_entsize(namelen, has_ftype);
        BigEndian::write_u64(&mut buf[pos..pos + 8], *ino);
        buf[pos + 8] = namelen as u8;
        buf[pos + 9..pos + 9 + namelen].copy_from_slice(name);
        if has_ftype {
            buf[pos + 9 + namelen] = *ft;
        }
        // Trailing tag = this entry's byte offset within the block.
        BigEndian::write_u16(&mut buf[pos + ent - 2..pos + ent], pos as u16);
        leaf.push((dir_hashname(name), (pos >> 3) as u32));
        pos += ent;
    }

    // Free gap between the data area and the leaf section (a multiple of 8,
    // since all the pieces are 8-aligned).
    let gap_off = pos;
    let gap_len = leaf_start - pos;
    if gap_len >= 8 {
        BigEndian::write_u16(&mut buf[gap_off..gap_off + 2], 0xFFFF); // free tag
        BigEndian::write_u16(&mut buf[gap_off + 2..gap_off + 4], gap_len as u16);
        // Unused record's trailing tag = its own offset.
        BigEndian::write_u16(
            &mut buf[gap_off + gap_len - 2..gap_off + gap_len],
            gap_off as u16,
        );
        // bestfree[0] points at this gap.
        BigEndian::write_u16(&mut buf[bestfree0_off..bestfree0_off + 2], gap_off as u16);
        BigEndian::write_u16(
            &mut buf[bestfree0_off + 2..bestfree0_off + 4],
            gap_len as u16,
        );
    }

    // Leaf hash index, sorted ascending by hashval.
    leaf.sort_by_key(|&(h, _)| h);
    for (i, (h, a)) in leaf.iter().enumerate() {
        let off = leaf_start + i * 8;
        BigEndian::write_u32(&mut buf[off..off + 4], *h);
        BigEndian::write_u32(&mut buf[off + 4..off + 8], *a);
    }
    // Tail: count, stale.
    let tail = dirblksize - 8;
    BigEndian::write_u32(&mut buf[tail..tail + 4], leaf_count as u32);
    BigEndian::write_u32(&mut buf[tail + 4..tail + 8], 0);

    Ok(buf)
}

/// Walk a short-form fork into `(parent_ino, [(name, ino, ftype)])`.
fn sf_entries_with_ftype(
    fork: &[u8],
    has_ftype: bool,
) -> Result<(u64, DirEntries), FilesystemError> {
    if fork.len() < 6 {
        return Err(FilesystemError::Parse("short-form fork too small".into()));
    }
    let count = fork[0] as usize;
    let i8count = fork[1];
    let ino_width = if i8count > 0 { 8 } else { 4 };
    let read_ino = |b: &[u8]| -> u64 {
        if ino_width == 8 {
            BigEndian::read_u64(b)
        } else {
            BigEndian::read_u32(b) as u64
        }
    };
    let parent = read_ino(&fork[2..2 + ino_width]);
    let mut out = Vec::with_capacity(count);
    let mut pos = 2 + ino_width;
    for _ in 0..count {
        let namelen = fork[pos] as usize;
        let name_off = pos + 3;
        let name_end = name_off + namelen;
        let ft = if has_ftype { fork[name_end] } else { 0 };
        let ino_off = name_end + usize::from(has_ftype);
        let ino = read_ino(&fork[ino_off..ino_off + ino_width]);
        out.push((
            String::from_utf8_lossy(&fork[name_off..name_end]).to_string(),
            ino,
            ft,
        ));
        pos = ino_off + ino_width;
    }
    Ok((parent, out))
}

/// Walk a single-block directory into `(dotdot_ino, [(name, ino, ftype)])`,
/// skipping `.`/`..`.
fn block_entries_with_ftype(
    block: &[u8],
    has_ftype: bool,
) -> Result<(u64, DirEntries), FilesystemError> {
    let block_end = block.len();
    let tail = block_end - 8;
    let leaf_count = BigEndian::read_u32(&block[tail..tail + 4]) as usize;
    let data_end = block_end - (leaf_count * 8 + 8);
    let mut out = Vec::new();
    let mut dotdot = 0u64;
    let mut pos = XFS_DIR2_DATA_HDR_LEN;
    while pos + 4 <= data_end {
        let freetag = BigEndian::read_u16(&block[pos..pos + 2]);
        if freetag == 0xFFFF {
            let len = BigEndian::read_u16(&block[pos + 2..pos + 4]) as usize;
            if len == 0 {
                break;
            }
            pos += len;
            continue;
        }
        let ino = BigEndian::read_u64(&block[pos..pos + 8]);
        let namelen = block[pos + 8] as usize;
        let name = &block[pos + 9..pos + 9 + namelen];
        let ft = if has_ftype {
            block[pos + 9 + namelen]
        } else {
            0
        };
        if name == b".." {
            dotdot = ino;
        } else if name != b"." {
            out.push((String::from_utf8_lossy(name).to_string(), ino, ft));
        }
        pos += dir2_data_entsize(namelen, has_ftype);
    }
    Ok((dotdot, out))
}

#[cfg(test)]
mod tests {
    //! Unit tests for the dir-block builders. The integration round-trip on
    //! the v5 fixture lives in `super::tests` (see
    //! `dir3_block_builder_round_trips_through_dir2_reader` etc.).
    use super::*;
    use crate::fs::xfs::dir2;

    /// Look up the `XfsSuperblock` constants we need to fabricate a
    /// stamper-ready superblock without an actual on-disk image.
    fn fake_v5_sb(meta_uuid_byte: u8) -> crate::fs::xfs::sb::XfsSuperblock {
        crate::fs::xfs::sb::XfsSuperblock {
            magicnum: 0x5846_5342,
            blocksize: 4096,
            dblocks: 0,
            rblocks: 0,
            rextents: 0,
            logstart: 0,
            logblocks: 0,
            rootino: 0,
            rbmino: 0,
            rsumino: 0,
            uquotino: 0,
            gquotino: 0,
            agblocks: 0,
            agcount: 0,
            versionnum: 0x0005,
            sectsize: 512,
            inodesize: 512,
            inopblock: 8,
            fname: [0; 12],
            blocklog: 12,
            sectlog: 9,
            inodelog: 9,
            inopblog: 3,
            agblklog: 16,
            dirblklog: 0,
            inoalignmt: 0,
            features2: 0,
            features_compat: 0,
            features_ro_compat: 0,
            features_incompat: 0,
            sb_uuid: [meta_uuid_byte; 16],
            sb_meta_uuid: [meta_uuid_byte; 16],
        }
    }

    /// §2.1 hole (E.3): `build_block_dir` with `is_v5=true` produces an XDB3
    /// header, the v5 stamper validates, and the dir2 reader (which already
    /// knows dir3) walks every entry back.
    #[test]
    fn build_block_dir_v5_round_trips_through_reader_after_stamp() {
        let sb = fake_v5_sb(0x77);
        let entries = vec![
            ("alpha".to_string(), 0x1100u64, super::XFS_DIR3_FT_REG_FILE),
            ("beta".to_string(), 0x1200u64, super::XFS_DIR3_FT_DIR),
            ("gamma".to_string(), 0x1300u64, super::XFS_DIR3_FT_REG_FILE),
        ];
        let dirblksize = 4096;
        let self_ino = 0xABCD;
        let dotdot = 0xBEEF;
        let has_ftype = true;
        let mut buf =
            build_block_dir(&entries, self_ino, dotdot, dirblksize, has_ftype, true).unwrap();

        // Magic = XDB3 (v5 single-block dir).
        assert_eq!(
            BigEndian::read_u32(&buf[0..4]),
            dir2::XFS_DIR3_BLOCK_MAGIC,
            "expected XDB3 magic"
        );

        // Stamp the xfs_dir3_blk_hdr — owner = parent / self_ino, blkno = some
        // synthetic daddr. After stamping, crc_valid must return true.
        let blkno = 0xDEAD_BEEFu64;
        crate::fs::xfs::v5_crc::stamp_dir3_blk_hdr(&mut buf, blkno, self_ino, &sb);
        assert!(
            crate::fs::xfs::v5_crc::crc_valid(&buf, crate::fs::xfs::v5_crc::DIR3_BLK_CRC_OFF),
            "dir3 block CRC mismatch after build+stamp"
        );

        // Round-trip via the dir2 reader, which dispatches by magic and
        // handles dir3's 64-byte header.
        let parsed = dir2::parse_block(&buf, has_ftype).expect("reader walks XDB3 block");
        let names: Vec<&str> = parsed.iter().map(|e| e.name.as_str()).collect();
        for n in ["alpha", "beta", "gamma"] {
            assert!(names.contains(&n), "missing entry {n} in {names:?}");
        }
    }

    /// `build_leaf_data_block` v5 — XDD3 magic, bestfree[0] at offset 48,
    /// stamper validates.
    #[test]
    fn build_leaf_data_block_v5_stamps_and_parses() {
        let sb = fake_v5_sb(0x88);
        let entries = vec![
            ("alpha".to_string(), 0x2100u64, super::XFS_DIR3_FT_REG_FILE),
            ("beta".to_string(), 0x2200u64, super::XFS_DIR3_FT_REG_FILE),
        ];
        let mut data = build_leaf_data_block(
            &entries, 0x222, // self_ino (only used when extra_dot_dotdot is true)
            0x111, // dotdot
            1,     // file_block_in_dir
            4096,  // dirblksize
            true,  // has_ftype
            false, // no `.`/`..` for non-first data block
            true,  // v5
        )
        .unwrap();
        let block = &mut data.bytes;
        assert_eq!(BigEndian::read_u32(&block[0..4]), dir2::XFS_DIR3_DATA_MAGIC);

        // bestfree[0] = (offset, length) at bytes 48..52 on v5.
        let bf_off = BigEndian::read_u16(&block[48..50]);
        let bf_len = BigEndian::read_u16(&block[50..52]);
        assert!(bf_off >= 64, "bestfree[0].offset should sit past v5 header");
        assert!(bf_len > 0, "bestfree[0].length should be the tail gap");

        let blkno = 0xCAFE_BABEu64;
        crate::fs::xfs::v5_crc::stamp_dir3_blk_hdr(block, blkno, 0x111, &sb);
        assert!(
            crate::fs::xfs::v5_crc::crc_valid(block, crate::fs::xfs::v5_crc::DIR3_BLK_CRC_OFF),
            "XDD3 CRC mismatch after stamp"
        );

        // dir2 reader's parse_data_block already handles the XDD3 / 64-byte
        // header — confirm both entries surface.
        let parsed = dir2::parse_data_block(block, true).expect("walk XDD3 data block");
        let names: Vec<&str> = parsed.iter().map(|e| e.name.as_str()).collect();
        for n in ["alpha", "beta"] {
            assert!(names.contains(&n), "missing entry {n} in {names:?}");
        }
    }

    /// `build_leaf1_block` v5 — magic 0x3DF1 at offset 8..10 in the
    /// `xfs_da3_blkinfo`, 56-byte header before the entries, stamper
    /// validates.
    #[test]
    fn build_leaf1_block_v5_stamps_at_da3_offset() {
        let sb = fake_v5_sb(0x99);
        let entries = vec![(0x1111, 0x100), (0x2222, 0x200), (0x3333, 0x300)];
        let bests = [256u16, 256u16];
        let mut buf = build_leaf1_block(entries, &bests, 4096, true).unwrap();

        // Magic at offset 8..10 = XFS_DIR3_LEAF1_MAGIC.
        assert_eq!(
            BigEndian::read_u16(&buf[8..10]),
            XFS_DIR3_LEAF1_MAGIC,
            "expected 0x3DF1 dir3 leaf1 magic"
        );
        // ldlh `count` lives at offset 56 (right after the 56-byte da3 hdr).
        assert_eq!(
            BigEndian::read_u16(&buf[56..58]),
            3,
            "leaf entry count should round-trip"
        );

        let blkno = 0xFEED_FACEu64;
        crate::fs::xfs::v5_crc::stamp_da3_blkinfo(&mut buf, blkno, 0x222, &sb);
        assert!(
            crate::fs::xfs::v5_crc::crc_valid(&buf, crate::fs::xfs::v5_crc::DA3_CRC_OFF),
            "da3_blkinfo CRC mismatch after stamp"
        );
    }
}
