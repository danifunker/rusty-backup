//! Blank-XFS creator — the write-side counterpart to the reader in `mod.rs`.
//!
//! Emits a v5/CRC filesystem with the conservative feature set our reader
//! already speaks: `ftype` on, `finobt` / `rmapbt` / `reflink` / `bigtime` /
//! `sparse` / `nrext64` off. Geometry is fixed at 4 KiB blocks, 512-byte
//! sectors and 512-byte inodes, which is what `mkfs.xfs` picks for every
//! disk we care about.
//!
//! Layout per allocation group, mirroring `mkfs.xfs`:
//!
//! | block | contents |
//! |---|---|
//! | 0 | superblock, AGF, AGI, AGFL (one 512-byte sector each) |
//! | 1 / 2 / 3 | bnobt / cntbt / inobt roots |
//! | 4..7 | the four blocks parked on the AGFL free list |
//! | then | the log (log AG only), then the root inode chunk (AG 0 only) |
//! | rest | one free extent, described by a single record in each alloc btree |
//!
//! Only populated regions are written, so the log body and the trailing free
//! space stay sparse. Validated against `xfsprogs` 6.6 `xfs_repair -n` — see
//! `scripts/xfs-oracle.sh`.

use byteorder::{BigEndian, ByteOrder};
use std::io::{Seek, SeekFrom, Write};

use super::btree_build::{build_alloc_btree, build_sblock_btree, FreeExtent};
use super::sb::XfsSuperblock;
use super::types::{NULLAGBLOCK, XFS_AGF_MAGIC, XFS_AGI_MAGIC, XFS_DINODE_MAGIC, XFS_SB_MAGIC};
use super::v5_crc;
use crate::fs::filesystem::FilesystemError;

const BLOCKSIZE: u32 = 4096;
const BLOCKLOG: u8 = 12;
const SECTSIZE: u32 = 512;
const SECTLOG: u8 = 9;
const INODESIZE: u32 = 512;
const INODELOG: u8 = 9;
const INOPBLOCK: u16 = 8;
const INOPBLOG: u8 = 3;

/// AGFL magic ("XAFL"); the free-list sector is the one AG header the reader
/// never parses, so the constant lives here rather than in `types`.
const XFS_AGFL_MAGIC: u32 = 0x5841_464C;

/// `sb_versionnum` for the filesystems we emit: v5 plus MOREBITS, DIRV2,
/// EXTFLG, LOGV2, ALIGN and NLINK — byte-identical to `mkfs.xfs` 6.6.
const VERSIONNUM: u16 = 0xB4A5;
/// `sb_features2`: CRC | PROJID32 | ATTR2 | LAZYSBCOUNT.
const FEATURES2: u32 = 0x0000_018A;
/// `sb_features_incompat`: FTYPE only.
const FEATURES_INCOMPAT: u32 = 0x0000_0001;

/// Inode-chunk start alignment in blocks (`sb_inoalignmt`), = the 16 KiB v5
/// inode cluster at 4 KiB blocks.
const INOALIGNMT: u32 = 4;
/// Inodes per chunk and the blocks they occupy at 512-byte inodes.
const CHUNK_INODES: u32 = 64;
const CHUNK_BLOCKS: u32 = CHUNK_INODES * INODESIZE / BLOCKSIZE;

/// Blocks each AG spends before any user data: block 0's header sector group
/// plus the three btree roots, then the four blocks parked on the AGFL.
const AG_RESERVED_BLOCKS: u32 = 4;
const AGFL_RESERVE: u32 = 4;
const FIRST_USABLE_AGBNO: u32 = AG_RESERVED_BLOCKS + AGFL_RESERVE;

/// `XFS_AG_MIN_BLOCKS` / `XFS_AG_MAX_BLOCKS` at 4 KiB blocks. The maximum is
/// `XFS_MAX_AG_BYTES` (1 TiB) over the block size, less one.
const AG_MIN_BLOCKS: u64 = 4096;
const AG_MAX_BLOCKS: u64 = (1 << 28) - 1;

/// Log bounds in blocks: the kernel's 10 MiB floor and 2 GiB ceiling.
const LOG_MIN_BLOCKS: u64 = 2560;
const LOG_MAX_BLOCKS: u64 = 524_288;

const NULLAGINO: u32 = 0xFFFF_FFFF;
/// The `oh_tid` `mkfs.xfs` stamps on the log's unmount record.
const UNMOUNT_TID: u32 = 0xB0C0_D0D0;

/// Computed geometry for one blank volume.
struct Layout {
    dblocks: u64,
    agblocks: u32,
    agcount: u32,
    agblklog: u8,
    logblocks: u32,
    log_agno: u32,
    log_agbno: u32,
    root_chunk_agbno: u32,
    rootino: u64,
}

impl Layout {
    fn logstart(&self) -> u64 {
        ((self.log_agno as u64) << self.agblklog) | self.log_agbno as u64
    }

    /// AG-relative block where this AG's single free extent begins.
    fn free_start(&self, agno: u32) -> u32 {
        let mut next = FIRST_USABLE_AGBNO;
        if agno == self.log_agno {
            next += self.logblocks;
        }
        if agno == 0 {
            next = self.root_chunk_agbno + CHUNK_BLOCKS;
        }
        next
    }

    fn free_len(&self, agno: u32) -> u32 {
        self.agblocks.saturating_sub(self.free_start(agno))
    }
}

fn log2_roundup(v: u64) -> u8 {
    let mut l = 0u8;
    while (1u64 << l) < v {
        l += 1;
    }
    l
}

fn plan(size_bytes: u64) -> Result<Layout, FilesystemError> {
    let total_blocks = size_bytes / BLOCKSIZE as u64;
    // Two AGs is the floor: `xfs_repair` refuses to validate the geometry of a
    // single-AG filesystem without `-o force_geometry`.
    if total_blocks < 2 * AG_MIN_BLOCKS {
        return Err(FilesystemError::Unsupported(format!(
            "XFS needs at least {} bytes, got {size_bytes}",
            2 * AG_MIN_BLOCKS * BLOCKSIZE as u64
        )));
    }

    // Four AGs is the mkfs.xfs default; grow for huge disks, shrink so a small
    // one still clears XFS_AG_MIN_BLOCKS.
    let mut agcount = 4u64.max(total_blocks.div_ceil(AG_MAX_BLOCKS));
    while agcount > 2 && total_blocks / agcount < AG_MIN_BLOCKS {
        agcount -= 1;
    }
    let agblocks = total_blocks / agcount;
    // Equal-sized AGs: trimming at most agcount-1 blocks beats special-casing a
    // runt tail AG everywhere downstream.
    let dblocks = agblocks * agcount;

    let logblocks = (dblocks / 2048).clamp(LOG_MIN_BLOCKS, LOG_MAX_BLOCKS);
    let log_agno = (agcount / 2) as u32;
    let log_agbno = FIRST_USABLE_AGBNO;

    let root_chunk_agbno = {
        let after_log = if log_agno == 0 {
            FIRST_USABLE_AGBNO + logblocks as u32
        } else {
            FIRST_USABLE_AGBNO
        };
        after_log.next_multiple_of(INOALIGNMT)
    };

    let layout = Layout {
        dblocks,
        agblocks: agblocks as u32,
        agcount: agcount as u32,
        agblklog: log2_roundup(agblocks),
        logblocks: logblocks as u32,
        log_agno,
        log_agbno,
        root_chunk_agbno,
        rootino: (root_chunk_agbno as u64) << INOPBLOG,
    };

    // Every AG must keep at least one free block after its metadata, which is
    // what actually sets the practical minimum volume size.
    for agno in 0..layout.agcount {
        if layout.free_len(agno) == 0 {
            return Err(FilesystemError::Unsupported(format!(
                "XFS volume of {size_bytes} bytes is too small: allocation group \
                 {agno} has no free space after its metadata and log"
            )));
        }
    }
    Ok(layout)
}

/// A best-effort unique 16-byte UUID (version-4 shape), as elsewhere in the
/// tree — uniqueness is nice to have, not load-bearing.
fn make_uuid() -> [u8; 16] {
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos();
    let mut u = nanos.to_le_bytes();
    u[6] = (u[6] & 0x0F) | 0x40;
    u[8] = (u[8] & 0x3F) | 0x80;
    u
}

/// Build one superblock sector. Only the primary carries live counters; the
/// per-AG copies keep `sb_inprogress` set, as `mkfs.xfs` leaves them.
fn build_superblock(l: &Layout, label: &str, uuid: &[u8; 16], primary: bool) -> Vec<u8> {
    let mut b = vec![0u8; SECTSIZE as usize];
    BigEndian::write_u32(&mut b[0..4], XFS_SB_MAGIC);
    BigEndian::write_u32(&mut b[4..8], BLOCKSIZE);
    BigEndian::write_u64(&mut b[8..16], l.dblocks);
    b[32..48].copy_from_slice(uuid);
    BigEndian::write_u64(&mut b[48..56], l.logstart());
    BigEndian::write_u64(&mut b[56..64], l.rootino);
    BigEndian::write_u64(&mut b[64..72], l.rootino + 1);
    BigEndian::write_u64(&mut b[72..80], l.rootino + 2);
    BigEndian::write_u32(&mut b[80..84], 1); // sb_rextsize
    BigEndian::write_u32(&mut b[84..88], l.agblocks);
    BigEndian::write_u32(&mut b[88..92], l.agcount);
    BigEndian::write_u32(&mut b[96..100], l.logblocks);
    BigEndian::write_u16(&mut b[100..102], VERSIONNUM);
    BigEndian::write_u16(&mut b[102..104], SECTSIZE as u16);
    BigEndian::write_u16(&mut b[104..106], INODESIZE as u16);
    BigEndian::write_u16(&mut b[106..108], INOPBLOCK);
    let name = label.as_bytes();
    let n = name.len().min(super::types::XFS_LABEL_MAX);
    b[108..108 + n].copy_from_slice(&name[..n]);
    b[120] = BLOCKLOG;
    b[121] = SECTLOG;
    b[122] = INODELOG;
    b[123] = INOPBLOG;
    b[124] = l.agblklog;
    b[126] = u8::from(!primary); // sb_inprogress
    b[127] = 25; // sb_imax_pct

    if primary {
        let free: u64 = (0..l.agcount).map(|ag| l.free_len(ag) as u64).sum();
        BigEndian::write_u64(&mut b[128..136], CHUNK_INODES as u64);
        BigEndian::write_u64(&mut b[136..144], (CHUNK_INODES - 3) as u64);
        // AGFL blocks are unallocated too, so they count towards free space
        // even though the per-AG `agf_freeblks` excludes them.
        BigEndian::write_u64(
            &mut b[144..152],
            free + (l.agcount as u64) * AGFL_RESERVE as u64,
        );
    }

    BigEndian::write_u32(&mut b[180..184], INOALIGNMT);
    BigEndian::write_u32(&mut b[196..200], 1); // sb_logsunit
    BigEndian::write_u32(&mut b[200..204], FEATURES2);
    BigEndian::write_u32(&mut b[204..208], FEATURES2); // sb_bad_features2
    BigEndian::write_u32(&mut b[216..220], FEATURES_INCOMPAT);
    v5_crc::stamp_superblock(&mut b);
    b
}

fn build_agf(l: &Layout, agno: u32, sb: &XfsSuperblock) -> Vec<u8> {
    let mut b = vec![0u8; SECTSIZE as usize];
    let free = l.free_len(agno);
    BigEndian::write_u32(&mut b[0..4], XFS_AGF_MAGIC);
    BigEndian::write_u32(&mut b[4..8], 1); // agf_versionnum
    BigEndian::write_u32(&mut b[8..12], agno);
    BigEndian::write_u32(&mut b[12..16], l.agblocks);
    BigEndian::write_u32(&mut b[16..20], 1); // agf_roots[bno]
    BigEndian::write_u32(&mut b[20..24], 2); // agf_roots[cnt]
    BigEndian::write_u32(&mut b[28..32], 1); // agf_levels[bno]
    BigEndian::write_u32(&mut b[32..36], 1); // agf_levels[cnt]
    BigEndian::write_u32(&mut b[40..44], 1); // agf_flfirst
    BigEndian::write_u32(&mut b[44..48], AGFL_RESERVE); // agf_fllast
    BigEndian::write_u32(&mut b[48..52], AGFL_RESERVE); // agf_flcount
    BigEndian::write_u32(&mut b[52..56], free);
    BigEndian::write_u32(&mut b[56..60], free); // agf_longest
    v5_crc::stamp_agf(&mut b, sb);
    b
}

fn build_agi(l: &Layout, agno: u32, sb: &XfsSuperblock) -> Vec<u8> {
    let mut b = vec![0u8; SECTSIZE as usize];
    BigEndian::write_u32(&mut b[0..4], XFS_AGI_MAGIC);
    BigEndian::write_u32(&mut b[4..8], 1); // agi_versionnum
    BigEndian::write_u32(&mut b[8..12], agno);
    BigEndian::write_u32(&mut b[12..16], l.agblocks);
    BigEndian::write_u32(&mut b[20..24], 3); // agi_root
    BigEndian::write_u32(&mut b[24..28], 1); // agi_level
    if agno == 0 {
        BigEndian::write_u32(&mut b[16..20], CHUNK_INODES);
        BigEndian::write_u32(&mut b[28..32], CHUNK_INODES - 3);
        BigEndian::write_u32(&mut b[32..36], l.rootino as u32);
    } else {
        BigEndian::write_u32(&mut b[32..36], NULLAGINO);
    }
    BigEndian::write_u32(&mut b[36..40], NULLAGINO); // agi_dirino
    for slot in 0..64 {
        let off = 40 + slot * 4;
        BigEndian::write_u32(&mut b[off..off + 4], NULLAGINO);
    }
    v5_crc::stamp_agi(&mut b, sb);
    b
}

fn build_agfl(l: &Layout, agno: u32, sb: &XfsSuperblock) -> Vec<u8> {
    let mut b = vec![0u8; SECTSIZE as usize];
    BigEndian::write_u32(&mut b[0..4], XFS_AGFL_MAGIC);
    BigEndian::write_u32(&mut b[4..8], agno);
    let slots = (SECTSIZE as usize - 36) / 4;
    for slot in 0..slots {
        let off = 36 + slot * 4;
        BigEndian::write_u32(&mut b[off..off + 4], NULLAGBLOCK);
    }
    // mkfs.xfs pushes the four reserved blocks on at index 1, leaving slot 0
    // unused; agf_flfirst/fllast mirror that.
    for i in 0..AGFL_RESERVE {
        let off = 36 + (1 + i as usize) * 4;
        BigEndian::write_u32(&mut b[off..off + 4], AG_RESERVED_BLOCKS + i);
    }
    let _ = l;
    v5_crc::stamp_agfl(&mut b, sb);
    b
}

/// One on-disk v3 inode. `mode` of zero produces the skeleton `mkfs.xfs`
/// leaves in every unallocated slot of a freshly minted chunk.
fn build_inode(
    ino: u64,
    mode: u16,
    format: u8,
    nlink: u32,
    size: u64,
    sb: &XfsSuperblock,
) -> Vec<u8> {
    let mut b = vec![0u8; INODESIZE as usize];
    BigEndian::write_u16(&mut b[0..2], XFS_DINODE_MAGIC);
    BigEndian::write_u16(&mut b[2..4], mode);
    b[4] = 3; // di_version
    b[5] = format;
    BigEndian::write_u32(&mut b[16..20], nlink);
    BigEndian::write_u64(&mut b[56..64], size);
    if mode != 0 {
        b[83] = 2; // di_aformat = extents
    }
    BigEndian::write_u32(&mut b[96..100], NULLAGINO); // di_next_unlinked
    v5_crc::stamp_inode_v3(&mut b, ino, sb);
    b
}

/// The AG 0 inode chunk: root directory, realtime bitmap/summary inodes, then
/// 61 free slots.
fn build_inode_chunk(l: &Layout, sb: &XfsSuperblock) -> Vec<u8> {
    let mut chunk = Vec::with_capacity((CHUNK_BLOCKS * BLOCKSIZE) as usize);

    // Root: an empty shortform directory, so its whole fork is the 6-byte
    // xfs_dir2_sf_hdr (count, i8count, 4-byte parent).
    let mut root = build_inode(l.rootino, 0o040755, 1, 2, 6, sb);
    let fork = sb.fork_offset();
    BigEndian::write_u32(&mut root[fork + 2..fork + 6], l.rootino as u32);
    v5_crc::stamp_inode_v3(&mut root, l.rootino, sb);
    chunk.extend_from_slice(&root);

    // The realtime bitmap inode carries NEWRTBM even on a non-realtime volume.
    let mut rbm = build_inode(l.rootino + 1, 0o100000, 2, 1, 0, sb);
    BigEndian::write_u16(&mut rbm[90..92], 0x0004); // di_flags = XFS_DIFLAG_NEWRTBM
    v5_crc::stamp_inode_v3(&mut rbm, l.rootino + 1, sb);
    chunk.extend_from_slice(&rbm);
    chunk.extend_from_slice(&build_inode(l.rootino + 2, 0o100000, 2, 1, 0, sb));

    for i in 3..CHUNK_INODES as u64 {
        chunk.extend_from_slice(&build_inode(l.rootino + i, 0, 0, 0, 0, sb));
    }
    chunk
}

/// The clean-unmount log record `mkfs.xfs` writes at the head of the log: a
/// 512-byte `xlog_rec_header` plus one 512-byte unmount transaction.
fn build_log_head(uuid: &[u8; 16]) -> Vec<u8> {
    let mut b = vec![0u8; 1024];
    BigEndian::write_u32(&mut b[0..4], 0xFEED_BABE); // h_magicno
    BigEndian::write_u32(&mut b[4..8], 1); // h_cycle
    BigEndian::write_u32(&mut b[8..12], 2); // h_version
    BigEndian::write_u32(&mut b[12..16], 512); // h_len
    BigEndian::write_u64(&mut b[16..24], 1 << 32); // h_lsn = cycle 1, block 0
    BigEndian::write_u64(&mut b[24..32], 1 << 32); // h_tail_lsn
    BigEndian::write_u32(&mut b[36..40], u32::MAX); // h_prev_block
    BigEndian::write_u32(&mut b[40..44], 1); // h_num_logops
    BigEndian::write_u32(&mut b[300..304], 1); // h_fmt = XLOG_FMT_LINUX_LE
    b[304..320].copy_from_slice(uuid);
    BigEndian::write_u32(&mut b[320..324], 32768); // h_size

    // The op header's first word is displaced by the per-512-byte cycle stamp,
    // which parks the original in h_cycle_data[0].
    BigEndian::write_u32(&mut b[44..48], UNMOUNT_TID);
    BigEndian::write_u32(&mut b[512..516], 1);
    BigEndian::write_u32(&mut b[516..520], 8); // oh_len
    b[520] = 0xAA; // oh_clientid = XFS_LOG
    b[521] = 0x20; // oh_flags = XLOG_UNMOUNT_TRANS
    b[524..526].copy_from_slice(&0x556Eu16.to_le_bytes()); // XLOG_UNMOUNT_TYPE
    b
}

/// Stream a blank XFS volume into `sink` at `part_off`, writing only the
/// regions that carry metadata. Returns the formatted length, which can be a
/// touch under `size_bytes` when the trailing partial AG is dropped.
pub fn write_blank_xfs<W: Write + Seek>(
    sink: &mut W,
    part_off: u64,
    size_bytes: u64,
    label: &str,
) -> Result<u64, FilesystemError> {
    let l = plan(size_bytes)?;
    let uuid = make_uuid();
    let sb_bytes = build_superblock(&l, label, &uuid, true);
    let sb = XfsSuperblock::parse(&sb_bytes)?;

    let mut put = |offset: u64, data: &[u8]| -> Result<(), FilesystemError> {
        sink.seek(SeekFrom::Start(part_off + offset))
            .map_err(FilesystemError::Io)?;
        sink.write_all(data).map_err(FilesystemError::Io)
    };

    let bs = BLOCKSIZE as u64;
    for agno in 0..l.agcount {
        let ag_base = agno as u64 * l.agblocks as u64 * bs;
        let sb_sector = if agno == 0 {
            sb_bytes.clone()
        } else {
            build_superblock(&l, label, &uuid, false)
        };
        put(ag_base, &sb_sector)?;
        put(ag_base + SECTSIZE as u64, &build_agf(&l, agno, &sb))?;
        put(ag_base + 2 * SECTSIZE as u64, &build_agi(&l, agno, &sb))?;
        put(ag_base + 3 * SECTSIZE as u64, &build_agfl(&l, agno, &sb))?;

        let free = l.free_len(agno);
        let extents = if free > 0 {
            vec![FreeExtent {
                startblock: l.free_start(agno),
                blockcount: free,
            }]
        } else {
            Vec::new()
        };
        for (root_agbno, magic) in [(1u32, sb.bnobt_magic()), (2, sb.cntbt_magic())] {
            let tree = build_alloc_btree(
                &extents,
                magic,
                BLOCKSIZE as usize,
                agno,
                &[root_agbno],
                Some(&sb),
            );
            for blk in &tree.blocks {
                put(ag_base + blk.agbno as u64 * bs, &blk.bytes)?;
            }
        }

        // inobt: one record for AG 0's chunk (bit set = free, so the root and
        // the two realtime inodes clear the low three bits), empty elsewhere.
        let mut records = Vec::new();
        if agno == 0 {
            records.resize(16, 0);
            BigEndian::write_u32(&mut records[0..4], l.rootino as u32);
            BigEndian::write_u32(&mut records[4..8], CHUNK_INODES - 3);
            BigEndian::write_u64(&mut records[8..16], !0u64 << 3);
        }
        let inobt = build_sblock_btree(
            &records,
            16,
            4,
            sb.inobt_magic(),
            BLOCKSIZE as usize,
            agno,
            &[3],
            Some(&sb),
        );
        for blk in &inobt.blocks {
            put(ag_base + blk.agbno as u64 * bs, &blk.bytes)?;
        }

        if agno == 0 {
            put(
                ag_base + l.root_chunk_agbno as u64 * bs,
                &build_inode_chunk(&l, &sb),
            )?;
        }
        if agno == l.log_agno {
            put(ag_base + l.log_agbno as u64 * bs, &build_log_head(&uuid))?;
        }
    }

    Ok(l.dblocks * bs)
}

/// Format a blank XFS volume into memory. Used by the multi-partition builder
/// and tests; `new` streams straight to a file instead.
pub fn create_blank_xfs(size_bytes: u64, label: &str) -> Result<Vec<u8>, FilesystemError> {
    let plan = plan(size_bytes)?;
    let mut img = vec![0u8; (plan.dblocks * BLOCKSIZE as u64) as usize];
    write_blank_xfs(
        &mut std::io::Cursor::new(&mut img[..]),
        0,
        size_bytes,
        label,
    )?;
    Ok(img)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::fs::filesystem::Filesystem;
    use std::io::Cursor;

    /// 32 MiB is the two-AG floor, 64 MiB the first four-AG size, 300 MiB the
    /// smallest mkfs.xfs will itself make — and the one with non-power-of-two
    /// AGs, which is what caught the `bb_blkno` bug. Each is materialized in
    /// RAM, so anything larger is checked through `plan` instead: the 32-bit
    /// Windows target runs out of address space long before it runs out of
    /// memory.
    const SIZES: &[u64] = &[32 << 20, 64 << 20, 300 << 20];

    #[test]
    fn geometry_matches_mkfs_for_a_512_mib_volume() {
        let l = plan(512 << 20).unwrap();
        assert_eq!(l.dblocks, 131072);
        assert_eq!(l.agcount, 4);
        assert_eq!(l.agblocks, 32768);
        assert_eq!(l.agblklog, 15);
        assert_eq!(l.rootino, 64);
        assert_eq!(l.free_start(1), 8);
    }

    #[test]
    fn refuses_volumes_that_cannot_hold_a_log() {
        assert!(plan(4 << 20).is_err());
    }

    /// Geometry at sizes too large to materialize in a test. AG count stays at
    /// the mkfs.xfs default of four until an AG would pass XFS_AG_MAX_BLOCKS,
    /// and the log tracks dblocks/2048 between its 10 MiB and 2 GiB bounds.
    #[test]
    fn large_volumes_scale_their_ags_and_log() {
        let one_gib = plan(1 << 30).unwrap();
        assert_eq!(one_gib.agcount, 4);
        assert_eq!(one_gib.agblocks, 65536);
        assert_eq!(one_gib.logblocks as u64, LOG_MIN_BLOCKS);

        // 1 TiB: still four AGs, but the log has grown past its floor.
        let one_tib = plan(1 << 40).unwrap();
        assert_eq!(one_tib.agcount, 4);
        assert_eq!(one_tib.logblocks as u64, one_tib.dblocks / 2048);

        // 8 TiB: four AGs would each pass the 1 TiB XFS_AG_MAX_BLOCKS ceiling,
        // so the count has to grow instead.
        let eight_tib = plan(8u64 << 40).unwrap();
        assert!(eight_tib.agcount > 4, "agcount {}", eight_tib.agcount);
        assert!(
            u64::from(eight_tib.agblocks) <= AG_MAX_BLOCKS,
            "agblocks {} over the 1 TiB ceiling",
            eight_tib.agblocks,
        );
        assert!(u64::from(eight_tib.logblocks) <= LOG_MAX_BLOCKS);
    }

    #[test]
    fn every_size_round_trips_through_our_own_reader() {
        for &size in SIZES {
            let img = create_blank_xfs(size, "rbtest").unwrap();
            let mut fs = crate::fs::xfs::XfsFilesystem::open(Cursor::new(img), 0)
                .unwrap_or_else(|e| panic!("{size}: open: {e}"));
            let root = fs.root().unwrap();
            assert!(
                fs.list_directory(&root).unwrap().is_empty(),
                "{size}: a fresh volume's root must be empty"
            );
            assert_eq!(fs.volume_label(), Some("rbtest"), "{size}");
        }
    }

    #[test]
    fn our_own_fsck_finds_a_fresh_volume_clean() {
        for &size in SIZES {
            let img = create_blank_xfs(size, "rbtest").unwrap();
            let mut fs = crate::fs::xfs::XfsFilesystem::open(Cursor::new(img), 0).unwrap();
            let report = fs
                .run_fsck()
                .unwrap_or_else(|e| panic!("{size}: fsck: {e}"));
            assert!(
                report.errors.is_empty(),
                "{size}: fresh volume is not clean: {:?}",
                report.errors
            );
        }
    }
}
