//! XFS bmap extent record decoder (`xfs_bmbt_rec` → `xfs_bmbt_irec`).
//!
//! On-disk format (128 bits, big-endian, MSB-first bit numbering):
//! - bit 0: flag (0 = normal, 1 = preallocated/unwritten)
//! - bits 1..54: file offset, in filesystem blocks (54 bits)
//! - bits 55..106: start block (52 bits)
//! - bits 107..127: block count (21 bits)
//!
//! Decoding mirrors `xfs_bmbt_disk_get_all` in
//! `~/xfs-efs/refs/xfs-modern/xfs/libxfs/xfs_bmap_btree.c`.

use byteorder::{BigEndian, ByteOrder};

/// `BMAP` — bmap btree block magic (v4, long-form pointers, no CRC).
pub const XFS_BMAP_MAGIC: u32 = 0x424D_4150;

/// `BMA3` — bmap btree block magic (v5, long-form pointers, CRC).
pub const XFS_BMAP_CRC_MAGIC: u32 = 0x424D_4133;

/// Long-form (v4 without CRC) bmap btree block header size:
/// `magic(4) + level(2) + numrecs(2) + leftsib(8) + rightsib(8)` = 24 bytes.
pub const XFS_BTREE_LBLOCK_LEN: usize = 24;

/// Long-form CRC (v5) bmap btree block header size. v4 header (24) plus
/// `blkno(8) + lsn(8) + uuid(16) + owner(8) + crc(4) + pad(4)` = 72 bytes.
pub const XFS_BTREE_LBLOCK_CRC_LEN: usize = 72;

/// `NULLFSBLOCK` — sentinel for "no sibling / no child" in btree pointers.
pub const NULLFSBLOCK: u64 = u64::MAX;

/// Decoded extent record.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct XfsBmbtIrec {
    /// File offset in filesystem blocks.
    pub startoff: u64,
    /// Starting filesystem block (combined AG+block address).
    pub startblock: u64,
    /// Length in filesystem blocks.
    pub blockcount: u64,
    /// True for unwritten (preallocated) extents — read as zero.
    pub unwritten: bool,
}

/// Decode a packed extent record.
pub fn decode_extent(rec: &[u8; 16]) -> XfsBmbtIrec {
    let l0 = BigEndian::read_u64(&rec[0..8]);
    let l1 = BigEndian::read_u64(&rec[8..16]);
    let unwritten = (l0 >> 63) != 0;
    // bits 1..54 of the 128-bit stream → bits 9..63 of l0.
    let startoff = (l0 & ((1u64 << 63) - 1)) >> 9;
    // bits 55..106 → low 9 bits of l0 + high 43 bits of l1.
    let startblock = ((l0 & 0x1FF) << 43) | (l1 >> 21);
    // bits 107..127 → low 21 bits of l1.
    let blockcount = l1 & ((1u64 << 21) - 1);
    XfsBmbtIrec {
        startoff,
        startblock,
        blockcount,
        unwritten,
    }
}

/// Pack an extent record from logical fields, the inverse of [`decode_extent`]
/// (mirrors `xfs_bmbt_disk_set_all`). Used by the write path to lay down a
/// file's data-fork extent.
pub fn encode_extent(unwritten: bool, startoff: u64, startblock: u64, blockcount: u64) -> [u8; 16] {
    let mut buf = [0u8; 16];
    let flag = u64::from(unwritten);
    let l0 = (flag << 63) | (startoff << 9) | (startblock >> 43);
    let l1 = (startblock << 21) | (blockcount & ((1u64 << 21) - 1));
    BigEndian::write_u64(&mut buf[0..8], l0);
    BigEndian::write_u64(&mut buf[8..16], l1);
    buf
}

/// Translate an XFS filesystem block (the combined AG+block address used in
/// extent records) into a byte offset relative to the start of the partition.
///
///   agno    = fsblock >> agblklog
///   agbno   = fsblock & ((1 << agblklog) - 1)
///   byte    = (agno * agblocks + agbno) * blocksize
pub fn fsblock_to_partition_byte(fsblock: u64, agblocks: u32, agblklog: u8, blocksize: u32) -> u64 {
    let agno = fsblock >> agblklog;
    let agbno = fsblock & ((1u64 << agblklog) - 1);
    (agno * agblocks as u64 + agbno) * blocksize as u64
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Pack a record from logical fields (round-trip partner of decode).
    fn pack(unwritten: bool, startoff: u64, startblock: u64, blockcount: u64) -> [u8; 16] {
        encode_extent(unwritten, startoff, startblock, blockcount)
    }

    #[test]
    fn decodes_normal_record() {
        // Small extent: file_off=0, start=8, count=1, normal.
        let buf = pack(false, 0, 8, 1);
        let irec = decode_extent(&buf);
        assert_eq!(
            irec,
            XfsBmbtIrec {
                startoff: 0,
                startblock: 8,
                blockcount: 1,
                unwritten: false,
            }
        );
    }

    #[test]
    fn decodes_unwritten_flag() {
        let buf = pack(true, 12345, 67890, 42);
        let irec = decode_extent(&buf);
        assert!(irec.unwritten);
        assert_eq!(irec.startoff, 12345);
        assert_eq!(irec.startblock, 67890);
        assert_eq!(irec.blockcount, 42);
    }

    #[test]
    fn decodes_large_offset_and_block() {
        // Stress bit-packing: file_off uses all 54 bits, startblock uses all 52,
        // blockcount uses all 21.
        let big_off = (1u64 << 54) - 1;
        let big_blk = (1u64 << 52) - 1;
        let big_cnt = (1u64 << 21) - 1;
        let buf = pack(false, big_off, big_blk, big_cnt);
        let irec = decode_extent(&buf);
        assert_eq!(irec.startoff, big_off);
        assert_eq!(irec.startblock, big_blk);
        assert_eq!(irec.blockcount, big_cnt);
        assert!(!irec.unwritten);
    }

    #[test]
    fn fsblock_translation_first_ag() {
        // AG0: agno=0 → byte = block * blocksize.
        assert_eq!(fsblock_to_partition_byte(8, 65536, 16, 4096), 8 * 4096);
    }

    #[test]
    fn fsblock_translation_second_ag() {
        // agblklog=16 → AG mask = 0xFFFF. fsblock = (1<<16)|5 → ag=1, agbno=5.
        // byte = (1*65536 + 5) * 4096.
        let fsb = (1u64 << 16) | 5;
        #[allow(clippy::identity_op)] // `1 * 65536` mirrors ag=1 in the comment above
        let expect = (1u64 * 65536 + 5) * 4096;
        assert_eq!(fsblock_to_partition_byte(fsb, 65536, 16, 4096), expect);
    }
}
