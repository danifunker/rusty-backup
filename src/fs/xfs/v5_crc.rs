//! v5/CRC stamping primitives for XFS metadata blocks.
//!
//! Every v5 metadata block carries a 4-byte CRC-32C (Castagnoli) over the
//! block buffer with its own CRC field zeroed, plus a uuid/blkno/lsn/owner
//! tuple in a per-block-type header. These helpers compute the CRC and
//! splat the header fields; callers still own the rest of the block layout
//! (records, entries, bestfree, etc.).
//!
//! CRC field on disk is **little-endian** (`__le32` in `xfs_format.h`)
//! while every other field on an XFS block is big-endian. Easy bug source.
//!
//! Reference: `libxfs/xfs_cksum.h`, `xfs_format.h` for offsets.
//!
//! No edit-path behaviour changes in this module — these are pure plumbing
//! used by the per-structure write paths landing in slices E.2 through E.5.

use byteorder::{BigEndian, ByteOrder};

use super::sb::XfsSuperblock;

// ---------- CRC field offsets per block type ----------

/// `di_crc` offset inside a v3 inode core (4 bytes, LE).
pub const INODE_V3_DI_CRC_OFF: usize = 100;
/// `di_changecount` offset inside a v3 inode core (8 bytes, BE).
pub const INODE_V3_DI_CHANGECOUNT_OFF: usize = 104;
/// `di_lsn` offset inside a v3 inode core (8 bytes, BE).
pub const INODE_V3_DI_LSN_OFF: usize = 112;
/// `di_ino` offset inside a v3 inode core (8 bytes, BE). Equals the inode
/// number; the verifier rejects a mismatch.
pub const INODE_V3_DI_INO_OFF: usize = 152;
/// `di_uuid` offset inside a v3 inode core (16 bytes). Must match
/// [`XfsSuperblock::meta_uuid`].
pub const INODE_V3_DI_UUID_OFF: usize = 160;

/// `crc` offset inside an `xfs_dir3_blk_hdr` (data / single-block dir).
pub const DIR3_BLK_CRC_OFF: usize = 4;
pub const DIR3_BLK_BLKNO_OFF: usize = 8;
pub const DIR3_BLK_LSN_OFF: usize = 16;
pub const DIR3_BLK_UUID_OFF: usize = 24;
pub const DIR3_BLK_OWNER_OFF: usize = 40;

/// `crc` offset inside an `xfs_da3_blkinfo` (dir3 leaf / node, dabtree).
/// Layout: forw(4) back(4) magic(2) pad(2) **crc**(4) blkno(8) lsn(8)
/// uuid(16) owner(8) = 56 bytes total.
pub const DA3_CRC_OFF: usize = 12;
pub const DA3_BLKNO_OFF: usize = 16;
pub const DA3_LSN_OFF: usize = 24;
pub const DA3_UUID_OFF: usize = 32;
pub const DA3_OWNER_OFF: usize = 48;

/// Short-form (AG-relative) btree CRC-header field offsets — inobt / bnobt
/// / cntbt v5. Layout: magic(4) level(2) numrecs(2) leftsib(4) rightsib(4)
/// **blkno**(8) **lsn**(8) **uuid**(16) **owner**(4 — agno) **crc**(4) =
/// 56 bytes.
pub const SBLOCK_CRC_BLKNO_OFF: usize = 16;
pub const SBLOCK_CRC_LSN_OFF: usize = 24;
pub const SBLOCK_CRC_UUID_OFF: usize = 32;
pub const SBLOCK_CRC_OWNER_OFF: usize = 48;
pub const SBLOCK_CRC_CRC_OFF: usize = 52;

/// Long-form (full-fsblock) btree CRC-header field offsets — bmbt v5.
/// Layout: magic(4) level(2) numrecs(2) leftsib(8) rightsib(8) **blkno**(8)
/// **lsn**(8) **uuid**(16) **owner**(8 — inode) **crc**(4) pad(4) = 72.
pub const LBLOCK_CRC_BLKNO_OFF: usize = 24;
pub const LBLOCK_CRC_LSN_OFF: usize = 32;
pub const LBLOCK_CRC_UUID_OFF: usize = 40;
pub const LBLOCK_CRC_OWNER_OFF: usize = 56;
pub const LBLOCK_CRC_CRC_OFF: usize = 64;

/// AGF v5 CRC-header field offsets. Layout: agf_magicnum(4)
/// agf_versionnum(4) agf_seqno(4) agf_length(4) agf_roots[3](12)
/// agf_levels[3](12) agf_flfirst(4) agf_fllast(4) agf_flcount(4)
/// agf_freeblks(4) agf_longest(4) agf_btreeblks(4) **agf_uuid**(16)
/// agf_rmap_blocks(4) agf_refcount_blocks(4) agf_refcount_root(4)
/// agf_refcount_level(4) agf_spare64[14](112) **agf_lsn**(8)
/// **agf_crc**(4) agf_spare2(4). We only touch the CRC-header fields;
/// every other field is set by the AGF writer.
pub const AGF_UUID_OFF: usize = 64;
pub const AGF_LSN_OFF: usize = 208;
pub const AGF_CRC_OFF: usize = 216;

/// AGI v5 CRC-header field offsets. agi_magicnum(4) agi_versionnum(4)
/// agi_seqno(4) agi_length(4) agi_count(4) agi_root(4) agi_level(4)
/// agi_freecount(4) agi_newino(4) agi_dirino(4) agi_unlinked[64](256)
/// **agi_uuid**(16) **agi_crc**(4) agi_pad32(4) **agi_lsn**(8)
/// agi_free_root(4) agi_free_level(4) agi_iblocks(4) agi_fblocks(4).
pub const AGI_UUID_OFF: usize = 296;
pub const AGI_CRC_OFF: usize = 312;
pub const AGI_LSN_OFF: usize = 320;

/// AGFL v5 CRC-header field offsets. agfl_magicnum(4) agfl_seqno(4)
/// **agfl_uuid**(16) **agfl_lsn**(8) **agfl_crc**(4), then the agbno
/// array. Total header = 36 bytes — matches the `arr_off = 36` we already
/// use in the freespace_rebuild AGFL reader.
pub const AGFL_UUID_OFF: usize = 8;
pub const AGFL_LSN_OFF: usize = 24;
pub const AGFL_CRC_OFF: usize = 32;

/// Superblock v5 CRC field offset. `sb_crc` lives at byte 224; the CRC
/// covers the entire SB buffer (sectsize bytes) with that field zeroed.
pub const SB_CRC_OFF: usize = 224;
/// Superblock v5 `sb_lsn` offset (BE64 — unlike the per-block lsn fields,
/// the SB lsn isn't directly referenced by metadata stampers; we expose
/// the offset so the SB writer can bump it without redefining constants).
pub const SB_LSN_OFF: usize = 240;

/// `XFS_LSN_NULL` — value we stamp into `lsn` fields. The log replays
/// transactions whose lsn is **less than** the on-disk lsn, so a freshly
/// minted block with no transaction backing it carries the all-ones
/// sentinel that compares greater than every real lsn.
pub const XFS_LSN_NULL: u64 = u64::MAX;

// ---------- CRC primitive ----------

/// Compute the standard CRC-32C (Castagnoli, init=0xFFFFFFFF, xorout=
/// 0xFFFFFFFF) over `buf` — what `xfs_start_cksum_safe` produces. The
/// `crc32c` crate's defaults already match.
pub fn crc32c(buf: &[u8]) -> u32 {
    crc32c::crc32c(buf)
}

/// Zero the 4-byte CRC field at `crc_off`, compute CRC-32C over the entire
/// `buf`, and write the result back at `crc_off` as little-endian. Mirrors
/// `xfs_start_cksum_safe` + write-back. Panics in debug if `crc_off + 4 >
/// buf.len()` — every caller picks a constant offset for a known block
/// layout, so a slip is a bug.
pub fn stamp_crc(buf: &mut [u8], crc_off: usize) {
    assert!(
        crc_off + 4 <= buf.len(),
        "stamp_crc: crc_off {crc_off}+4 > buf.len() {}",
        buf.len()
    );
    buf[crc_off..crc_off + 4].fill(0);
    let crc = crc32c(buf);
    buf[crc_off..crc_off + 4].copy_from_slice(&crc.to_le_bytes());
}

/// Verify the CRC at `crc_off` matches what stamping would produce. Used by
/// the unit tests below — readers don't call this (they trust the CRC and
/// surface the structural error if they're wrong).
#[cfg(test)]
pub fn crc_valid(buf: &[u8], crc_off: usize) -> bool {
    let stored = u32::from_le_bytes(buf[crc_off..crc_off + 4].try_into().unwrap());
    let mut tmp = buf.to_vec();
    tmp[crc_off..crc_off + 4].fill(0);
    crc32c(&tmp) == stored
}

// ---------- Per-block-type stampers ----------

/// Stamp the v3 inode core CRC-header fields. Writes uuid (= meta_uuid),
/// di_ino (= `ino`), di_lsn (NULL sentinel), increments di_changecount by
/// one, then computes and writes di_crc. `buf` must hold a full on-disk
/// inode (sb.inodesize bytes); the CRC covers the entire inode buffer.
///
/// v4 callers must not call this — the v4 inode has neither di_crc nor the
/// other v3-only fields. Edit sites dispatch on `sb.is_v5()` first.
pub fn stamp_inode_v3(buf: &mut [u8], ino: u64, sb: &XfsSuperblock) {
    let inodesize = sb.inodesize as usize;
    assert!(
        buf.len() >= inodesize,
        "stamp_inode_v3: buf {} < inodesize {inodesize}",
        buf.len()
    );
    let buf = &mut buf[..inodesize];

    // di_ino (152): the inode's own number, BE64.
    BigEndian::write_u64(&mut buf[INODE_V3_DI_INO_OFF..INODE_V3_DI_INO_OFF + 8], ino);
    // di_uuid (160): always meta_uuid (sb_meta_uuid if META_UUID, else
    // sb_uuid).
    buf[INODE_V3_DI_UUID_OFF..INODE_V3_DI_UUID_OFF + 16].copy_from_slice(sb.meta_uuid());
    // di_lsn (112): NULL sentinel for blocks not yet covered by a real
    // transaction.
    BigEndian::write_u64(
        &mut buf[INODE_V3_DI_LSN_OFF..INODE_V3_DI_LSN_OFF + 8],
        XFS_LSN_NULL,
    );
    // di_changecount (104): bump by one on every metadata write (xfs_repair
    // doesn't require a specific value, but a monotone counter is what
    // every mounted-kernel write does).
    let old =
        BigEndian::read_u64(&buf[INODE_V3_DI_CHANGECOUNT_OFF..INODE_V3_DI_CHANGECOUNT_OFF + 8]);
    BigEndian::write_u64(
        &mut buf[INODE_V3_DI_CHANGECOUNT_OFF..INODE_V3_DI_CHANGECOUNT_OFF + 8],
        old.wrapping_add(1),
    );

    stamp_crc(buf, INODE_V3_DI_CRC_OFF);
}

/// Stamp the `xfs_dir3_blk_hdr` CRC-header fields and CRC. Used for dir3
/// single-block (`XDB3`) and multi-block data (`XDD3`) blocks. The CRC
/// covers the full directory block.
pub fn stamp_dir3_blk_hdr(buf: &mut [u8], blkno: u64, owner_ino: u64, sb: &XfsSuperblock) {
    assert!(buf.len() >= 48, "dir3_blk_hdr buf too small");
    BigEndian::write_u64(&mut buf[DIR3_BLK_BLKNO_OFF..DIR3_BLK_BLKNO_OFF + 8], blkno);
    BigEndian::write_u64(
        &mut buf[DIR3_BLK_LSN_OFF..DIR3_BLK_LSN_OFF + 8],
        XFS_LSN_NULL,
    );
    buf[DIR3_BLK_UUID_OFF..DIR3_BLK_UUID_OFF + 16].copy_from_slice(sb.meta_uuid());
    BigEndian::write_u64(
        &mut buf[DIR3_BLK_OWNER_OFF..DIR3_BLK_OWNER_OFF + 8],
        owner_ino,
    );
    stamp_crc(buf, DIR3_BLK_CRC_OFF);
}

/// Stamp the `xfs_da3_blkinfo` CRC-header fields and CRC. Used for dir3
/// leaf (`3DF1`/`3DFF`) and dabtree node blocks. The CRC covers the full
/// directory block.
pub fn stamp_da3_blkinfo(buf: &mut [u8], blkno: u64, owner_ino: u64, sb: &XfsSuperblock) {
    assert!(buf.len() >= 56, "da3_blkinfo buf too small");
    BigEndian::write_u64(&mut buf[DA3_BLKNO_OFF..DA3_BLKNO_OFF + 8], blkno);
    BigEndian::write_u64(&mut buf[DA3_LSN_OFF..DA3_LSN_OFF + 8], XFS_LSN_NULL);
    buf[DA3_UUID_OFF..DA3_UUID_OFF + 16].copy_from_slice(sb.meta_uuid());
    BigEndian::write_u64(&mut buf[DA3_OWNER_OFF..DA3_OWNER_OFF + 8], owner_ino);
    stamp_crc(buf, DA3_CRC_OFF);
}

/// Stamp the short-form (AG-relative) btree CRC header — inobt / bnobt /
/// cntbt v5. `owner_agno` is the AG number. The CRC covers the full
/// fsblock-sized buffer.
pub fn stamp_sblock_crc_header(buf: &mut [u8], blkno: u64, owner_agno: u32, sb: &XfsSuperblock) {
    assert!(buf.len() >= 56, "sblock crc buf too small");
    BigEndian::write_u64(
        &mut buf[SBLOCK_CRC_BLKNO_OFF..SBLOCK_CRC_BLKNO_OFF + 8],
        blkno,
    );
    BigEndian::write_u64(
        &mut buf[SBLOCK_CRC_LSN_OFF..SBLOCK_CRC_LSN_OFF + 8],
        XFS_LSN_NULL,
    );
    buf[SBLOCK_CRC_UUID_OFF..SBLOCK_CRC_UUID_OFF + 16].copy_from_slice(sb.meta_uuid());
    BigEndian::write_u32(
        &mut buf[SBLOCK_CRC_OWNER_OFF..SBLOCK_CRC_OWNER_OFF + 4],
        owner_agno,
    );
    stamp_crc(buf, SBLOCK_CRC_CRC_OFF);
}

/// Stamp the long-form (full-fsblock) btree CRC header — bmbt v5.
/// `owner_ino` is the file's inode number. The CRC covers the full
/// fsblock-sized buffer.
pub fn stamp_lblock_crc_header(buf: &mut [u8], blkno: u64, owner_ino: u64, sb: &XfsSuperblock) {
    assert!(buf.len() >= 72, "lblock crc buf too small");
    BigEndian::write_u64(
        &mut buf[LBLOCK_CRC_BLKNO_OFF..LBLOCK_CRC_BLKNO_OFF + 8],
        blkno,
    );
    BigEndian::write_u64(
        &mut buf[LBLOCK_CRC_LSN_OFF..LBLOCK_CRC_LSN_OFF + 8],
        XFS_LSN_NULL,
    );
    buf[LBLOCK_CRC_UUID_OFF..LBLOCK_CRC_UUID_OFF + 16].copy_from_slice(sb.meta_uuid());
    BigEndian::write_u64(
        &mut buf[LBLOCK_CRC_OWNER_OFF..LBLOCK_CRC_OWNER_OFF + 8],
        owner_ino,
    );
    stamp_crc(buf, LBLOCK_CRC_CRC_OFF);
}

/// Stamp an AGF sector's v5 CRC-header fields and CRC.
pub fn stamp_agf(sec: &mut [u8], sb: &XfsSuperblock) {
    assert!(sec.len() >= 220, "AGF sector too small");
    sec[AGF_UUID_OFF..AGF_UUID_OFF + 16].copy_from_slice(sb.meta_uuid());
    BigEndian::write_u64(&mut sec[AGF_LSN_OFF..AGF_LSN_OFF + 8], XFS_LSN_NULL);
    stamp_crc(sec, AGF_CRC_OFF);
}

/// Stamp an AGI sector's v5 CRC-header fields and CRC.
pub fn stamp_agi(sec: &mut [u8], sb: &XfsSuperblock) {
    assert!(sec.len() >= 328, "AGI sector too small");
    sec[AGI_UUID_OFF..AGI_UUID_OFF + 16].copy_from_slice(sb.meta_uuid());
    BigEndian::write_u64(&mut sec[AGI_LSN_OFF..AGI_LSN_OFF + 8], XFS_LSN_NULL);
    stamp_crc(sec, AGI_CRC_OFF);
}

/// Stamp an AGFL sector's v5 CRC-header fields and CRC.
pub fn stamp_agfl(sec: &mut [u8], sb: &XfsSuperblock) {
    assert!(sec.len() >= 36, "AGFL sector too small");
    sec[AGFL_UUID_OFF..AGFL_UUID_OFF + 16].copy_from_slice(sb.meta_uuid());
    BigEndian::write_u64(&mut sec[AGFL_LSN_OFF..AGFL_LSN_OFF + 8], XFS_LSN_NULL);
    stamp_crc(sec, AGFL_CRC_OFF);
}

/// Stamp a superblock buffer's CRC. The SB CRC covers the entire sector;
/// `sb_lsn` stays at its current value (caller bumps it when it cares).
pub fn stamp_superblock(sec: &mut [u8]) {
    assert!(sec.len() >= 228, "SB sector too small");
    stamp_crc(sec, SB_CRC_OFF);
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Build a minimal `XfsSuperblock` for the stamper tests — only the
    /// fields the stampers actually read (`is_v5`, `meta_uuid`,
    /// `inodesize`, `features_incompat`).
    fn test_sb(meta_uuid_byte: u8, inodesize: u16) -> XfsSuperblock {
        XfsSuperblock {
            magicnum: 0,
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
            inodesize,
            inopblock: 0,
            fname: [0; 12],
            blocklog: 12,
            sectlog: 9,
            inodelog: 8,
            inopblog: 4,
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

    #[test]
    fn crc_matches_xfs_canonical_vector() {
        // The canonical CRC-32C of "123456789" is 0xE3069283 (Castagnoli
        // with init=0xFFFFFFFF, xorout=0xFFFFFFFF). xfsprogs' test suite
        // uses the same parameters; if this fires, the crc32c crate
        // doesn't match what xfs_repair expects.
        assert_eq!(crc32c(b"123456789"), 0xE306_9283);
    }

    #[test]
    fn stamp_crc_round_trips_via_verify() {
        let mut buf = vec![0xA5u8; 128];
        // Pretend byte 64 is the CRC field.
        stamp_crc(&mut buf, 64);
        assert!(crc_valid(&buf, 64));
        // Mutating any other byte breaks the CRC.
        buf[0] ^= 1;
        assert!(!crc_valid(&buf, 64));
    }

    #[test]
    fn stamp_inode_v3_writes_ino_uuid_lsn_and_crc() {
        let sb = test_sb(0xCC, 512);
        let mut buf = vec![0u8; 512];
        // Pretend the inode core's magic / version live where they belong;
        // the stamper doesn't touch them, but a v3 verifier would. For the
        // stamping test we only care about CRC + the four header fields.
        stamp_inode_v3(&mut buf, 0x1122_3344_5566_7788, &sb);
        // di_ino == passed ino.
        assert_eq!(
            BigEndian::read_u64(&buf[INODE_V3_DI_INO_OFF..INODE_V3_DI_INO_OFF + 8]),
            0x1122_3344_5566_7788
        );
        // di_uuid == sb.meta_uuid().
        assert_eq!(
            &buf[INODE_V3_DI_UUID_OFF..INODE_V3_DI_UUID_OFF + 16],
            &[0xCC; 16]
        );
        // di_lsn == NULL sentinel.
        assert_eq!(
            BigEndian::read_u64(&buf[INODE_V3_DI_LSN_OFF..INODE_V3_DI_LSN_OFF + 8]),
            XFS_LSN_NULL
        );
        // di_changecount bumped from 0 to 1.
        assert_eq!(
            BigEndian::read_u64(&buf[INODE_V3_DI_CHANGECOUNT_OFF..INODE_V3_DI_CHANGECOUNT_OFF + 8]),
            1
        );
        // CRC verifies.
        assert!(crc_valid(&buf, INODE_V3_DI_CRC_OFF));
    }

    #[test]
    fn stamp_inode_v3_increments_existing_changecount() {
        let sb = test_sb(0, 256);
        let mut buf = vec![0u8; 256];
        BigEndian::write_u64(
            &mut buf[INODE_V3_DI_CHANGECOUNT_OFF..INODE_V3_DI_CHANGECOUNT_OFF + 8],
            42,
        );
        stamp_inode_v3(&mut buf, 1, &sb);
        assert_eq!(
            BigEndian::read_u64(&buf[INODE_V3_DI_CHANGECOUNT_OFF..INODE_V3_DI_CHANGECOUNT_OFF + 8]),
            43
        );
    }

    #[test]
    fn stamp_dir3_blk_hdr_writes_all_fields_and_crc() {
        let sb = test_sb(0xAB, 256);
        let mut buf = vec![0u8; 4096];
        stamp_dir3_blk_hdr(&mut buf, 0xDEAD_BEEF, 0x1234, &sb);
        assert_eq!(
            BigEndian::read_u64(&buf[DIR3_BLK_BLKNO_OFF..DIR3_BLK_BLKNO_OFF + 8]),
            0xDEAD_BEEF
        );
        assert_eq!(&buf[DIR3_BLK_UUID_OFF..DIR3_BLK_UUID_OFF + 16], &[0xAB; 16]);
        assert_eq!(
            BigEndian::read_u64(&buf[DIR3_BLK_OWNER_OFF..DIR3_BLK_OWNER_OFF + 8]),
            0x1234
        );
        assert!(crc_valid(&buf, DIR3_BLK_CRC_OFF));
    }

    #[test]
    fn stamp_da3_blkinfo_writes_all_fields_and_crc() {
        let sb = test_sb(0xEE, 256);
        let mut buf = vec![0u8; 4096];
        stamp_da3_blkinfo(&mut buf, 0xCAFE, 0xBABE, &sb);
        assert_eq!(
            BigEndian::read_u64(&buf[DA3_BLKNO_OFF..DA3_BLKNO_OFF + 8]),
            0xCAFE
        );
        assert_eq!(&buf[DA3_UUID_OFF..DA3_UUID_OFF + 16], &[0xEE; 16]);
        assert_eq!(
            BigEndian::read_u64(&buf[DA3_OWNER_OFF..DA3_OWNER_OFF + 8]),
            0xBABE
        );
        assert!(crc_valid(&buf, DA3_CRC_OFF));
    }

    #[test]
    fn stamp_sblock_crc_header_writes_all_fields_and_crc() {
        let sb = test_sb(0x11, 256);
        let mut buf = vec![0u8; 4096];
        stamp_sblock_crc_header(&mut buf, 0x1001, 7, &sb);
        assert_eq!(
            BigEndian::read_u64(&buf[SBLOCK_CRC_BLKNO_OFF..SBLOCK_CRC_BLKNO_OFF + 8]),
            0x1001
        );
        assert_eq!(
            &buf[SBLOCK_CRC_UUID_OFF..SBLOCK_CRC_UUID_OFF + 16],
            &[0x11; 16]
        );
        assert_eq!(
            BigEndian::read_u32(&buf[SBLOCK_CRC_OWNER_OFF..SBLOCK_CRC_OWNER_OFF + 4]),
            7
        );
        assert!(crc_valid(&buf, SBLOCK_CRC_CRC_OFF));
    }

    #[test]
    fn stamp_lblock_crc_header_writes_all_fields_and_crc() {
        let sb = test_sb(0x22, 256);
        let mut buf = vec![0u8; 4096];
        stamp_lblock_crc_header(&mut buf, 0x2002, 0xA1B2_C3D4, &sb);
        assert_eq!(
            BigEndian::read_u64(&buf[LBLOCK_CRC_BLKNO_OFF..LBLOCK_CRC_BLKNO_OFF + 8]),
            0x2002
        );
        assert_eq!(
            &buf[LBLOCK_CRC_UUID_OFF..LBLOCK_CRC_UUID_OFF + 16],
            &[0x22; 16]
        );
        assert_eq!(
            BigEndian::read_u64(&buf[LBLOCK_CRC_OWNER_OFF..LBLOCK_CRC_OWNER_OFF + 8]),
            0xA1B2_C3D4
        );
        assert!(crc_valid(&buf, LBLOCK_CRC_CRC_OFF));
    }

    #[test]
    fn stamp_agf_agi_agfl_round_trip() {
        let sb = test_sb(0x33, 256);
        let mut agf = vec![0u8; 512];
        stamp_agf(&mut agf, &sb);
        assert!(crc_valid(&agf, AGF_CRC_OFF));

        let mut agi = vec![0u8; 512];
        stamp_agi(&mut agi, &sb);
        assert!(crc_valid(&agi, AGI_CRC_OFF));

        let mut agfl = vec![0u8; 512];
        stamp_agfl(&mut agfl, &sb);
        assert!(crc_valid(&agfl, AGFL_CRC_OFF));
    }

    #[test]
    fn stamp_superblock_round_trips() {
        let mut sb_sector = vec![0u8; 512];
        // Some plausible non-zero bytes so an all-zero block doesn't
        // accidentally yield a CRC of 0.
        sb_sector[0..4].copy_from_slice(b"XFSB");
        stamp_superblock(&mut sb_sector);
        assert!(crc_valid(&sb_sector, SB_CRC_OFF));
    }
}
