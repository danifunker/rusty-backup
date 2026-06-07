//! Shared on-disk constants and small types for the XFS reader.
//!
//! References:
//! - `~/xfs-efs/refs/xfs-modern/xfs/libxfs/xfs_format.h` — canonical layout.
//! - "XFS Algorithms & Data Structures" 3rd edition (SGI).

/// Superblock magic, big-endian ("XFSB").
pub const XFS_SB_MAGIC: u32 = 0x5846_5342;

/// AGF magic ("XAGF"). Parsed for completeness in Step 5.
pub const XFS_AGF_MAGIC: u32 = 0x5841_4746;

/// AGI magic ("XAGI"). Parsed for completeness in Step 5.
pub const XFS_AGI_MAGIC: u32 = 0x5841_4749;

/// Inode magic ("IN"), big-endian.
pub const XFS_DINODE_MAGIC: u16 = 0x494E;

/// Inode-allocation btree magic ("IABT", v4) and its v5/CRC variant ("IAB3").
pub const XFS_IBT_MAGIC: u32 = 0x4941_4254;
pub const XFS_IBT_CRC_MAGIC: u32 = 0x4941_4233;

/// Free-inode btree magic — v5 only ("FIBT" + CRC variant "FIB3").
/// Tracks inode chunks with at least one free inode; record format is
/// identical to the inobt's `xfs_inobt_rec`, so the same reader walks
/// it with a different magic. Present when `XFS_SB_FEAT_RO_FINOBT`
/// (bit 0 of `features_ro_compat`) is set — modern mkfs.xfs default.
#[allow(dead_code)]
pub const XFS_FIBT_MAGIC: u32 = 0x4649_4254;
pub const XFS_FIBT_CRC_MAGIC: u32 = 0x4649_4233;

/// Reverse-mapping btree magic — v5 only ("RMB3"). Records what's
/// allocated to whom for every block in the AG. Present when
/// `XFS_SB_FEAT_RO_RMAPBT` (bit 1 of `features_ro_compat`) is set —
/// modern xfsprogs 6.6.0 default.
pub const XFS_RMAP_CRC_MAGIC: u32 = 0x524d_4233;

/// Reflink refcount btree magic — v5 only ("R3FC"). Tracks per-extent
/// reference counts for reflink-shared data. Present when
/// `XFS_SB_FEAT_RO_REFLINK` (bit 2 of `features_ro_compat`) is set —
/// modern mkfs.xfs default (refcountbt allocated but empty unless
/// reflinks have been created). Note: older kernel headers list
/// `R3C ` (0x52334320); xfsprogs 6.x writes `R3FC` (0x52334643), which
/// is what we cross-check against on the v5 modern fixture.
pub const XFS_REFC_CRC_MAGIC: u32 = 0x5233_4643;

/// Free-space btree magics: by-block ("ABTB"/"AB3B") and by-count
/// ("ABTC"/"AB3C"). Used by the allocation-walk phase.
pub const XFS_ABTB_MAGIC: u32 = 0x4142_5442;
pub const XFS_ABTB_CRC_MAGIC: u32 = 0x4142_3342;
pub const XFS_ABTC_MAGIC: u32 = 0x4142_5443;
pub const XFS_ABTC_CRC_MAGIC: u32 = 0x4142_3343;

/// `features_ro_compat` feature bits the verifier / repair pipeline
/// needs to recognise. Read-only-compat means we can read the FS
/// without supporting these features; writes / repairs that don't
/// model them risk leaving them stale.
pub const XFS_SB_FEAT_RO_FINOBT: u32 = 1 << 0;
pub const XFS_SB_FEAT_RO_RMAPBT: u32 = 1 << 1;
pub const XFS_SB_FEAT_RO_REFLINK: u32 = 1 << 2;
#[allow(dead_code)]
pub const XFS_SB_FEAT_RO_INOBTCNT: u32 = 1 << 3;

/// Short-form (AG-relative) btree block header length. v4 is
/// magic(4)+level(2)+numrecs(2)+leftsib(4)+rightsib(4) = 16; v5/CRC adds
/// blkno(8)+lsn(8)+uuid(16)+owner(4)+crc(4) = +40 → 56.
pub const XFS_BTREE_SBLOCK_LEN: usize = 16;
pub const XFS_BTREE_SBLOCK_CRC_LEN: usize = 56;

/// Sentinel AG-relative block pointer ("no sibling / no child").
pub const NULLAGBLOCK: u32 = 0xFFFF_FFFF;

/// Inodes per inobt record / chunk (the `ir_free` bitmask is 64 bits).
pub const XFS_INODES_PER_CHUNK: usize = 64;

/// Low nibble of `sb_versionnum` carries the format version.
pub const XFS_SB_VERSION_NUMBITS: u16 = 0x000F;
pub const XFS_SB_VERSION_4: u16 = 4;
pub const XFS_SB_VERSION_5: u16 = 5;

/// `sb_versionnum & DIRV2BIT` selects dir2 over dir1.
pub const XFS_SB_VERSION_DIRV2BIT: u16 = 0x2000;

/// XFS volume label fixed width.
pub const XFS_LABEL_MAX: usize = 12;

/// Selected directory format. Surfaced on `XfsFilesystem` so list/read code
/// can dispatch even though Step 5 only stubs out the actual parsers.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DirFormat {
    Dir1,
    Dir2,
}

/// Inode `di_format` values. Only the variants the reader handles are named;
/// everything else flows through `Other`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DiFormat {
    /// 1 — fork data inline (small dirs, symlinks, devs).
    Local,
    /// 2 — fork data is a packed extent list.
    Extents,
    /// 3 — fork data is a bmap btree root.
    Btree,
    Other(u8),
}

impl DiFormat {
    pub fn from_raw(v: u8) -> Self {
        match v {
            1 => DiFormat::Local,
            2 => DiFormat::Extents,
            3 => DiFormat::Btree,
            other => DiFormat::Other(other),
        }
    }
}
