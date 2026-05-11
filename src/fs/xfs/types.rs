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
