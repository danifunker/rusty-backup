//! Unix-family filesystems, each behind its own Cargo feature.
//!
//! | Feature | Modules | Filesystem |
//! |---|---|---|
//! | `ext` | [`ext`], [`ext_csum`], [`ext_format`], [`ext_fsck`] | ext2 / ext3 / ext4: read, edit, create, check |
//! | `ufs` | [`ufs`], [`ufs_format`], [`ufs_fsck`] | UFS1 / UFS2 / 4.3BSD FFS, either endianness: read, edit, create, check |
//! | `efs` | [`efs`], [`efs_fsck`], [`efs_resize`], [`efs_v1`], [`efs_v1_fsck`], [`efs_v1_resize`] | SGI EFS (IRIX) and the IRIS 3000 EFS v1: read, edit, check, resize |
//! | `jfs` | [`jfs`], [`jfs_fsck`], [`jfs1`] | JFS2 (Linux / OS/2 / AIX 5): read, edit, check; AIX JFS1, read-only |
//! | `btrfs` | [`btrfs`] | btrfs, read-only |
//! | `reiserfs` | [`reiserfs`] | ReiserFS 3.5 / 3.6, read-only |
//! | `vxfs` | [`vxfs`], [`vxfs_edit`] | Veritas VxFS (HP-UX, UnixWare): read, edit |
//! | `minix` | [`minix`], [`minix_fsck`] | Minix V1 / V2 / V3: read, edit, create, check |
//! | `bfs` | [`bfs`], [`bfs_fsck`], [`bfs_write`] | BeOS / Haiku BFS: read, edit, create, check |

// `is_multiple_of` & co. come from disk-core's rust173 polyfill on 1.73; the real floor check is a 1.73 build.
#![allow(clippy::incompatible_msrv)]

#[cfg(feature = "bfs")]
pub mod bfs;
#[cfg(feature = "bfs")]
pub mod bfs_fsck;
#[cfg(feature = "bfs")]
pub mod bfs_write;
#[cfg(feature = "btrfs")]
pub mod btrfs;
#[cfg(feature = "efs")]
pub mod efs;
#[cfg(feature = "efs")]
pub mod efs_fsck;
#[cfg(feature = "efs")]
pub mod efs_resize;
#[cfg(feature = "efs")]
pub mod efs_v1;
#[cfg(feature = "efs")]
pub mod efs_v1_fsck;
#[cfg(feature = "efs")]
pub mod efs_v1_resize;
#[cfg(feature = "ext")]
pub mod ext;
#[cfg(feature = "ext")]
pub mod ext_csum;
#[cfg(feature = "ext")]
pub mod ext_format;
#[cfg(feature = "ext")]
pub mod ext_fsck;
#[cfg(feature = "jfs")]
pub mod jfs;
#[cfg(feature = "jfs")]
pub mod jfs1;
#[cfg(feature = "jfs")]
pub mod jfs_fsck;
#[cfg(feature = "minix")]
pub mod minix;
#[cfg(feature = "minix")]
pub mod minix_fsck;
#[cfg(feature = "reiserfs")]
pub mod reiserfs;
#[cfg(feature = "ufs")]
pub mod ufs;
#[cfg(feature = "ufs")]
pub mod ufs_format;
#[cfg(feature = "ufs")]
pub mod ufs_fsck;
#[cfg(feature = "vxfs")]
pub mod vxfs;
#[cfg(feature = "vxfs")]
pub mod vxfs_edit;
