//! PC filesystems, each behind its own Cargo feature.
//!
//! | Feature | Modules | Filesystem |
//! |---|---|---|
//! | `fat` | [`fat`], [`fat_fsck`] | FAT12 / FAT16 / FAT32 with VFAT long names: read, edit, create, check, resize, compact |
//! | `exfat` | [`exfat`], [`exfat_fsck`], [`exfat_clone`] | exFAT: read, edit, create, check, resize, defragmenting clone |
//! | `ntfs` | [`ntfs`], [`ntfs_fsck`], [`ntfs_format`], [`ntfs_clone`] | NTFS: read, edit, create, check, resize, defragmenting clone |
//! | `human68k` | [`human68k`], [`human68k_clone`] | Sharp X68000 Human68k: read, edit, check, resize, clone |
//!
//! [`patch`] (boot-sector field patching) and [`layout_preserving`] (a reader that
//! zero-fills free clusters) are always built. HPFS lives in the GPL `fs-hpfs` crate.

// `is_multiple_of` & co. come from disk-core's rust173 polyfill on 1.73; the real floor check is a 1.73 build.
#![allow(clippy::incompatible_msrv)]

#[cfg(feature = "exfat")]
pub mod exfat;
#[cfg(feature = "exfat")]
pub mod exfat_clone;
#[cfg(feature = "exfat")]
pub mod exfat_fsck;
#[cfg(feature = "fat")]
pub mod fat;
#[cfg(feature = "fat")]
pub mod fat_fsck;
#[cfg(feature = "human68k")]
pub mod human68k;
#[cfg(feature = "human68k")]
pub mod human68k_clone;
pub mod layout_preserving;
#[cfg(feature = "ntfs")]
pub mod ntfs;
#[cfg(feature = "ntfs")]
pub mod ntfs_clone;
#[cfg(feature = "ntfs")]
pub mod ntfs_format;
#[cfg(feature = "ntfs")]
pub mod ntfs_fsck;
#[cfg(feature = "ntfs")]
mod ntfs_tables;
pub mod patch;
