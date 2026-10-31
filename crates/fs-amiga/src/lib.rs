//! Amiga filesystems, each behind its own Cargo feature.
//!
//! | Feature | Modules | Filesystem |
//! |---|---|---|
//! | `affs` | [`affs`], [`affs_fsck`] | AmigaDOS OFS / FFS (`DOS\0`..`DOS\7`): read, edit, Disk Validator check |
//! | `pfs3` | [`pfs3`], [`pfs3_fsck`], [`pfs3_clone`] | Professional File System 3 (`PFS\3`, `PDS\3`, muFS): read, edit, check, defragmenting clone |
//! | `sfs` | [`sfs`], [`sfs_fsck`] | Smart File System (`SFS\0`, `SFS\2`): read, edit, check |
//!
//! [`affs_common`] (AmigaDOS datestamps and checksums) is always built.
//! All three use **set bit = free** bitmaps and big-endian fields.

// `is_multiple_of` & co. come from disk-core's rust173 polyfill on 1.73; the real floor check is a 1.73 build.
#![allow(clippy::incompatible_msrv)]

#[cfg(feature = "affs")]
pub mod affs;
pub mod affs_common;
#[cfg(feature = "affs")]
pub mod affs_fsck;
#[cfg(feature = "pfs3")]
pub mod pfs3;
#[cfg(feature = "pfs3")]
pub mod pfs3_clone;
#[cfg(feature = "pfs3")]
pub mod pfs3_fsck;
#[cfg(feature = "sfs")]
pub mod sfs;
#[cfg(feature = "sfs")]
pub mod sfs_fsck;
