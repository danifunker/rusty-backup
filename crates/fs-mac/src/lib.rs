//! Classic Mac filesystems, each behind its own Cargo feature.
//!
//! | Feature | Modules | What |
//! |---|---|---|
//! | `hfs` | [`hfs`], [`hfs_fsck`], [`hfs_clone`], [`hfs_boot`], [`hfv`], [`mac_alias`] | HFS: read, edit, create, check, clone to a new block size, boot blocks, flat BasiliskII `.hfv` volumes |
//! | `mfs` | [`mfs`] | Macintosh File System (the 400K-floppy filesystem): read, edit, create |
//! | `hfsplus` | [`hfsplus`], [`hfsplus_fsck`], [`hfsplus_journal`], [`hfsplus_clone`], [`hfsplus_defrag`], [`hfsplus_wrapper_clone`] | HFS+ / HFSX: read, edit, create, check, journal replay, defragmenting clone (flat or HFS-wrapped) |
//! | `lisa` | [`lisa`] | Lisa File System, read-only, from DiskCopy 4.2 images with tags |
//! | `forks` | [`resource_fork`], [`binhex`], [`fork_export`] | resource forks, BinHex 4.0, fork-aware export |
//!
//! [`hfs_common`] (B-trees, dates, type/creator codes) is always built. Name ordering comes
//! from the GPL `fs-mac-unicode` crate. All fields are big-endian.

// `is_multiple_of` & co. come from disk-core's rust173 polyfill on 1.73; the real floor check is a 1.73 build.
#![allow(clippy::incompatible_msrv)]

#[cfg(feature = "forks")]
pub mod binhex;
#[cfg(feature = "forks")]
pub mod fork_export;
#[cfg(feature = "hfs")]
pub mod hfs;
#[cfg(feature = "hfs")]
pub mod hfs_boot;
#[cfg(feature = "hfs")]
pub mod hfs_clone;
pub mod hfs_common;
#[cfg(feature = "hfs")]
pub mod hfs_fsck;
#[cfg(feature = "hfsplus")]
pub mod hfsplus;
#[cfg(feature = "hfsplus")]
pub mod hfsplus_clone;
#[cfg(feature = "hfsplus")]
pub mod hfsplus_defrag;
#[cfg(feature = "hfsplus")]
pub mod hfsplus_fsck;
#[cfg(feature = "hfsplus")]
pub mod hfsplus_journal;
#[cfg(feature = "hfsplus")]
pub mod hfsplus_wrapper_clone;
#[cfg(feature = "hfs")]
pub mod hfv;
#[cfg(feature = "lisa")]
pub mod lisa;
#[cfg(feature = "hfs")]
pub mod mac_alias;
#[cfg(feature = "mfs")]
pub mod mfs;
#[cfg(feature = "forks")]
pub mod resource_fork;
