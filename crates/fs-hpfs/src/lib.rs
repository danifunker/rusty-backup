//! OS/2 HPFS, in the [`hpfs`] module: read, edit, create and check.
//!
//! GPL-2.0-only: the directory (dnode) editing follows the Linux kernel's `fs/hpfs/dnode.c`.

// `is_multiple_of` & co. come from disk-core's rust173 polyfill on 1.73; the real floor check is a 1.73 build.
#![allow(clippy::incompatible_msrv)]

pub mod hpfs;
