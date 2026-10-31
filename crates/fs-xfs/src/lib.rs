//! XFS, in the [`xfs`] module: read (IRIX v4 dir1/dir2 through Linux v5 with CRCs),
//! edit, fsck, and an `xfs_repair`-style repair.
//!
//! GPL-2.0-only: the directory name hash follows xfsprogs' `xfs_da_hashname`, and
//! the repair phases use `xfs_repair` as their behavioural reference.

// `is_multiple_of` & co. come from disk-core's rust173 polyfill on 1.73; the real floor check is a 1.73 build.
#![allow(clippy::incompatible_msrv)]

pub mod xfs;
