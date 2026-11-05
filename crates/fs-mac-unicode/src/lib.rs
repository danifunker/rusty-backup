//! HFS / HFS+ catalog-name ordering, split out of `fs-mac` because its tables are derived from GPL sources.
//!
//! - [`hfs_unicode`]: Apple's HFS+ case-fold table and canonical decomposition (TN1150), as the
//!   Linux kernel's `fs/hfsplus/{tables.c,unicode.c}` encode them (GPL-2.0-only).
//! - [`collate`]: `compare_hfsplus_keys` on top of those tables, and `compare_hfs_keys` with the
//!   Mac Roman `HFS_CHARORDER` table from hfsutils (GPL-2.0-or-later).

pub mod collate;
pub mod hfs_unicode;
