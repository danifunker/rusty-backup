//! Moved to the `disk-core` crate; re-exported so `crate::fs::unix_common::*` paths keep working.

pub use disk_core::unix_common::{bitmap, compact, inode, validate};
