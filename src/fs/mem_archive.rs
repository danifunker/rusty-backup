//! Moved to the `fs-tar` crate (features `mem-archive`, `zip`); re-exported so `crate::fs::mem_archive` paths keep working.

pub use fs_tar::mem_archive::*;
