//! Helpers the Unix-family drivers share: used-block bitmaps and compaction.
//! (The inode helpers other families use too live in `disk_core::unix_common::inode`.)

pub mod bitmap;
pub mod compact;
