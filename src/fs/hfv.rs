//! Moved to the `fs-mac` crate; re-exported so `crate::fs::hfv` paths keep working.

pub use fs_mac::hfv::*;

#[cfg(test)]
mod tests {
    use super::*;
    use crate::partition::PartitionTable;
    use std::io::Cursor;

    #[test]
    fn a_blank_hfv_detects_as_an_hfs_superfloppy() {
        let img = build_blank_hfv(
            16 * 1024 * 1024,
            suggest_block_size(16 * 1024 * 1024),
            "BlankHFV",
        )
        .unwrap();
        // Re-detects as a partition-less HFS superfloppy.
        let mut cur = Cursor::new(img);
        match PartitionTable::detect(&mut cur).unwrap() {
            PartitionTable::None { fs_hint, .. } => assert_eq!(fs_hint, "HFS"),
            other => panic!("expected HFS superfloppy, got {other:?}"),
        }
    }
}
