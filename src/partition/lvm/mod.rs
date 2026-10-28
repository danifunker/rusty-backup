//! Logical volume managers, one module per OS platform.
//!
//! Each platform keeps its own on-disk parsing (HP-UX LVM today; AIX LVM and Linux
//! LVM2 to follow) and shares only [`LogicalVolume`]: an LV as the runs of bytes it
//! occupies on this disk, in logical order. Only single-disk volume groups are
//! supported. An LV with extents on another physical volume is still listed so the
//! user can see it, but marked incomplete and refused when opened.

use serde::{Deserialize, Serialize};

pub mod hpux;

/// One logical volume, as the byte runs it occupies on this physical volume.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct LogicalVolume {
    /// Platform name, e.g. `lvol3` on HP-UX or `hd4` on AIX.
    pub name: String,
    /// Size from the LV's own extent count, whether or not every extent is here.
    pub size_bytes: u64,
    /// `(byte offset on this disk, length)` runs in logical order, adjacent extents merged.
    pub runs: Vec<(u64, u64)>,
    /// Every logical extent was found on this disk.
    pub complete: bool,
}

impl LogicalVolume {
    /// Build from `(logical extent number, byte offset)` pairs found on this disk.
    pub fn from_extents(
        name: String,
        extent_count: u64,
        extent_bytes: u64,
        mut found: Vec<(u64, u64)>,
    ) -> Self {
        found.sort_unstable();
        found.dedup_by_key(|e| e.0);
        let complete = found.len() as u64 == extent_count
            && found.iter().enumerate().all(|(i, e)| e.0 == i as u64);
        let mut runs: Vec<(u64, u64)> = Vec::new();
        for (_, off) in found {
            match runs.last_mut() {
                Some(last) if last.0 + last.1 == off => last.1 += extent_bytes,
                _ => runs.push((off, extent_bytes)),
            }
        }
        Self {
            name,
            size_bytes: extent_count * extent_bytes,
            runs,
            complete,
        }
    }

    /// Start byte when the LV is wholly on this disk in one run, so it opens like a partition.
    pub fn contiguous_start(&self) -> Option<u64> {
        match self.runs.as_slice() {
            [(start, len)] if self.complete && *len == self.size_bytes => Some(*start),
            _ => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn merges_adjacent_extents_and_flags_gaps() {
        let lv = LogicalVolume::from_extents("a".into(), 3, 10, vec![(1, 110), (0, 100), (2, 120)]);
        assert_eq!(lv.runs, vec![(100, 30)]);
        assert_eq!(lv.contiguous_start(), Some(100));

        let split = LogicalVolume::from_extents("b".into(), 2, 10, vec![(0, 100), (1, 300)]);
        assert!(split.complete);
        assert_eq!(split.contiguous_start(), None, "two runs");

        let partial = LogicalVolume::from_extents("c".into(), 3, 10, vec![(0, 100)]);
        assert!(!partial.complete, "extents on another disk");
        assert_eq!(partial.contiguous_start(), None);
    }
}
