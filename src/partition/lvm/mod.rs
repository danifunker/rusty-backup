//! Logical volume managers, one module per OS platform.
//!
//! Each platform keeps its own on-disk parsing (HP-UX LVM today; AIX LVM and Linux
//! LVM2 to follow) and shares only [`LogicalVolume`]: an LV as the runs of bytes it
//! occupies on this disk, in logical order. Only single-disk volume groups are
//! supported. An LV with extents on another physical volume is still listed so the
//! user can see it, but marked incomplete and refused when opened. An LV split into
//! several runs opens through [`ExtentReader`], its runs carried in the partition's
//! type string ([`encode_runs`]), so no partition-open call site has to change.

use std::io::{self, Read, Seek, SeekFrom};

use serde::{Deserialize, Serialize};

pub mod aix;
pub mod hpux;

/// Type-string prefix for an LV made of several runs; the runs follow as `offset+len,...`.
pub const LVM_RUNS: &str = "LVM-RUNS:";

/// Encode an LV's byte runs into a partition type string.
pub fn encode_runs(runs: &[(u64, u64)]) -> String {
    let body: Vec<String> = runs.iter().map(|(o, l)| format!("{o}+{l}")).collect();
    format!("{LVM_RUNS}{}", body.join(","))
}

/// Decode the runs from an [`encode_runs`] type string.
pub fn decode_runs(type_string: &str) -> Option<Vec<(u64, u64)>> {
    type_string
        .strip_prefix(LVM_RUNS)?
        .split(',')
        .map(|r| {
            let (o, l) = r.split_once('+')?;
            Some((o.parse().ok()?, l.parse().ok()?))
        })
        .collect()
}

/// Presents an LV's runs on the disk as one contiguous, read-only volume starting at 0.
pub struct ExtentReader<R> {
    inner: R,
    runs: Vec<(u64, u64)>,
    len: u64,
    pos: u64,
}

impl<R: Read + Seek> ExtentReader<R> {
    pub fn new(inner: R, runs: Vec<(u64, u64)>) -> Self {
        let len = runs.iter().map(|r| r.1).sum();
        Self {
            inner,
            runs,
            len,
            pos: 0,
        }
    }

    /// Length of the stitched volume.
    pub fn len(&self) -> u64 {
        self.len
    }

    /// Whether the volume has no runs.
    pub fn is_empty(&self) -> bool {
        self.len == 0
    }
}

impl<R: Read + Seek> Read for ExtentReader<R> {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        let mut base = 0u64;
        for &(off, len) in &self.runs {
            if self.pos < base + len {
                let within = self.pos - base;
                let n = ((len - within) as usize).min(buf.len());
                self.inner.seek(SeekFrom::Start(off + within))?;
                let got = self.inner.read(&mut buf[..n])?;
                self.pos += got as u64;
                return Ok(got);
            }
            base += len;
        }
        Ok(0)
    }
}

impl<R: Read + Seek> Seek for ExtentReader<R> {
    fn seek(&mut self, to: SeekFrom) -> io::Result<u64> {
        let target = match to {
            SeekFrom::Start(n) => n as i128,
            SeekFrom::End(d) => self.len as i128 + d as i128,
            SeekFrom::Current(d) => self.pos as i128 + d as i128,
        };
        if target < 0 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "seek before start of LV",
            ));
        }
        self.pos = target as u64;
        Ok(self.pos)
    }
}

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
    fn extent_reader_stitches_runs_and_round_trips_the_encoding() {
        let disk: Vec<u8> = (0..=255u8).collect();
        let runs = vec![(200, 10), (10, 5), (100, 3)];
        assert_eq!(decode_runs(&encode_runs(&runs)), Some(runs.clone()));
        let mut r = ExtentReader::new(std::io::Cursor::new(disk), runs);
        assert_eq!(r.len(), 18);
        let mut all = Vec::new();
        r.read_to_end(&mut all).unwrap();
        let want: Vec<u8> = (200..210).chain(10..15).chain(100..103).collect();
        assert_eq!(all, want);
        r.seek(SeekFrom::Start(9)).unwrap();
        let mut two = [0u8; 2];
        r.read_exact(&mut two).unwrap();
        assert_eq!(two, [209, 10], "a read crosses the run boundary");
    }

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
