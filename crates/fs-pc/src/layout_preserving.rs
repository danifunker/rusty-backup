//! `LayoutPreservingReader`: a forward-only `Read` that streams a partition's
//! bytes verbatim except inside caller-supplied "zero ranges", which are
//! emitted as zero-fill without touching the source.
//!
//! Used by the single-file CHD backup pipeline to produce a stream whose
//! length equals the partition's source size and whose allocated regions
//! match the source byte-for-byte. Free filesystem regions zero out into
//! CHD's compressed-zero hunks at no storage cost.
//!
//! The per-FS factories live in `fat.rs`, `ntfs.rs`, `exfat.rs` — each
//! consumes its existing `Compact*Reader` (which has already parsed the
//! allocation map) and converts the allocated-cluster set into the sorted
//! zero-range list this reader expects.

use std::io::{self, Read, Seek, SeekFrom};

/// Sorted, non-overlapping byte ranges (relative to the start of the
/// partition) that should be emitted as zeros instead of source bytes.
type ZeroRange = (u64, u64); // (start_in_partition, length)

/// Forward-only `Read` over a partition that interleaves source bytes
/// with zero-fill regions per a precomputed plan.
pub struct LayoutPreservingReader<R> {
    source: R,
    partition_offset: u64,
    total_size: u64,
    zero_ranges: Vec<ZeroRange>,

    /// Current output position relative to the start of the partition
    /// (i.e. always in `[0, total_size]`).
    position: u64,
    /// Index into `zero_ranges` of the next range whose `end` is greater
    /// than `position`. Monotonically non-decreasing as the stream
    /// advances.
    next_zero_idx: usize,
    /// Whether `source` is currently positioned at `partition_offset +
    /// position`. False after a zero-range emission, since we don't
    /// touch the source while filling zeros.
    source_aligned: bool,
}

impl<R: Read + Seek> LayoutPreservingReader<R> {
    /// Build a new reader. `zero_ranges` must be sorted by start offset,
    /// non-overlapping, and entirely within `[0, total_size)`. Empty
    /// ranges (length 0) are filtered out for caller convenience.
    pub fn new(
        source: R,
        partition_offset: u64,
        total_size: u64,
        mut zero_ranges: Vec<ZeroRange>,
    ) -> io::Result<Self> {
        zero_ranges.retain(|(_, len)| *len > 0);
        // Defensive validation — a wrong free-range list silently
        // produces a corrupt stream; better to fail loudly during
        // construction.
        for w in zero_ranges.windows(2) {
            let (a_start, a_len) = w[0];
            let (b_start, _) = w[1];
            if a_start + a_len > b_start {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    format!(
                        "LayoutPreservingReader: overlapping zero ranges at {a_start}..{} \
                         and {b_start}",
                        a_start + a_len
                    ),
                ));
            }
        }
        if let Some((start, len)) = zero_ranges.last() {
            if start + len > total_size {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    format!(
                        "LayoutPreservingReader: zero range {start}..{} exceeds total_size {total_size}",
                        start + len
                    ),
                ));
            }
        }

        Ok(Self {
            source,
            partition_offset,
            total_size,
            zero_ranges,
            position: 0,
            next_zero_idx: 0,
            source_aligned: false,
        })
    }

    fn current_zero_range(&mut self) -> Option<ZeroRange> {
        // Skip any zero ranges that ended before the current position.
        while self.next_zero_idx < self.zero_ranges.len() {
            let (start, len) = self.zero_ranges[self.next_zero_idx];
            if start + len <= self.position {
                self.next_zero_idx += 1;
                continue;
            }
            return Some((start, len));
        }
        None
    }
}

impl<R: Read + Seek> Read for LayoutPreservingReader<R> {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        if self.position >= self.total_size || buf.is_empty() {
            return Ok(0);
        }
        let max = ((self.total_size - self.position) as usize).min(buf.len());

        // Inside a zero range? Emit zeros up to `min(end_of_range, buf
        // capacity, total_size)`.
        if let Some((zr_start, zr_len)) = self.current_zero_range() {
            if self.position >= zr_start && self.position < zr_start + zr_len {
                let zr_end = zr_start + zr_len;
                let n = ((zr_end - self.position) as usize).min(max);
                buf[..n].fill(0);
                self.position += n as u64;
                self.source_aligned = false;
                return Ok(n);
            }
        }

        // Source region: read up to the next zero-range boundary (or
        // EOF), whichever is sooner. We re-seek the source explicitly
        // whenever we last emitted zeros — interleaving zero fill with
        // source reads otherwise leaves the underlying file offset out
        // of sync with `self.position`.
        let next_boundary = self
            .current_zero_range()
            .map(|(s, _)| s)
            .unwrap_or(self.total_size);
        let source_window = ((next_boundary - self.position) as usize).min(max);
        if source_window == 0 {
            // Empty source window means we hit a zero range starting
            // exactly at the current position; loop in caller.
            return Ok(0);
        }

        if !self.source_aligned {
            self.source
                .seek(SeekFrom::Start(self.partition_offset + self.position))?;
            self.source_aligned = true;
        }

        let n = self.source.read(&mut buf[..source_window])?;
        if n == 0 && source_window > 0 {
            // Source ended early — zero-fill the rest of the window so
            // callers always see a stream of length total_size. This
            // matches the behavior of DiskImageStream's source-short
            // padding and avoids surprising truncation when the source
            // file's logical size lags behind the partition extent.
            buf[..source_window].fill(0);
            self.position += source_window as u64;
            self.source_aligned = false;
            return Ok(source_window);
        }
        self.position += n as u64;
        Ok(n)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Cursor;

    #[test]
    fn no_zero_ranges_passthrough() {
        let src = (0..16).collect::<Vec<u8>>();
        let mut r = LayoutPreservingReader::new(Cursor::new(src.clone()), 0, 16, vec![]).unwrap();
        let mut out = Vec::new();
        r.read_to_end(&mut out).unwrap();
        assert_eq!(out, src);
    }

    #[test]
    fn zero_range_in_middle() {
        let src: Vec<u8> = (1..=16).collect(); // 1..=16
        let mut r = LayoutPreservingReader::new(Cursor::new(src), 0, 16, vec![(4, 4)]).unwrap();
        let mut out = Vec::new();
        r.read_to_end(&mut out).unwrap();
        // bytes 0..4 = 1..4, bytes 4..8 = zero, bytes 8..16 = 9..16
        assert_eq!(out[..4], [1, 2, 3, 4]);
        assert_eq!(out[4..8], [0, 0, 0, 0]);
        assert_eq!(out[8..], [9, 10, 11, 12, 13, 14, 15, 16]);
    }

    #[test]
    fn zero_range_at_start_and_end() {
        let src: Vec<u8> = (1..=16).collect();
        let mut r =
            LayoutPreservingReader::new(Cursor::new(src), 0, 16, vec![(0, 2), (14, 2)]).unwrap();
        let mut out = Vec::new();
        r.read_to_end(&mut out).unwrap();
        assert_eq!(out[..2], [0, 0]);
        assert_eq!(out[2..14], [3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14]);
        assert_eq!(out[14..], [0, 0]);
    }

    #[test]
    fn partition_offset_seeks_source() {
        let mut src = vec![0u8; 32];
        for (i, slot) in src.iter_mut().enumerate() {
            *slot = i as u8;
        }
        // Partition starts at offset 8, length 16 → output should be 8..24.
        let mut r = LayoutPreservingReader::new(Cursor::new(src), 8, 16, vec![]).unwrap();
        let mut out = Vec::new();
        r.read_to_end(&mut out).unwrap();
        assert_eq!(out, (8u8..24).collect::<Vec<_>>());
    }

    #[test]
    fn rejects_overlapping_ranges() {
        let src = vec![0u8; 32];
        let r = LayoutPreservingReader::new(Cursor::new(src), 0, 32, vec![(0, 8), (4, 4)]);
        assert!(r.is_err());
    }

    #[test]
    fn rejects_range_past_end() {
        let src = vec![0u8; 16];
        let r = LayoutPreservingReader::new(Cursor::new(src), 0, 16, vec![(8, 16)]);
        assert!(r.is_err());
    }

    #[test]
    fn short_source_zero_pads_tail() {
        // Partition declared as 16 bytes but source has only 8.
        let src = vec![1u8; 8];
        let mut r = LayoutPreservingReader::new(Cursor::new(src), 0, 16, vec![]).unwrap();
        let mut out = Vec::new();
        r.read_to_end(&mut out).unwrap();
        assert_eq!(out[..8], [1; 8]);
        assert_eq!(out[8..], [0; 8]);
    }
}
