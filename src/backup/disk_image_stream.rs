//! Sparse-stream composer for single-file CHD backups.
//!
//! `DiskImageStream` synthesises a `Read` over a whole-disk image without
//! materialising the disk in memory or on temp storage. The caller declares
//! a total disk size and registers a sequence of byte-range "segments",
//! each backed by an arbitrary `Read`. The composer walks forward, reading
//! from the current segment when the byte position falls inside it and
//! emitting zeros everywhere else (gaps between segments, the post-last-
//! segment tail).
//!
//! This is pure plumbing — it has no knowledge of filesystems, partition
//! tables, or compaction. The caller's job is to:
//!
//!  1. Synthesise the partition-table sectors and register them as the
//!     first segment (typically `[0, header_sectors * 512)`).
//!  2. For each partition, register a segment at the partition's byte
//!     offset, sourced from a `compact_partition_reader` (smart mode) or
//!     a raw `Read` cropped to the partition extent (sector-by-sector).
//!  3. Pass the resulting `DiskImageStream` to `compress_chd`.
//!
//! The composer enforces in-order, non-overlapping segments at builder
//! time so a misconfigured caller fails fast rather than producing a
//! corrupt CHD.

use std::io::{self, Read};

/// One contiguous byte range backed by a `Read`. Byte positions outside
/// any segment's `[offset, offset+length)` are zero-filled.
struct Segment {
    offset: u64,
    length: u64,
    source: Box<dyn Read + Send>,
}

/// Builder for a [`DiskImageStream`]. Add segments in ascending offset
/// order; overlaps and out-of-order additions are rejected immediately.
pub struct DiskImageStreamBuilder {
    total_length: u64,
    segments: Vec<Segment>,
}

/// Errors surfaced by [`DiskImageStreamBuilder::add_segment`] and
/// [`DiskImageStreamBuilder::build`].
#[derive(Debug, thiserror::Error)]
pub enum DiskImageStreamError {
    #[error("segment offset {offset} is below the previous segment's end {prev_end}")]
    OutOfOrder { offset: u64, prev_end: u64 },
    #[error("segment [{offset}, {end}) extends past disk size {total}")]
    OutOfBounds { offset: u64, end: u64, total: u64 },
    #[error("segment length must be non-zero (offset {offset})")]
    ZeroLength { offset: u64 },
}

impl DiskImageStreamBuilder {
    pub fn new(total_length: u64) -> Self {
        Self {
            total_length,
            segments: Vec::new(),
        }
    }

    /// Register a segment. Must be called with strictly ascending `offset`
    /// values, must not overlap a previously-added segment, and must lie
    /// entirely within `[0, total_length)`.
    ///
    /// Per CONTRIBUTING.md the underlying source is a streaming `Read` —
    /// the composer reads forward only and never seeks.
    pub fn add_segment(
        &mut self,
        offset: u64,
        length: u64,
        source: Box<dyn Read + Send>,
    ) -> Result<(), DiskImageStreamError> {
        if length == 0 {
            return Err(DiskImageStreamError::ZeroLength { offset });
        }
        let end = offset
            .checked_add(length)
            .ok_or(DiskImageStreamError::OutOfBounds {
                offset,
                end: u64::MAX,
                total: self.total_length,
            })?;
        if end > self.total_length {
            return Err(DiskImageStreamError::OutOfBounds {
                offset,
                end,
                total: self.total_length,
            });
        }
        if let Some(last) = self.segments.last() {
            let prev_end = last.offset + last.length;
            if offset < prev_end {
                return Err(DiskImageStreamError::OutOfOrder { offset, prev_end });
            }
        }
        self.segments.push(Segment {
            offset,
            length,
            source,
        });
        Ok(())
    }

    pub fn build(self) -> DiskImageStream {
        DiskImageStream {
            total_length: self.total_length,
            segments: self.segments,
            position: 0,
            current_segment: 0,
            bytes_read_from_current: 0,
        }
    }
}

/// A forward-only `Read` over a synthesised disk image. Yields each
/// registered segment's bytes at its declared offset and zero-fills the
/// gaps. Reading past `total_length` returns EOF.
///
/// If a segment's underlying `Read` returns EOF before producing
/// `length` bytes, the remainder of the segment is zero-padded. This
/// matches `libchdman-rs`'s behavior on tail-short HD CHD inputs and
/// keeps the on-disk image at exactly the declared total length.
pub struct DiskImageStream {
    total_length: u64,
    segments: Vec<Segment>,
    position: u64,
    current_segment: usize,
    bytes_read_from_current: u64,
}

impl DiskImageStream {
    /// Total bytes the stream will yield before EOF.
    pub fn total_length(&self) -> u64 {
        self.total_length
    }
}

impl Read for DiskImageStream {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        if self.position >= self.total_length || buf.is_empty() {
            return Ok(0);
        }
        let want = (buf.len() as u64).min(self.total_length - self.position) as usize;
        let buf = &mut buf[..want];

        // Are we inside a segment, in a gap, or past the last segment?
        let region_end = if self.current_segment < self.segments.len() {
            let seg = &self.segments[self.current_segment];
            if self.position < seg.offset {
                // Gap before this segment — zero-fill up to the segment start.
                seg.offset
            } else {
                // Inside the segment — clamp to its end.
                seg.offset + seg.length
            }
        } else {
            // Past every segment — zero-fill to the disk end.
            self.total_length
        };

        let chunk = (region_end - self.position).min(buf.len() as u64) as usize;
        let chunk_buf = &mut buf[..chunk];

        let in_segment = self.current_segment < self.segments.len()
            && self.position >= self.segments[self.current_segment].offset;

        let bytes_filled = if in_segment {
            let seg = &mut self.segments[self.current_segment];
            let want_from_source =
                (seg.length - self.bytes_read_from_current).min(chunk as u64) as usize;
            let n = if want_from_source > 0 {
                seg.source.read(&mut chunk_buf[..want_from_source])?
            } else {
                0
            };
            self.bytes_read_from_current += n as u64;
            // Zero-pad if the source ran short of the declared segment length.
            if n < chunk {
                for b in &mut chunk_buf[n..chunk] {
                    *b = 0;
                }
            }
            chunk
        } else {
            // Gap or tail: pure zeros.
            for b in chunk_buf.iter_mut() {
                *b = 0;
            }
            chunk
        };

        self.position += bytes_filled as u64;

        // Did we just finish the current segment? Advance.
        if self.current_segment < self.segments.len() {
            let seg = &self.segments[self.current_segment];
            if self.position >= seg.offset + seg.length {
                self.current_segment += 1;
                self.bytes_read_from_current = 0;
            }
        }

        Ok(bytes_filled)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Cursor;

    fn read_all(mut stream: DiskImageStream) -> Vec<u8> {
        let total = stream.total_length() as usize;
        let mut out = Vec::with_capacity(total);
        // Drive Read through a small buffer to exercise multi-call boundaries
        // (one full chunk of 13 bytes is intentionally not a power of two).
        let mut buf = [0u8; 13];
        loop {
            let n = stream.read(&mut buf).unwrap();
            if n == 0 {
                break;
            }
            out.extend_from_slice(&buf[..n]);
        }
        out
    }

    #[test]
    fn empty_stream_yields_zeros() {
        let stream = DiskImageStreamBuilder::new(64).build();
        let bytes = read_all(stream);
        assert_eq!(bytes, vec![0u8; 64]);
    }

    #[test]
    fn single_segment_at_origin_then_tail_zeros() {
        let mut b = DiskImageStreamBuilder::new(32);
        b.add_segment(0, 8, Box::new(Cursor::new(vec![0xAA; 8])))
            .unwrap();
        let bytes = read_all(b.build());
        let mut want = vec![0xAA; 8];
        want.extend(vec![0u8; 24]);
        assert_eq!(bytes, want);
    }

    #[test]
    fn segment_in_middle_with_leading_and_trailing_gaps() {
        let mut b = DiskImageStreamBuilder::new(32);
        b.add_segment(8, 8, Box::new(Cursor::new(vec![0xBB; 8])))
            .unwrap();
        let bytes = read_all(b.build());
        let mut want = vec![0u8; 8];
        want.extend(vec![0xBB; 8]);
        want.extend(vec![0u8; 16]);
        assert_eq!(bytes, want);
    }

    #[test]
    fn two_segments_with_gap_interleave_correctly() {
        let mut b = DiskImageStreamBuilder::new(32);
        b.add_segment(4, 4, Box::new(Cursor::new(vec![0x11; 4])))
            .unwrap();
        b.add_segment(16, 8, Box::new(Cursor::new(vec![0x22; 8])))
            .unwrap();
        let bytes = read_all(b.build());
        let mut want = vec![0u8; 4];
        want.extend(vec![0x11; 4]);
        want.extend(vec![0u8; 8]);
        want.extend(vec![0x22; 8]);
        want.extend(vec![0u8; 8]);
        assert_eq!(bytes, want);
    }

    #[test]
    fn source_short_of_declared_length_zero_pads_remainder() {
        let mut b = DiskImageStreamBuilder::new(16);
        // Declare a 12-byte segment but only feed 4 bytes — the remaining 8
        // must be zero-padded so the total stream length is preserved.
        b.add_segment(0, 12, Box::new(Cursor::new(vec![0xCC; 4])))
            .unwrap();
        let bytes = read_all(b.build());
        let mut want = vec![0xCC; 4];
        want.extend(vec![0u8; 12]); // 8 padding + 4 tail
        assert_eq!(bytes, want);
    }

    #[test]
    fn source_longer_than_declared_length_truncates() {
        let mut b = DiskImageStreamBuilder::new(16);
        b.add_segment(0, 4, Box::new(Cursor::new(vec![0xDD; 100])))
            .unwrap();
        let bytes = read_all(b.build());
        let mut want = vec![0xDD; 4];
        want.extend(vec![0u8; 12]);
        assert_eq!(bytes, want);
    }

    #[test]
    fn out_of_order_segment_is_rejected() {
        let mut b = DiskImageStreamBuilder::new(64);
        b.add_segment(16, 8, Box::new(Cursor::new(vec![0u8; 8])))
            .unwrap();
        let err = b
            .add_segment(8, 4, Box::new(Cursor::new(vec![0u8; 4])))
            .unwrap_err();
        assert!(matches!(err, DiskImageStreamError::OutOfOrder { .. }));
    }

    #[test]
    fn overlapping_segment_is_rejected() {
        let mut b = DiskImageStreamBuilder::new(64);
        b.add_segment(0, 16, Box::new(Cursor::new(vec![0u8; 16])))
            .unwrap();
        // Starts inside the previous segment.
        let err = b
            .add_segment(8, 4, Box::new(Cursor::new(vec![0u8; 4])))
            .unwrap_err();
        assert!(matches!(err, DiskImageStreamError::OutOfOrder { .. }));
    }

    #[test]
    fn segment_past_disk_end_is_rejected() {
        let mut b = DiskImageStreamBuilder::new(32);
        let err = b
            .add_segment(28, 8, Box::new(Cursor::new(vec![0u8; 8])))
            .unwrap_err();
        assert!(matches!(err, DiskImageStreamError::OutOfBounds { .. }));
    }

    #[test]
    fn zero_length_segment_is_rejected() {
        let mut b = DiskImageStreamBuilder::new(32);
        let err = b
            .add_segment(0, 0, Box::new(Cursor::new(vec![0u8; 0])))
            .unwrap_err();
        assert!(matches!(err, DiskImageStreamError::ZeroLength { .. }));
    }

    #[test]
    fn touching_segments_with_no_gap_are_allowed() {
        // A segment ending at offset N and another starting at exactly N is
        // legal — no overlap, no gap. Required for back-to-back partitions
        // with no inter-partition spacing.
        let mut b = DiskImageStreamBuilder::new(32);
        b.add_segment(0, 8, Box::new(Cursor::new(vec![0x55; 8])))
            .unwrap();
        b.add_segment(8, 8, Box::new(Cursor::new(vec![0x66; 8])))
            .unwrap();
        let bytes = read_all(b.build());
        let mut want = vec![0x55; 8];
        want.extend(vec![0x66; 8]);
        want.extend(vec![0u8; 16]);
        assert_eq!(bytes, want);
    }
}
