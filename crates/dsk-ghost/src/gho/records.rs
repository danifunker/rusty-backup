//! The inner record stream: record headers and the iterator over them.

use super::*;

// ---------------------------------------------------------------------------
// Inner record stream (session 5.5b)
// ---------------------------------------------------------------------------

/// Magic that precedes every record's body, little-endian (`0x012F18D8`).
/// Bytes on disk: `D8 18 2F 01`.
pub const GHO_RECORD_MAGIC: u32 = 0x012F_18D8;

/// On-disk byte length of a record header (`u16 type | u16 marker | u32
/// magic | u16 body_len`).
pub const GHO_RECORD_HEADER_LEN: usize = 10;

/// Maximum bytes we'll scan past the container header looking for the first
/// inner-stream record magic. In our corpus the inner stream starts well
/// before 16 KiB (7.5 at 0xC00, 11.5 at 0x1600). Cap at 64 KiB to bound
/// pathological cases.
pub const GHO_INNER_STREAM_SCAN_LIMIT: u64 = 64 * 1024;

/// Single record header.
///
/// Layout reverse-engineered from our fixture corpus:
///
/// ```text
/// offset  size  field
/// 0       2     type        u16 LE — record kind (e.g. 0x0017 = first
///                            sector / BPB block, 0x0004 = FAT-entry-style
///                            payload). Exact taxonomy lives in 5.6.
/// 2       2     marker      u16 LE — 0x0000 on Ghost 7.5, 0x95FD on Ghost
///                            11.5 across every fixture in our corpus.
///                            Likely a per-version checksum/discriminator.
///                            Surfaced raw; not used for dispatch.
/// 4       4     magic       u32 LE — always 0x012F18D8 (sanity check).
/// 8       2     body_len    u16 LE — number of body bytes that follow.
/// ```
///
/// The 7.5-vs-11.5 record stream is identical at (type, body_len) level —
/// only `marker` differs. Confirmed by `record_streams_match_between_75_and_115`
/// against `7.5/PART/PART.GHO` and `11.5/GH11/fulldisk.GHS`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct GhoRecordHeader {
    /// Record kind. Raw u16; meanings catalogued by 5.6.
    pub type_code: u16,
    /// Per-version marker. `0x0000` on 7.5, `0x95FD` on 11.5.
    pub marker: u16,
    /// Body length in bytes (does NOT include this header).
    pub body_len: u16,
}

impl GhoRecordHeader {
    /// Parse a record header from a 10-byte slice. Returns `Err` if the
    /// magic doesn't match (caller should treat that as end-of-stream).
    pub fn parse_bytes(bytes: &[u8]) -> Result<Self> {
        if bytes.len() < GHO_RECORD_HEADER_LEN {
            bail!(
                "GHO record header needs {} bytes, got {}",
                GHO_RECORD_HEADER_LEN,
                bytes.len()
            );
        }
        let type_code = u16::from_le_bytes([bytes[0], bytes[1]]);
        let marker = u16::from_le_bytes([bytes[2], bytes[3]]);
        let magic = u32::from_le_bytes([bytes[4], bytes[5], bytes[6], bytes[7]]);
        if magic != GHO_RECORD_MAGIC {
            bail!(
                "GHO record magic at expected offset is {:#010x}, expected {:#010x}",
                magic,
                GHO_RECORD_MAGIC
            );
        }
        let body_len = u16::from_le_bytes([bytes[8], bytes[9]]);
        Ok(Self {
            type_code,
            marker,
            body_len,
        })
    }

    /// Read a record header from `reader` at its current position. Returns
    /// `Ok(None)` on clean end-of-file, `Err` on malformed data.
    pub fn read_from<R: Read>(reader: &mut R) -> Result<Option<Self>> {
        let mut buf = [0u8; GHO_RECORD_HEADER_LEN];
        match reader.read_exact(&mut buf) {
            Ok(()) => {}
            Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => return Ok(None),
            Err(e) => return Err(e.into()),
        }
        Self::parse_bytes(&buf).map(Some)
    }
}

/// Locate the start of the inner record stream by scanning forward from
/// `header_end` for the record magic `0xD8182F01`.
///
/// Ghost pads the container metadata region to a different sector count
/// on 7.5 vs 11.5 (sector 6 vs sector 11 in our corpus), so we don't
/// hardcode the offset — the scan-forward approach absorbs the version
/// difference without per-version code.
///
/// `header_end` is the byte offset where parsing should resume — typically
/// `GHO_HEADER_PREFIX_LEN` (12) when the file has no password verifier,
/// `GHO_HEADER_PREFIX_LEN + GHO_PASSWORD_VERIFIER_LEN` (28) when it does.
pub fn find_inner_stream_start<R: Read + Seek>(reader: &mut R, header_end: u64) -> Result<u64> {
    let needle = GHO_RECORD_MAGIC.to_le_bytes();
    let mut buf = vec![0u8; 4096];
    let mut scanned: u64 = 0;
    let mut window_start = header_end;
    reader
        .seek(SeekFrom::Start(header_end))
        .context("seeking past container header")?;

    // Carry the last 3 bytes of the prior chunk so we don't miss a
    // magic that straddles a chunk boundary.
    let mut tail: Vec<u8> = Vec::new();
    while scanned < GHO_INNER_STREAM_SCAN_LIMIT {
        let n = reader.read(&mut buf)?;
        if n == 0 {
            break;
        }
        // Build a scan slice = tail + new chunk.
        let mut scan = Vec::with_capacity(tail.len() + n);
        scan.extend_from_slice(&tail);
        scan.extend_from_slice(&buf[..n]);

        if let Some(pos) = scan.windows(needle.len()).position(|w| w == needle) {
            // Magic starts at byte (window_start + pos - tail.len()),
            // but the record HEADER starts 4 bytes earlier (type+marker).
            let magic_offset = window_start + pos as u64 - tail.len() as u64;
            let header_offset = magic_offset.saturating_sub(4);
            return Ok(header_offset);
        }

        // Keep last (needle.len() - 1) bytes for the next iteration.
        let keep = needle.len() - 1;
        if scan.len() >= keep {
            tail = scan[scan.len() - keep..].to_vec();
        } else {
            tail = scan;
        }
        window_start += n as u64;
        scanned += n as u64;
    }
    Err(anyhow!(
        "GHO inner-stream magic not found within {} bytes of container header",
        GHO_INNER_STREAM_SCAN_LIMIT
    ))
}

/// Walk the inner record stream (reading only the plaintext 10-byte headers)
/// and collect every record's body range `(body_start, body_len)`.
///
/// Used to build the decryption map for password-protected images: each body
/// is independently encrypted, so we need the exact byte ranges to decrypt.
/// Records with a zero-length body are skipped (nothing to decrypt).
pub(super) fn collect_gho_body_ranges<R: Read + Seek>(
    reader: &mut R,
    header_end: u64,
    file_size: u64,
) -> Result<Vec<(u64, u32)>> {
    // Mirror `parse_gho_image`'s walk so the body map covers exactly the
    // records the real (decrypted) decode pass will read: bound by file size,
    // resync on a magic mismatch, stop at a clean end.
    let mut offset = find_inner_stream_start(&mut *reader, header_end)
        .context("locating inner record stream for encrypted GHO")?;
    let mut bodies = Vec::new();
    while offset + GHO_RECORD_HEADER_LEN as u64 <= file_size {
        reader.seek(SeekFrom::Start(offset))?;
        let rec = match GhoRecordHeader::read_from(reader) {
            Ok(Some(r)) => r,
            Ok(None) => break,
            Err(_) => match find_inner_stream_start(&mut *reader, offset) {
                Ok(o) if o > offset && o < file_size => {
                    offset = o;
                    continue;
                }
                _ => break,
            },
        };
        let body_start = offset + GHO_RECORD_HEADER_LEN as u64;
        if rec.body_len > 0 {
            bodies.push((body_start, rec.body_len as u32));
        }
        offset = body_start + rec.body_len as u64;
    }
    Ok(bodies)
}

/// Iterator that walks the records in a Ghost backup's inner stream.
///
/// Each `next()` reads a 10-byte header, validates the magic, then **skips
/// past the body** (using `Seek::seek` — bodies can be 512 bytes of FAT
/// data and we don't need to materialize them here in 5.5b). The body is
/// not returned; callers that need it should re-seek to
/// `current_offset()` and read `header.body_len` bytes themselves. The
/// 5.6 decoder will swap this iterator for a streaming variant that
/// returns body bytes.
pub struct GhoRecordIter<R: Read + Seek> {
    pub(super) reader: R,
    /// Next-record byte offset within `reader`.
    pub(super) next_offset: u64,
    /// Records read so far (diagnostics).
    pub(super) count: usize,
}

impl<R: Read + Seek> GhoRecordIter<R> {
    /// Build an iterator that starts reading at `start_offset` in `reader`.
    pub fn new(mut reader: R, start_offset: u64) -> Result<Self> {
        reader.seek(SeekFrom::Start(start_offset))?;
        Ok(Self {
            reader,
            next_offset: start_offset,
            count: 0,
        })
    }

    /// Byte offset within the source reader where the next record starts.
    pub fn current_offset(&self) -> u64 {
        self.next_offset
    }

    /// Records yielded so far. Named `records_read` (not `count`) to
    /// avoid shadowing the `Iterator::count()` trait method, which
    /// consumes the iterator.
    pub fn records_read(&self) -> usize {
        self.count
    }
}

impl<R: Read + Seek> Iterator for GhoRecordIter<R> {
    type Item = Result<GhoRecordHeader>;

    fn next(&mut self) -> Option<Self::Item> {
        if let Err(e) = self.reader.seek(SeekFrom::Start(self.next_offset)) {
            return Some(Err(e.into()));
        }
        let header = match GhoRecordHeader::read_from(&mut self.reader) {
            Ok(Some(h)) => h,
            Ok(None) => return None,
            Err(e) => return Some(Err(e)),
        };
        let advance = GHO_RECORD_HEADER_LEN as u64 + header.body_len as u64;
        self.next_offset += advance;
        self.count += 1;
        Some(Ok(header))
    }
}
