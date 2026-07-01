//! DART (Disk Archive/Retrieval Tool) — Apple's compressed disk-image container,
//! contemporary with DiskCopy 4.2 and used to distribute Mac / Lisa / Apple II /
//! MS-DOS floppies internally at Apple.
//!
//! A DART file is a small header followed by fixed 20960-byte chunks, each of
//! which holds **40 512-byte blocks followed by 40 12-byte tags** — so, unlike
//! plain DiskCopy, DART preserves the sector tags that the Lisa filesystem
//! depends on. Each chunk is compressed independently with one of three schemes:
//! "fast" (word RLE), "best" (LZHUF), or "none". Decoding yields a `(data, tags)`
//! pair: Lisa images (`src_type == 2`) feed [`crate::fs::lisa::LisaFilesystem`],
//! everything else feeds the normal filesystem detection on the data region.
//!
//! Format reference: `DART-notes.md` (CiderPress2, Apache-2.0); this is a
//! clean-room implementation from that specification. All fields are big-endian.

use std::io::{Read, Seek, SeekFrom};

use super::lzhuf::lzhuf_decompress;

/// 512-byte blocks per chunk.
pub const CHUNK_BLOCKS: usize = 40;
/// Bytes of block data per chunk (40 * 512).
pub const CHUNK_DATA_LEN: usize = 512 * CHUNK_BLOCKS;
/// Bytes of tag data per chunk (40 * 12).
pub const CHUNK_TAG_LEN: usize = 12 * CHUNK_BLOCKS;
/// Total decompressed bytes per chunk.
pub const CHUNK_TOTAL_LEN: usize = CHUNK_DATA_LEN + CHUNK_TAG_LEN; // 20960

const SRC_CMP_FAST: u8 = 0;
const SRC_CMP_BEST: u8 = 1;
const SRC_CMP_NONE: u8 = 2;
/// Per-chunk marker: this chunk is stored uncompressed.
const BLEN_UNCOMPRESSED: u16 = 0xffff;

/// Parsed DART header.
#[derive(Debug, Clone)]
pub struct DartHeader {
    /// Compression: 0=fast (RLE), 1=best (LZHUF), 2=none.
    pub src_cmp: u8,
    /// Disk kind: 1=Mac, 2=Lisa, 3=Apple II, 16=Mac 1440K, 17=DOS 720K, 18=DOS 1440K.
    pub src_type: u8,
    /// Source disk size in KiB (e.g. 400, 800, 1440).
    pub src_size: u16,
    /// Per-chunk compressed lengths (40 entries for <=800K, 72 for 1440K).
    pub blengths: Vec<u16>,
}

impl DartHeader {
    /// Number of 512-byte blocks in the whole disk (`src_size` KiB * 2).
    pub fn numblocks(&self) -> usize {
        self.src_size as usize * 2
    }

    /// Number of real (non-padding) chunks.
    pub fn num_chunks(&self) -> usize {
        self.numblocks() / CHUNK_BLOCKS
    }

    /// Header size in bytes: 4 + array.
    pub fn header_len(&self) -> usize {
        4 + self.blengths.len() * 2
    }

    /// Compressed byte length of chunk `i` as it sits in the file.
    fn chunk_stored_len(&self, i: usize) -> usize {
        let bl = self.blengths[i];
        if bl == BLEN_UNCOMPRESSED || self.src_cmp == SRC_CMP_NONE {
            CHUNK_TOTAL_LEN
        } else if self.src_cmp == SRC_CMP_FAST {
            bl as usize * 2 // fast lengths are in 16-bit words
        } else {
            bl as usize // best lengths are in bytes
        }
    }
}

/// A decoded DART image: the concatenated block data and tag data, plus the
/// source disk identity. `data` is `numblocks * 512`, `tags` is `numblocks * 12`.
pub struct DartImage {
    pub data: Vec<u8>,
    pub tags: Vec<u8>,
    pub src_type: u8,
    pub src_size: u16,
}

impl DartImage {
    /// True for a Lisa disk (`src_type == 2`).
    pub fn is_lisa(&self) -> bool {
        self.src_type == 2
    }

    /// True if any tag byte is non-zero (Mac disks tag-fill with zeros).
    pub fn has_nonzero_tags(&self) -> bool {
        self.tags.iter().any(|&b| b != 0)
    }
}

fn be16(b: &[u8], o: usize) -> u16 {
    ((b[o] as u16) << 8) | (b[o + 1] as u16)
}

/// Parse a DART header from the first bytes of a file. Returns `None` if the
/// fields don't look like DART.
pub fn parse_dart_header(buf: &[u8]) -> Option<DartHeader> {
    if buf.len() < 4 {
        return None;
    }
    let src_cmp = buf[0];
    let src_type = buf[1];
    let src_size = be16(buf, 2);
    if src_cmp > 2 {
        return None;
    }
    if !matches!(src_type, 1 | 2 | 3 | 16 | 17 | 18) {
        return None;
    }
    // Known Apple floppy sizes; each must be a whole number of 40-block chunks.
    if !matches!(src_size, 400 | 720 | 800 | 1440) {
        return None;
    }
    let array_len = if src_size <= 800 { 40 } else { 72 };
    let need = 4 + array_len * 2;
    if buf.len() < need {
        return None;
    }
    let blengths = (0..array_len).map(|k| be16(buf, 4 + k * 2)).collect();
    Some(DartHeader {
        src_cmp,
        src_type,
        src_size,
        blengths,
    })
}

/// Detect a DART image by validating that the header plus every chunk's stored
/// length exactly accounts for `file_size`. `head` must contain at least the
/// full header (<= 148 bytes). Strong enough to distinguish DART from other
/// headerless images, since DART has no magic number.
pub fn detect_dart(head: &[u8], file_size: u64) -> Option<DartHeader> {
    let hdr = parse_dart_header(head)?;
    let mut total = hdr.header_len() as u64;
    for i in 0..hdr.num_chunks() {
        if hdr.blengths[i] == 0 {
            return None; // a real chunk should never have length 0
        }
        total += hdr.chunk_stored_len(i) as u64;
    }
    if total == file_size {
        Some(hdr)
    } else {
        None
    }
}

/// Decode a DART file (given the whole file bytes) into a [`DartImage`].
pub fn decode_dart_bytes(buf: &[u8]) -> Result<DartImage, String> {
    let hdr = parse_dart_header(buf).ok_or_else(|| "not a DART header".to_string())?;
    let mut data = Vec::with_capacity(hdr.numblocks() * 512);
    let mut tags = Vec::with_capacity(hdr.numblocks() * 12);
    let mut unc = vec![0u8; CHUNK_TOTAL_LEN];
    let mut off = hdr.header_len();

    for i in 0..hdr.num_chunks() {
        let bl = hdr.blengths[i];
        if bl == 0 {
            break;
        }
        let stored = hdr.chunk_stored_len(i);
        let end = off + stored;
        if end > buf.len() {
            return Err(format!("truncated at chunk {i}"));
        }
        let chunk = &buf[off..end];

        if bl == BLEN_UNCOMPRESSED || hdr.src_cmp == SRC_CMP_NONE {
            unc.copy_from_slice(chunk);
        } else if hdr.src_cmp == SRC_CMP_FAST {
            unpack_rle(chunk, &mut unc).map_err(|e| format!("chunk {i}: {e}"))?;
        } else {
            debug_assert_eq!(hdr.src_cmp, SRC_CMP_BEST);
            let d = lzhuf_decompress(chunk, CHUNK_TOTAL_LEN, 0x00)
                .map_err(|e| format!("chunk {i}: {e}"))?;
            if d.len() != CHUNK_TOTAL_LEN {
                return Err(format!("chunk {i}: LZH produced {} bytes", d.len()));
            }
            unc.copy_from_slice(&d);
        }
        off = end;

        data.extend_from_slice(&unc[0..CHUNK_DATA_LEN]);
        tags.extend_from_slice(&unc[CHUNK_DATA_LEN..CHUNK_TOTAL_LEN]);
    }

    if data.len() != hdr.numblocks() * 512 || tags.len() != hdr.numblocks() * 12 {
        return Err(format!(
            "incomplete: data {} tags {} (expected {} / {})",
            data.len(),
            tags.len(),
            hdr.numblocks() * 512,
            hdr.numblocks() * 12
        ));
    }

    Ok(DartImage {
        data,
        tags,
        src_type: hdr.src_type,
        src_size: hdr.src_size,
    })
}

/// Decode a DART image from a reader (reads the whole file into memory; DART
/// floppies are at most ~1.5 MB).
pub fn decode_dart<R: Read + Seek>(mut reader: R) -> Result<DartImage, String> {
    reader.seek(SeekFrom::Start(0)).map_err(|e| e.to_string())?;
    let mut buf = Vec::new();
    reader.read_to_end(&mut buf).map_err(|e| e.to_string())?;
    decode_dart_bytes(&buf)
}

/// Decode a DART file into a flat image stream for the browse/inspect pipeline.
/// A disk that carries sector tags (a Lisa disk) is re-wrapped as a DiskCopy 4.2
/// image **with** its tags so the tag-dependent Lisa filesystem can read them;
/// every other disk (Mac / Apple II / MS-DOS, whose tags are zero-filled) yields
/// just the block data, a plain image the normal filesystem detection handles.
pub fn decode_dart_to_image(buf: &[u8]) -> Result<Vec<u8>, String> {
    let img = decode_dart_bytes(buf)?;
    if img.has_nonzero_tags() {
        Ok(super::dc42::encode_dc42_with_tags(
            "Lisa", &img.data, &img.tags,
        ))
    } else {
        Ok(img.data)
    }
}

/// True if `path` has a `.dart` extension.
pub fn is_dart_path(path: &std::path::Path) -> bool {
    path.extension()
        .and_then(|e| e.to_str())
        .is_some_and(|s| s.eq_ignore_ascii_case("dart"))
}

/// Content-detect a DART file: the header plus every chunk's stored length must
/// exactly account for `file_size` (DART has no magic number).
pub fn is_dart(head: &[u8], file_size: u64) -> bool {
    detect_dart(head, file_size).is_some()
}

/// DART "fast" RLE: the input is a series of big-endian 16-bit words. The first
/// word of each run is a signed count; positive N copies the next N words
/// verbatim, negative N repeats the following 1-word pattern -N times. Expands
/// into exactly `out.len()` bytes.
fn unpack_rle(input: &[u8], out: &mut [u8]) -> Result<(), String> {
    let mut i = 0usize;
    let mut o = 0usize;
    while i + 2 <= input.len() {
        let count = i16::from_be_bytes([input[i], input[i + 1]]) as i32;
        i += 2;
        if count > 0 {
            let n = count as usize * 2;
            if i + n > input.len() || o + n > out.len() {
                return Err("RLE copy overrun".into());
            }
            out[o..o + n].copy_from_slice(&input[i..i + n]);
            i += n;
            o += n;
        } else if count < 0 {
            if i + 2 > input.len() {
                return Err("RLE pattern truncated".into());
            }
            let (hi, lo) = (input[i], input[i + 1]);
            i += 2;
            let reps = (-count) as usize;
            if o + reps * 2 > out.len() {
                return Err("RLE run overrun".into());
            }
            for _ in 0..reps {
                out[o] = hi;
                out[o + 1] = lo;
                o += 2;
            }
        } else {
            return Err("RLE zero count".into());
        }
    }
    if o != out.len() {
        return Err(format!("RLE expanded {o} of {} bytes", out.len()));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rle_notes_example() {
        // From DART-notes.md: `fe00 0000 0003 4244 df19 1e19` -> 1024 zero bytes
        // then 42 44 df 19 1e 19.
        let input = [
            0xfe, 0x00, 0x00, 0x00, 0x00, 0x03, 0x42, 0x44, 0xdf, 0x19, 0x1e, 0x19,
        ];
        let mut out = vec![0xAAu8; 1024 + 6];
        unpack_rle(&input, &mut out).expect("rle");
        assert!(out[..1024].iter().all(|&b| b == 0));
        assert_eq!(&out[1024..], &[0x42, 0x44, 0xdf, 0x19, 0x1e, 0x19]);
    }

    /// Build a minimal all-zero 400K Lisa DART using fast/RLE: each 20960-byte
    /// chunk is 10480 zero words, encoded as one negative run [D7 10 00 00].
    fn zero_lisa_dart_400k() -> Vec<u8> {
        let chunks = 400 * 2 / CHUNK_BLOCKS; // 20
        let neg = (-(10480i32)) as i16; // -10480 words = 20960 zero bytes
        let mut hdr = vec![SRC_CMP_FAST, 2u8]; // fast, Lisa
        hdr.extend_from_slice(&400u16.to_be_bytes());
        // 40-entry array; first `chunks` are 2 words each, rest zero.
        for k in 0..40 {
            let bl: u16 = if k < chunks { 2 } else { 0 };
            hdr.extend_from_slice(&bl.to_be_bytes());
        }
        let mut out = hdr;
        let chunk = {
            let mut c = Vec::new();
            c.extend_from_slice(&neg.to_be_bytes());
            c.extend_from_slice(&[0u8, 0u8]); // pattern 0x0000
            c
        };
        for _ in 0..chunks {
            out.extend_from_slice(&chunk);
        }
        out
    }

    #[test]
    fn detect_and_decode_fast_lisa() {
        let img = zero_lisa_dart_400k();
        let hdr = detect_dart(&img[..84], img.len() as u64).expect("detect");
        assert_eq!(hdr.src_type, 2);
        assert_eq!(hdr.src_size, 400);
        assert_eq!(hdr.num_chunks(), 20);

        let decoded = decode_dart_bytes(&img).expect("decode");
        assert!(decoded.is_lisa());
        assert_eq!(decoded.data.len(), 409_600);
        assert_eq!(decoded.tags.len(), 9_600);
        assert!(decoded.data.iter().all(|&b| b == 0));
        assert!(!decoded.has_nonzero_tags());
    }

    #[test]
    fn detect_rejects_wrong_size() {
        let img = zero_lisa_dart_400k();
        assert!(detect_dart(&img[..84], img.len() as u64 + 1).is_none());
    }

    #[test]
    fn rejects_non_dart_header() {
        // srcType 99 is not a valid disk kind.
        assert!(parse_dart_header(&[0, 99, 0x01, 0x90]).is_none());
    }
}
