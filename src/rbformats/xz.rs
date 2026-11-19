//! `.xz` disk images (`.img.xz`, the Raspberry Pi / usbode release shape).
//!
//! The decoded size comes from the stream index at the end of the file, so measuring an image
//! never decompresses it. [`decode_into`] streams the decoded disk into any `Write`: the
//! physical writer pours it straight onto a card, `source_reader` into a temp file when a
//! caller needs random access.
//!
//! Two codecs. `native-xz` (default) is liblzma, which streams with memory bounded by the
//! dictionary. The pure-Rust fallback (lzma-rs, for the vintage builds) holds one whole xz
//! block in RAM, so it refuses an image whose largest block exceeds [`PURE_RUST_BLOCK_LIMIT`];
//! `xz -T0` writes blocks of a few dictionaries, a single-threaded `xz` one block per file.

use std::fs::File;
use std::io::{self, BufReader, Read, Seek, SeekFrom, Write};
use std::path::Path;

/// The six bytes every xz stream starts with.
pub const XZ_MAGIC: [u8; 6] = *b"\xFD7zXZ\x00";
const FOOTER_MAGIC: [u8; 2] = *b"YZ";
const HEADER_LEN: u64 = 12;
const FOOTER_LEN: u64 = 12;
/// The largest index we read; a record is at most 18 bytes, so this is ~3.7 M blocks.
const MAX_INDEX: u64 = 64 * 1024 * 1024;
/// The biggest xz block the pure-Rust codec decodes (it buffers a block whole).
pub const PURE_RUST_BLOCK_LIMIT: u64 = 256 * 1024 * 1024;

/// What an xz file's indexes say about it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct XzSizes {
    /// Decoded length of every stream together.
    pub uncompressed: u64,
    /// Decoded length of the largest block.
    pub largest_block: u64,
    pub streams: u32,
}

/// `.xz` extension and the xz magic, so a mislabelled file is not taken for one.
pub fn is_xz_path(path: &Path) -> bool {
    let ext_ok = path
        .extension()
        .and_then(|e| e.to_str())
        .is_some_and(|e| e.eq_ignore_ascii_case("xz"));
    ext_ok
        && File::open(path)
            .map(|mut f| has_xz_magic(&mut f))
            .unwrap_or(false)
}

fn has_xz_magic<R: Read>(r: &mut R) -> bool {
    let mut magic = [0u8; 6];
    r.read_exact(&mut magic).is_ok() && magic == XZ_MAGIC
}

fn bad(msg: impl Into<String>) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, msg.into())
}

fn read_at<R: Read + Seek>(r: &mut R, at: u64, buf: &mut [u8]) -> io::Result<()> {
    r.seek(SeekFrom::Start(at))?;
    r.read_exact(buf)
}

/// One xz multibyte integer (7 bits a byte, little end first, at most 9 bytes).
fn varint(buf: &[u8], at: &mut usize) -> io::Result<u64> {
    let mut v: u64 = 0;
    for i in 0..9 {
        let b = *buf
            .get(*at)
            .ok_or_else(|| bad("xz index ends inside a number"))?;
        *at += 1;
        v |= u64::from(b & 0x7F) << (7 * i);
        if b & 0x80 == 0 {
            return Ok(v);
        }
    }
    Err(bad("xz index number is longer than 9 bytes"))
}

/// Walk the streams back from the end of the file, summing their indexes.
pub fn sizes<R: Read + Seek>(r: &mut R) -> io::Result<XzSizes> {
    streams(r).map(|(s, _)| s)
}

/// [`sizes`] and each stream's byte range in the file, first stream first.
fn streams<R: Read + Seek>(r: &mut R) -> io::Result<(XzSizes, Vec<(u64, u64)>)> {
    let mut ranges = Vec::new();
    let mut end = r.seek(SeekFrom::End(0))?;
    let mut out = XzSizes {
        uncompressed: 0,
        largest_block: 0,
        streams: 0,
    };
    loop {
        // Stream padding: zero bytes in groups of four.
        loop {
            if end < HEADER_LEN + FOOTER_LEN {
                return Err(bad("too short to be an xz stream"));
            }
            let mut word = [0u8; 4];
            read_at(r, end - 4, &mut word)?;
            if word != [0; 4] {
                break;
            }
            end -= 4;
        }
        let mut footer = [0u8; FOOTER_LEN as usize];
        read_at(r, end - FOOTER_LEN, &mut footer)?;
        if footer[10..12] != FOOTER_MAGIC {
            return Err(bad("no xz stream footer"));
        }
        let backward = u32::from_le_bytes([footer[4], footer[5], footer[6], footer[7]]);
        let index_len = (u64::from(backward) + 1) * 4;
        if index_len > MAX_INDEX || index_len + FOOTER_LEN + HEADER_LEN > end {
            return Err(bad("xz index size is out of range"));
        }
        let index_at = end - FOOTER_LEN - index_len;
        let mut index = vec![0u8; index_len as usize];
        read_at(r, index_at, &mut index)?;
        if index[0] != 0 {
            return Err(bad("xz index indicator is not zero"));
        }
        let mut at = 1;
        let records = varint(&index, &mut at)?;
        let mut blocks: u64 = 0;
        for _ in 0..records {
            let unpadded = varint(&index, &mut at)?;
            let size = varint(&index, &mut at)?;
            blocks = blocks
                .checked_add(unpadded.div_ceil(4) * 4)
                .ok_or_else(|| bad("xz block sizes overflow"))?;
            out.uncompressed = out
                .uncompressed
                .checked_add(size)
                .ok_or_else(|| bad("xz sizes overflow"))?;
            out.largest_block = out.largest_block.max(size);
        }
        let start = index_at
            .checked_sub(blocks)
            .and_then(|s| s.checked_sub(HEADER_LEN))
            .ok_or_else(|| bad("xz blocks run past the start of the file"))?;
        let mut header = [0u8; 6];
        read_at(r, start, &mut header)?;
        if header != XZ_MAGIC {
            return Err(bad("xz index does not lead back to a stream header"));
        }
        out.streams += 1;
        ranges.push((start, end));
        if start == 0 {
            ranges.reverse();
            return Ok((out, ranges));
        }
        end = start;
    }
}

/// [`sizes`] of the file at `path`.
pub fn file_sizes(path: &Path) -> io::Result<XzSizes> {
    sizes(&mut File::open(path)?)
}

/// Decode every stream of `path` into `out`, in order.
pub fn decode_into<W: Write>(path: &Path, out: &mut W) -> io::Result<()> {
    let input = BufReader::with_capacity(1 << 20, File::open(path)?);
    decode_stream(path, input, out)
}

#[cfg(feature = "native-xz")]
fn decode_stream<W: Write>(_path: &Path, input: BufReader<File>, out: &mut W) -> io::Result<()> {
    let mut dec = liblzma::read::XzDecoder::new_multi_decoder(input);
    io::copy(&mut dec, out).map(|_| ())
}

#[cfg(not(feature = "native-xz"))]
fn decode_stream<W: Write>(_path: &Path, input: BufReader<File>, out: &mut W) -> io::Result<()> {
    let mut file = input.into_inner();
    let (s, ranges) = streams(&mut file)?;
    if s.largest_block > PURE_RUST_BLOCK_LIMIT {
        return Err(bad(format!(
            "this build decodes xz blocks of up to {} MiB, and this image has one of {} MiB; \
             recompress it with `xz -T0` or use a build with native xz",
            PURE_RUST_BLOCK_LIMIT >> 20,
            s.largest_block >> 20
        )));
    }
    // lzma-rs refuses bytes after a stream, so each one is decoded from its own range.
    for (start, end) in ranges {
        file.seek(SeekFrom::Start(start))?;
        let mut one = BufReader::with_capacity(1 << 20, (&mut file).take(end - start));
        lzma_rs::xz_decompress(&mut one, out).map_err(|e| match e {
            lzma_rs::error::Error::IoError(e) => e,
            other => bad(format!("xz decode failed: {other:?}")),
        })?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn compress(data: &[u8]) -> Vec<u8> {
        let mut out = Vec::new();
        lzma_rs::xz_compress(&mut io::Cursor::new(data), &mut out).unwrap();
        out
    }

    fn pattern(len: usize, seed: u8) -> Vec<u8> {
        (0..len)
            .map(|i| (i as u8).wrapping_mul(31) ^ seed)
            .collect()
    }

    #[test]
    fn the_index_gives_the_decoded_size_of_one_stream() {
        let data = pattern(3_000_000, 7);
        let s = sizes(&mut io::Cursor::new(compress(&data))).unwrap();
        assert_eq!(s.uncompressed, data.len() as u64);
        assert_eq!(s.streams, 1);
    }

    #[test]
    fn concatenated_padded_streams_sum_and_decode_in_order() {
        let (a, b) = (pattern(70_000, 1), pattern(123_457, 2));
        let mut file = compress(&a);
        file.extend_from_slice(&[0u8; 8]);
        file.extend(compress(&b));
        file.extend_from_slice(&[0u8; 4]);
        let s = sizes(&mut io::Cursor::new(&file)).unwrap();
        assert_eq!(s.uncompressed, (a.len() + b.len()) as u64);
        assert_eq!(s.streams, 2);

        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("disk.img.xz");
        std::fs::write(&path, &file).unwrap();
        assert!(is_xz_path(&path));
        let mut out = Vec::new();
        decode_into(&path, &mut out).unwrap();
        assert_eq!(out.len(), a.len() + b.len());
        assert_eq!(&out[..a.len()], a.as_slice());
        assert_eq!(&out[a.len()..], b.as_slice());
    }

    #[test]
    fn not_xz_is_refused() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("fake.img.xz");
        std::fs::write(&path, vec![0x55u8; 4096]).unwrap();
        assert!(!is_xz_path(&path));
        assert!(file_sizes(&path).is_err());
    }
}
