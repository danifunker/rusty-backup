//! DMG (UDIF) disk image format parser and virtual reader.
//!
//! Supports Apple UDIF compressed DMG images with zlib, bzip2, ADC, raw, and
//! zero block types.  Provides a `DmgReader` that implements `Read + Seek` over
//! the decompressed virtual disk.

use std::io::{self, BufReader, Cursor, Read, Seek, SeekFrom};

use anyhow::{bail, Context, Result};

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

/// Koly signature at the start of the 512-byte footer.
const KOLY_SIGNATURE: u32 = 0x6B6F_6C79; // "koly"
const KOLY_VERSION: u32 = 4;
const KOLY_HEADER_SIZE: u32 = 0x200;

/// Mish signature at the start of each blkx binary blob.
const MISH_SIGNATURE: u32 = 0x6D69_7368; // "mish"

// Block type constants (big-endian u32).
const METHOD_ZERO_0: u32 = 0x0000_0000;
const METHOD_COPY: u32 = 0x0000_0001;
const METHOD_ZERO_2: u32 = 0x0000_0002;
const METHOD_ADC: u32 = 0x8000_0004;
const METHOD_ZLIB: u32 = 0x8000_0005;
const METHOD_BZIP2: u32 = 0x8000_0006;
const METHOD_COMMENT: u32 = 0x7FFF_FFFE;
const METHOD_END: u32 = 0xFFFF_FFFF;

const SECTOR_SIZE: u64 = 512;

// ---------------------------------------------------------------------------
// Koly header
// ---------------------------------------------------------------------------

/// Parsed koly footer from a DMG file.
#[derive(Debug)]
struct KolyHeader {
    xml_offset: u64,
    xml_length: u64,
    data_fork_offset: u64,
    #[allow(dead_code)]
    data_fork_length: u64,
}

fn parse_koly(buf: &[u8; 512]) -> Option<KolyHeader> {
    let sig = u32::from_be_bytes([buf[0], buf[1], buf[2], buf[3]]);
    let ver = u32::from_be_bytes([buf[4], buf[5], buf[6], buf[7]]);
    let hsz = u32::from_be_bytes([buf[8], buf[9], buf[10], buf[11]]);
    if sig != KOLY_SIGNATURE || ver != KOLY_VERSION || hsz != KOLY_HEADER_SIZE {
        return None;
    }

    let data_fork_offset = u64::from_be_bytes(buf[0x18..0x20].try_into().ok()?);
    let data_fork_length = u64::from_be_bytes(buf[0x20..0x28].try_into().ok()?);
    let xml_offset = u64::from_be_bytes(buf[0xD8..0xE0].try_into().ok()?);
    let xml_length = u64::from_be_bytes(buf[0xE0..0xE8].try_into().ok()?);

    Some(KolyHeader {
        xml_offset,
        xml_length,
        data_fork_offset,
        data_fork_length,
    })
}

// ---------------------------------------------------------------------------
// Mish / block parsing
// ---------------------------------------------------------------------------

/// A single compressed/uncompressed block within a mish partition.
#[derive(Debug, Clone)]
struct DmgBlock {
    block_type: u32,
    /// Byte offset in the uncompressed virtual disk (absolute).
    unpack_pos: u64,
    /// Byte size of this block when uncompressed.
    unpack_size: u64,
    /// Byte offset in the DMG file of the compressed data (absolute).
    pack_pos: u64,
    /// Byte size of the compressed data in the DMG file.
    pack_size: u64,
}

/// Parse a mish blob (base64-decoded from the XML plist).
///
/// Returns the list of blocks with absolute virtual and file positions.
fn parse_mish(data: &[u8], data_fork_offset: u64) -> Result<Vec<DmgBlock>> {
    if data.len() < 0xCC {
        bail!("mish block too short ({} bytes)", data.len());
    }

    let sig = u32::from_be_bytes([data[0], data[1], data[2], data[3]]);
    if sig != MISH_SIGNATURE {
        bail!("bad mish signature: 0x{:08X}", sig);
    }

    let start_sector = u64::from_be_bytes(data[0x08..0x10].try_into()?);
    let start_pack_pos = u64::from_be_bytes(data[0x18..0x20].try_into()?);
    let num_blocks = u32::from_be_bytes(data[0xC8..0xCC].try_into()?);

    let entries_start = 0xCC;
    let expected_len = entries_start + (num_blocks as usize) * 40;
    if data.len() < expected_len {
        bail!(
            "mish data too short for {} blocks (need {} bytes, have {})",
            num_blocks,
            expected_len,
            data.len()
        );
    }

    let mut blocks = Vec::with_capacity(num_blocks as usize);
    for i in 0..num_blocks as usize {
        let off = entries_start + i * 40;
        let block_type = u32::from_be_bytes(data[off..off + 4].try_into()?);

        // Skip comment and end markers
        if block_type == METHOD_COMMENT || block_type == METHOD_END {
            continue;
        }

        let unpack_sector = u64::from_be_bytes(data[off + 8..off + 16].try_into()?);
        let unpack_sectors = u64::from_be_bytes(data[off + 16..off + 24].try_into()?);
        let pack_offset = u64::from_be_bytes(data[off + 24..off + 32].try_into()?);
        let pack_size = u64::from_be_bytes(data[off + 32..off + 40].try_into()?);

        blocks.push(DmgBlock {
            block_type,
            unpack_pos: (start_sector + unpack_sector) * SECTOR_SIZE,
            unpack_size: unpack_sectors * SECTOR_SIZE,
            pack_pos: data_fork_offset + start_pack_pos + pack_offset,
            pack_size,
        });
    }

    Ok(blocks)
}

// ---------------------------------------------------------------------------
// XML plist parsing (minimal, no crate needed)
// ---------------------------------------------------------------------------

/// Extract base64-encoded blkx data entries from the DMG XML plist.
fn extract_blkx_data(xml: &str) -> Vec<Vec<u8>> {
    use base64::Engine;
    let engine = base64::engine::general_purpose::STANDARD;

    let mut results = Vec::new();

    // Find <key>blkx</key> followed by <array>...</array>
    // We do simple string searching — the plist XML is well-formed.
    let blkx_key = "<key>blkx</key>";
    let blkx_pos = match xml.find(blkx_key) {
        Some(p) => p + blkx_key.len(),
        None => return results,
    };

    let rest = &xml[blkx_pos..];

    // Find the <array> that follows
    let array_start = match rest.find("<array>") {
        Some(p) => p + "<array>".len(),
        None => return results,
    };
    let array_end = match rest.find("</array>") {
        Some(p) => p,
        None => return results,
    };

    if array_start >= array_end {
        return results;
    }

    let array_content = &rest[array_start..array_end];

    // Within the array, find each <data>...</data> block
    let mut search_pos = 0;
    while search_pos < array_content.len() {
        let data_start = match array_content[search_pos..].find("<data>") {
            Some(p) => search_pos + p + "<data>".len(),
            None => break,
        };
        let data_end = match array_content[data_start..].find("</data>") {
            Some(p) => data_start + p,
            None => break,
        };

        let b64_text: String = array_content[data_start..data_end]
            .chars()
            .filter(|c| !c.is_whitespace())
            .collect();

        if let Ok(decoded) = engine.decode(&b64_text) {
            results.push(decoded);
        }

        search_pos = data_end + "</data>".len();
    }

    results
}

// ---------------------------------------------------------------------------
// ADC decompression
// ---------------------------------------------------------------------------

/// Decompress ADC (Apple Data Compression) — a simple LZ77 variant.
fn decompress_adc(input: &[u8], output_size: usize) -> Result<Vec<u8>> {
    let mut out = Vec::with_capacity(output_size);
    let mut pos = 0;

    while pos < input.len() && out.len() < output_size {
        let b = input[pos];
        pos += 1;

        if b & 0x80 != 0 {
            // Literal run: length = (b - 0x80) + 1
            let len = (b as usize - 0x80) + 1;
            if pos + len > input.len() {
                bail!("ADC: literal run overflows input");
            }
            let end = (out.len() + len).min(output_size);
            let copy_len = end - out.len();
            out.extend_from_slice(&input[pos..pos + copy_len]);
            pos += len;
        } else if b & 0x40 != 0 {
            // Long match: length = (b - 0x40) + 4, distance = next 2 bytes (BE)
            let len = (b as usize - 0x40) + 4;
            if pos + 2 > input.len() {
                bail!("ADC: long match overflows input");
            }
            let distance = ((input[pos] as usize) << 8) | (input[pos + 1] as usize);
            pos += 2;
            copy_match(&mut out, distance, len, output_size);
        } else {
            // Short match: length = (b >> 2) + 3, distance = ((b & 3) << 8) + next byte
            let len = ((b >> 2) as usize) + 3;
            if pos + 1 > input.len() {
                bail!("ADC: short match overflows input");
            }
            let distance = (((b & 3) as usize) << 8) | (input[pos] as usize);
            pos += 1;
            copy_match(&mut out, distance, len, output_size);
        }
    }

    Ok(out)
}

/// Copy `len` bytes from `distance` bytes back in the output buffer.
fn copy_match(out: &mut Vec<u8>, distance: usize, len: usize, max_size: usize) {
    if distance == 0 || distance > out.len() {
        // Invalid distance — fill with zeros rather than crashing
        let fill = len.min(max_size - out.len());
        out.extend(std::iter::repeat_n(0u8, fill));
        return;
    }
    let start = out.len() - distance;
    for i in 0..len {
        if out.len() >= max_size {
            break;
        }
        let byte = out[start + (i % distance)];
        out.push(byte);
    }
}

// ---------------------------------------------------------------------------
// DmgReader — Read + Seek over a decompressed DMG
// ---------------------------------------------------------------------------

/// A virtual reader over a decompressed UDIF DMG image.
///
/// Implements `Read` and `Seek` so it can be used anywhere a raw disk reader
/// is expected.  Blocks are decompressed on demand with a single-block cache.
pub struct DmgReader {
    /// The underlying DMG file.
    source: BufReader<std::fs::File>,
    /// Sorted list of all blocks across all partitions.
    blocks: Vec<DmgBlock>,
    /// Total virtual (uncompressed) size.
    total_size: u64,
    /// Current virtual read position.
    position: u64,
    /// Cached decompressed block data.
    cache_block_idx: Option<usize>,
    cache_data: Vec<u8>,
}

impl DmgReader {
    /// Total uncompressed size of the virtual disk.
    pub fn total_size(&self) -> u64 {
        self.total_size
    }

    /// Find the block index containing the given virtual position.
    fn find_block(&self, pos: u64) -> Option<usize> {
        // Binary search for the block containing `pos`
        self.blocks
            .binary_search_by(|b| {
                if pos < b.unpack_pos {
                    std::cmp::Ordering::Greater
                } else if pos >= b.unpack_pos + b.unpack_size {
                    std::cmp::Ordering::Less
                } else {
                    std::cmp::Ordering::Equal
                }
            })
            .ok()
    }

    /// Ensure the block at `idx` is decompressed and cached.
    fn ensure_cached(&mut self, idx: usize) -> io::Result<()> {
        if self.cache_block_idx == Some(idx) {
            return Ok(());
        }

        let block = &self.blocks[idx];
        let data = match block.block_type {
            METHOD_ZERO_0 | METHOD_ZERO_2 => {
                vec![0u8; block.unpack_size as usize]
            }
            METHOD_COPY => {
                self.source
                    .seek(SeekFrom::Start(block.pack_pos))
                    .map_err(io::Error::other)?;
                let mut buf = vec![0u8; block.pack_size as usize];
                self.source.read_exact(&mut buf).map_err(io::Error::other)?;
                buf
            }
            METHOD_ZLIB => {
                self.source
                    .seek(SeekFrom::Start(block.pack_pos))
                    .map_err(io::Error::other)?;
                let mut compressed = vec![0u8; block.pack_size as usize];
                self.source
                    .read_exact(&mut compressed)
                    .map_err(io::Error::other)?;
                let mut decoder = flate2::read::ZlibDecoder::new(Cursor::new(compressed));
                let mut out = Vec::with_capacity(block.unpack_size as usize);
                decoder.read_to_end(&mut out).map_err(io::Error::other)?;
                out
            }
            METHOD_BZIP2 => {
                self.source
                    .seek(SeekFrom::Start(block.pack_pos))
                    .map_err(io::Error::other)?;
                let mut compressed = vec![0u8; block.pack_size as usize];
                self.source
                    .read_exact(&mut compressed)
                    .map_err(io::Error::other)?;
                let mut decoder = bzip2::read::BzDecoder::new(Cursor::new(compressed));
                let mut out = Vec::with_capacity(block.unpack_size as usize);
                decoder.read_to_end(&mut out).map_err(io::Error::other)?;
                out
            }
            METHOD_ADC => {
                self.source
                    .seek(SeekFrom::Start(block.pack_pos))
                    .map_err(io::Error::other)?;
                let mut compressed = vec![0u8; block.pack_size as usize];
                self.source
                    .read_exact(&mut compressed)
                    .map_err(io::Error::other)?;
                decompress_adc(&compressed, block.unpack_size as usize).map_err(io::Error::other)?
            }
            other => {
                return Err(io::Error::new(
                    io::ErrorKind::Unsupported,
                    format!("unsupported DMG block type: 0x{:08X}", other),
                ));
            }
        };

        self.cache_block_idx = Some(idx);
        self.cache_data = data;
        Ok(())
    }
}

impl Read for DmgReader {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        if self.position >= self.total_size || buf.is_empty() {
            return Ok(0);
        }

        let idx = match self.find_block(self.position) {
            Some(i) => i,
            None => {
                // Position falls in a gap between blocks — return zeros
                // Find the next block to know how many zero bytes to emit
                let next_block_start = self
                    .blocks
                    .iter()
                    .find(|b| b.unpack_pos > self.position)
                    .map(|b| b.unpack_pos)
                    .unwrap_or(self.total_size);
                let gap = (next_block_start - self.position) as usize;
                let n = buf.len().min(gap);
                buf[..n].fill(0);
                self.position += n as u64;
                return Ok(n);
            }
        };

        self.ensure_cached(idx)?;

        let block = &self.blocks[idx];
        let offset_in_block = (self.position - block.unpack_pos) as usize;
        let available = self.cache_data.len().saturating_sub(offset_in_block);
        let n = buf.len().min(available);
        if n == 0 {
            return Ok(0);
        }

        buf[..n].copy_from_slice(&self.cache_data[offset_in_block..offset_in_block + n]);
        self.position += n as u64;
        Ok(n)
    }
}

impl Seek for DmgReader {
    fn seek(&mut self, pos: SeekFrom) -> io::Result<u64> {
        let new_pos = match pos {
            SeekFrom::Start(p) => p as i64,
            SeekFrom::Current(delta) => self.position as i64 + delta,
            SeekFrom::End(delta) => self.total_size as i64 + delta,
        };
        if new_pos < 0 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "seek to negative position",
            ));
        }
        self.position = new_pos as u64;
        Ok(self.position)
    }
}

// ---------------------------------------------------------------------------
// Public API
// ---------------------------------------------------------------------------

/// Try to detect and open a DMG file as a `DmgReader`.
///
/// Returns `Some(DmgReader)` if the file has a valid koly footer with parseable
/// blkx entries.  Returns `None` if the file doesn't appear to be a UDIF DMG.
pub fn detect_dmg(file: std::fs::File) -> Result<Option<DmgReader>> {
    let file_size = file.metadata()?.len();
    if file_size < 512 {
        return Ok(None);
    }

    let mut reader = BufReader::new(file);

    // Read last 512 bytes for koly footer
    reader.seek(SeekFrom::End(-512))?;
    let mut footer = [0u8; 512];
    reader.read_exact(&mut footer)?;

    let koly = match parse_koly(&footer) {
        Some(k) => k,
        None => return Ok(None),
    };

    if koly.xml_length == 0 {
        // No XML plist — not a standard UDIF
        return Ok(None);
    }

    // Read XML plist
    reader.seek(SeekFrom::Start(koly.xml_offset))?;
    let mut xml_buf = vec![0u8; koly.xml_length as usize];
    reader
        .read_exact(&mut xml_buf)
        .context("failed to read DMG XML plist")?;
    let xml = String::from_utf8_lossy(&xml_buf);

    // Parse blkx entries
    let blkx_data = extract_blkx_data(&xml);
    if blkx_data.is_empty() {
        return Ok(None);
    }

    let mut all_blocks = Vec::new();
    for mish_data in &blkx_data {
        match parse_mish(mish_data, koly.data_fork_offset) {
            Ok(blocks) => all_blocks.extend(blocks),
            Err(e) => {
                log::warn!("Skipping bad mish block: {e}");
            }
        }
    }

    if all_blocks.is_empty() {
        return Ok(None);
    }

    // Sort by unpack position for binary search
    all_blocks.sort_by_key(|b| b.unpack_pos);

    // Compute total virtual size from the last block
    let total_size = all_blocks
        .iter()
        .map(|b| b.unpack_pos + b.unpack_size)
        .max()
        .unwrap_or(0);

    Ok(Some(DmgReader {
        source: reader,
        blocks: all_blocks,
        total_size,
        position: 0,
        cache_block_idx: None,
        cache_data: Vec::new(),
    }))
}

/// Human-readable description of the compression methods used in a DmgReader.
pub fn describe_dmg_methods(reader: &DmgReader) -> String {
    let mut methods = std::collections::BTreeSet::new();
    for b in &reader.blocks {
        match b.block_type {
            METHOD_ZERO_0 | METHOD_ZERO_2 => {}
            METHOD_COPY => {
                methods.insert("raw");
            }
            METHOD_ZLIB => {
                methods.insert("zlib");
            }
            METHOD_BZIP2 => {
                methods.insert("bzip2");
            }
            METHOD_ADC => {
                methods.insert("ADC");
            }
            _ => {
                methods.insert("other");
            }
        }
    }
    if methods.is_empty() {
        "zeros only".to_string()
    } else {
        methods.into_iter().collect::<Vec<_>>().join("+")
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;

    #[test]
    fn test_parse_koly_valid() {
        let mut buf = [0u8; 512];
        // Signature "koly"
        buf[0..4].copy_from_slice(&KOLY_SIGNATURE.to_be_bytes());
        // Version = 4
        buf[4..8].copy_from_slice(&KOLY_VERSION.to_be_bytes());
        // Header size = 0x200
        buf[8..12].copy_from_slice(&KOLY_HEADER_SIZE.to_be_bytes());
        // Data fork offset at 0x18 = 0
        buf[0x18..0x20].copy_from_slice(&0u64.to_be_bytes());
        // Data fork length at 0x20 = 1000
        buf[0x20..0x28].copy_from_slice(&1000u64.to_be_bytes());
        // XML offset at 0xD8 = 2000
        buf[0xD8..0xE0].copy_from_slice(&2000u64.to_be_bytes());
        // XML length at 0xE0 = 500
        buf[0xE0..0xE8].copy_from_slice(&500u64.to_be_bytes());

        let koly = parse_koly(&buf).unwrap();
        assert_eq!(koly.xml_offset, 2000);
        assert_eq!(koly.xml_length, 500);
        assert_eq!(koly.data_fork_offset, 0);
        assert_eq!(koly.data_fork_length, 1000);
    }

    #[test]
    fn test_parse_koly_bad_signature() {
        let mut buf = [0u8; 512];
        buf[0..4].copy_from_slice(b"nope");
        assert!(parse_koly(&buf).is_none());
    }

    #[test]
    fn test_parse_koly_bad_version() {
        let mut buf = [0u8; 512];
        buf[0..4].copy_from_slice(&KOLY_SIGNATURE.to_be_bytes());
        buf[4..8].copy_from_slice(&3u32.to_be_bytes()); // wrong version
        buf[8..12].copy_from_slice(&KOLY_HEADER_SIZE.to_be_bytes());
        assert!(parse_koly(&buf).is_none());
    }

    #[test]
    fn test_extract_blkx_data_empty() {
        let xml = r#"<plist><dict></dict></plist>"#;
        assert!(extract_blkx_data(xml).is_empty());
    }

    #[test]
    fn test_extract_blkx_data_with_entries() {
        use base64::Engine;
        let engine = base64::engine::general_purpose::STANDARD;
        let payload = vec![1u8, 2, 3, 4, 5];
        let b64 = engine.encode(&payload);
        let xml = format!(
            r#"<plist><dict>
            <key>resource-fork</key><dict>
            <key>blkx</key><array>
            <dict><key>Data</key><data>{}</data></dict>
            </array></dict></dict></plist>"#,
            b64
        );
        let results = extract_blkx_data(&xml);
        assert_eq!(results.len(), 1);
        assert_eq!(results[0], payload);
    }

    #[test]
    fn test_parse_mish_valid() {
        // Build a minimal mish block with 1 raw-copy entry
        let mut data = vec![0u8; 0xCC + 40];
        // Signature
        data[0..4].copy_from_slice(&MISH_SIGNATURE.to_be_bytes());
        // Version = 1
        data[4..8].copy_from_slice(&1u32.to_be_bytes());
        // Start sector = 0
        data[0x08..0x10].copy_from_slice(&0u64.to_be_bytes());
        // Num sectors = 8
        data[0x10..0x18].copy_from_slice(&8u64.to_be_bytes());
        // Start pack pos = 0
        data[0x18..0x20].copy_from_slice(&0u64.to_be_bytes());
        // Num blocks = 1
        data[0xC8..0xCC].copy_from_slice(&1u32.to_be_bytes());

        // Block entry at 0xCC
        let off = 0xCC;
        // Type = METHOD_COPY
        data[off..off + 4].copy_from_slice(&METHOD_COPY.to_be_bytes());
        // Comment = 0
        data[off + 4..off + 8].copy_from_slice(&0u32.to_be_bytes());
        // Unpack sector = 0
        data[off + 8..off + 16].copy_from_slice(&0u64.to_be_bytes());
        // Unpack sector count = 8
        data[off + 16..off + 24].copy_from_slice(&8u64.to_be_bytes());
        // Pack offset = 0
        data[off + 24..off + 32].copy_from_slice(&0u64.to_be_bytes());
        // Pack size = 4096
        data[off + 32..off + 40].copy_from_slice(&4096u64.to_be_bytes());

        let blocks = parse_mish(&data, 0).unwrap();
        assert_eq!(blocks.len(), 1);
        assert_eq!(blocks[0].block_type, METHOD_COPY);
        assert_eq!(blocks[0].unpack_pos, 0);
        assert_eq!(blocks[0].unpack_size, 8 * 512);
        assert_eq!(blocks[0].pack_pos, 0);
        assert_eq!(blocks[0].pack_size, 4096);
    }

    #[test]
    fn test_parse_mish_too_short() {
        let data = vec![0u8; 10];
        assert!(parse_mish(&data, 0).is_err());
    }

    #[test]
    fn test_decompress_adc_literals() {
        // Literal run: 0x80 | (len - 1) followed by literal bytes
        // 3 literal bytes: 0x82, then AA BB CC
        let input = vec![0x82, 0xAA, 0xBB, 0xCC];
        let out = decompress_adc(&input, 3).unwrap();
        assert_eq!(out, vec![0xAA, 0xBB, 0xCC]);
    }

    #[test]
    fn test_decompress_adc_short_match() {
        // First: 2 literal bytes (0x81, 0xAB, 0xCD)
        // Then: short match with len=3, distance=2
        //   b = ((3-3) << 2) | (distance >> 8) = 0x00
        //   b1 = distance & 0xFF = 0x02
        // Copies 3 bytes from 2 back: AB CD AB
        let input = vec![0x81, 0xAB, 0xCD, 0x00, 0x02];
        let out = decompress_adc(&input, 5).unwrap();
        assert_eq!(out, vec![0xAB, 0xCD, 0xAB, 0xCD, 0xAB]);
    }

    #[test]
    fn test_decompress_adc_long_match() {
        // First: 4 literal bytes
        let mut input = vec![0x83, 0x01, 0x02, 0x03, 0x04];
        // Long match: len=4, distance=4
        //   b = 0x40 | (4-4) = 0x40
        //   distance = 0x0004 (big-endian)
        input.extend_from_slice(&[0x40, 0x00, 0x04]);
        let out = decompress_adc(&input, 8).unwrap();
        assert_eq!(out, vec![0x01, 0x02, 0x03, 0x04, 0x01, 0x02, 0x03, 0x04]);
    }

    #[test]
    fn test_dmg_reader_zero_blocks() {
        // Test that a DmgReader with only zero blocks works
        let blocks = vec![DmgBlock {
            block_type: METHOD_ZERO_0,
            unpack_pos: 0,
            unpack_size: 1024,
            pack_pos: 0,
            pack_size: 0,
        }];

        // We need a real file for BufReader — use a temp file
        let tmp = tempfile::NamedTempFile::new().unwrap();
        let file = std::fs::File::open(tmp.path()).unwrap();

        let mut reader = DmgReader {
            source: BufReader::new(file),
            blocks,
            total_size: 1024,
            position: 0,
            cache_block_idx: None,
            cache_data: Vec::new(),
        };

        let mut buf = vec![0xFFu8; 512];
        let n = reader.read(&mut buf).unwrap();
        assert_eq!(n, 512);
        assert!(buf.iter().all(|&b| b == 0));
    }

    #[test]
    fn test_dmg_reader_seek() {
        let blocks = vec![DmgBlock {
            block_type: METHOD_ZERO_0,
            unpack_pos: 0,
            unpack_size: 2048,
            pack_pos: 0,
            pack_size: 0,
        }];

        let tmp = tempfile::NamedTempFile::new().unwrap();
        let file = std::fs::File::open(tmp.path()).unwrap();

        let mut reader = DmgReader {
            source: BufReader::new(file),
            blocks,
            total_size: 2048,
            position: 0,
            cache_block_idx: None,
            cache_data: Vec::new(),
        };

        // Seek to middle
        let pos = reader.seek(SeekFrom::Start(1024)).unwrap();
        assert_eq!(pos, 1024);

        // Seek from end
        let pos = reader.seek(SeekFrom::End(-512)).unwrap();
        assert_eq!(pos, 1536);

        // Seek relative
        let pos = reader.seek(SeekFrom::Current(-512)).unwrap();
        assert_eq!(pos, 1024);
    }

    #[test]
    fn test_dmg_reader_raw_blocks() {
        // Create a temp file with some data
        let mut tmp = tempfile::NamedTempFile::new().unwrap();
        let test_data = vec![0xABu8; 512];
        tmp.write_all(&test_data).unwrap();
        tmp.flush().unwrap();

        let file = std::fs::File::open(tmp.path()).unwrap();

        let blocks = vec![DmgBlock {
            block_type: METHOD_COPY,
            unpack_pos: 0,
            unpack_size: 512,
            pack_pos: 0,
            pack_size: 512,
        }];

        let mut reader = DmgReader {
            source: BufReader::new(file),
            blocks,
            total_size: 512,
            position: 0,
            cache_block_idx: None,
            cache_data: Vec::new(),
        };

        let mut buf = vec![0u8; 512];
        let n = reader.read(&mut buf).unwrap();
        assert_eq!(n, 512);
        assert!(buf.iter().all(|&b| b == 0xAB));
    }
}
