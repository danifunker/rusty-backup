//! Compact Pro (`.cpt`) archive reader — Bill Goodman's Compactor / Compact
//! Pro format, common for vintage Mac software and frequently distributed as a
//! BinHex-wrapped self-extracting archive (`.sea.hqx`).
//!
//! Two-stage compression: a byte-level RLE layer, optionally fed by an LZH
//! (LZSS + canonical Huffman) layer. Each file carries a data and/or resource
//! fork plus Finder info, mapping cleanly onto the shared [`StuffItEntry`] /
//! [`StuffItArchive`] plumbing the rest of `macarchive` already uses, so the
//! directory tree and all four output formats in `extract` come for free.
//!
//! Ported from XADMaster's `XADCompactProParser.m`,
//! `XADCompactProRLEHandle.m`, and `XADCompactProLZHHandle.m` (which build on
//! `XADLZSSHandle`, `XADPrefixCode`, and `CSInputBuffer`, all under
//! `~/repos/XADMaster`). The block-padding quirk in the LZH layer and the
//! RLE escape sequence are faithful reproductions of that reference.

use anyhow::{bail, Result};
use byteorder::{BigEndian, ByteOrder};
use std::sync::OnceLock;

use super::stuffit::{ForkCodec, ForkInfo, StuffItArchive, StuffItEntry};
use crate::fs::hfs::mac_roman_to_utf8;

/// Bytes of file metadata that follow a file entry's name (volume, offset,
/// type/creator, two dates, finder flags, crc, flags, four lengths).
const FILE_METADATA_LEN: usize = 45;

// ---------------------------------------------------------------------------
// Detection
// ---------------------------------------------------------------------------

/// True iff `bytes` is a Compact Pro archive. Mirrors XADMaster's recognizer:
/// the leading `0x01` marker is far too weak on its own, so we walk the index
/// at the end of the file and confirm the stored CRC-32 over the directory.
pub fn is_compactpro(bytes: &[u8]) -> bool {
    validate_index(bytes).unwrap_or(false)
}

/// Walk the index and check its CRC; `Some(true)` only when everything lines
/// up. Returns `Ok(false)` / `None`-equivalent for any structural problem so
/// the detector never panics on hostile input.
fn validate_index(bytes: &[u8]) -> Option<bool> {
    if bytes.len() < 8 || bytes[0] != 1 {
        return Some(false);
    }
    let offset = BigEndian::read_u32(bytes.get(4..8)?) as usize;
    // headcrc(4) + numentries(2) + commentlen(1)
    let correct = BigEndian::read_u32(bytes.get(offset..offset + 4)?);
    let mut p = offset + 4;

    let head = bytes.get(p..p + 3)?;
    let mut crc = crc32_update(0xffff_ffff, head);
    let numentries = BigEndian::read_u16(&head[0..2]) as usize;
    let commentsize = head[2] as usize;
    p += 3;

    let comment = bytes.get(p..p + commentsize)?;
    crc = crc32_update(crc, comment);
    p += commentsize;

    for _ in 0..numentries {
        let namelen = *bytes.get(p)?;
        crc = crc32_update(crc, &[namelen]);
        p += 1;
        let nl = (namelen & 0x7f) as usize;
        let namebytes = bytes.get(p..p + nl)?;
        crc = crc32_update(crc, namebytes);
        p += nl;
        let metadatasize = if namelen & 0x80 != 0 {
            2
        } else {
            FILE_METADATA_LEN
        };
        let meta = bytes.get(p..p + metadatasize)?;
        crc = crc32_update(crc, meta);
        p += metadatasize;
    }

    Some(crc == correct)
}

// ---------------------------------------------------------------------------
// Directory parsing
// ---------------------------------------------------------------------------

/// Parse a Compact Pro archive's directory into the shared entry tree. Fork
/// data offsets are absolute within `bytes`, exactly like the StuffIt parsers.
pub fn parse(bytes: &[u8]) -> Result<StuffItArchive> {
    if bytes.len() < 8 || bytes[0] != 1 {
        bail!("not a Compact Pro archive");
    }
    let index_offset = BigEndian::read_u32(&bytes[4..8]) as usize;
    let mut pos = index_offset;

    // headcrc(4) numentries(2) commentlen(1)
    if pos + 7 > bytes.len() {
        bail!("Compact Pro: truncated archive index");
    }
    pos += 4; // skip stored header CRC (validated in is_compactpro)
    let numentries = BigEndian::read_u16(&bytes[pos..pos + 2]) as usize;
    pos += 2;
    let commentlen = bytes[pos] as usize;
    pos += 1;
    pos = pos
        .checked_add(commentlen)
        .filter(|&p| p <= bytes.len())
        .ok_or_else(|| anyhow::anyhow!("Compact Pro: comment runs past end of archive"))?;

    let mut entries = Vec::new();
    parse_directory(bytes, &mut pos, &[], numentries, &mut entries)?;
    Ok(StuffItArchive { entries })
}

fn parse_directory(
    bytes: &[u8],
    pos: &mut usize,
    parent: &[String],
    mut numentries: usize,
    entries: &mut Vec<StuffItEntry>,
) -> Result<()> {
    while numentries > 0 {
        let namelen = *bytes
            .get(*pos)
            .ok_or_else(|| anyhow::anyhow!("Compact Pro: index truncated mid-entry"))?;
        *pos += 1;
        let nl = (namelen & 0x7f) as usize;
        let namebytes = bytes
            .get(*pos..*pos + nl)
            .ok_or_else(|| anyhow::anyhow!("Compact Pro: name runs past end of archive"))?;
        let name = mac_roman_to_utf8(namebytes);
        *pos += nl;

        let mut path = parent.to_vec();
        path.push(name.clone());

        if namelen & 0x80 != 0 {
            // Directory: 2-byte (recursive) descendant count.
            let numdir = read_u16(bytes, pos)? as usize;
            entries.push(StuffItEntry {
                path: path.clone(),
                name,
                is_dir: true,
                type_code: [0; 4],
                creator_code: [0; 4],
                finder_flags: 0,
                create_date: 0,
                mod_date: 0,
                data: None,
                rsrc: None,
            });
            parse_directory(bytes, pos, &path, numdir, entries)?;
            numentries = numentries
                .checked_sub(numdir + 1)
                .ok_or_else(|| anyhow::anyhow!("Compact Pro: inconsistent directory count"))?;
        } else {
            // File: 45 bytes of metadata.
            let meta = bytes
                .get(*pos..*pos + FILE_METADATA_LEN)
                .ok_or_else(|| anyhow::anyhow!("Compact Pro: file metadata past end of archive"))?;
            *pos += FILE_METADATA_LEN;

            // volume@0, fileoffs@1, type@5, creator@9, cdate@13, mdate@17,
            // finderflags@21, crc@23, flags@27, rsrclen@29, datalen@33,
            // rsrccomplen@37, datacomplen@41.
            let fileoffs = BigEndian::read_u32(&meta[1..5]) as u64;
            let mut type_code = [0u8; 4];
            type_code.copy_from_slice(&meta[5..9]);
            let mut creator_code = [0u8; 4];
            creator_code.copy_from_slice(&meta[9..13]);
            let create_date = BigEndian::read_u32(&meta[13..17]);
            let mod_date = BigEndian::read_u32(&meta[17..21]);
            let finder_flags = BigEndian::read_u16(&meta[21..23]);
            let crc = BigEndian::read_u32(&meta[23..27]);
            let flags = BigEndian::read_u16(&meta[27..29]);
            let resourcelength = BigEndian::read_u32(&meta[29..33]);
            let datalength = BigEndian::read_u32(&meta[33..37]);
            let resourcecomplen = BigEndian::read_u32(&meta[37..41]);
            let datacomplen = BigEndian::read_u32(&meta[41..45]);

            let encrypted = flags & 1 != 0;

            // Resource fork present when it has uncompressed bytes; its LZH bit
            // is flags&2. The single per-file CRC-32 is carried on both forks
            // and verified once per entry (see [`verify_entry_crc`]).
            let rsrc = if resourcelength > 0 {
                Some(ForkInfo {
                    method: 0,
                    codec: ForkCodec::CompactPro {
                        lzh: flags & 2 != 0,
                    },
                    encrypted,
                    uncompressed_len: resourcelength,
                    compressed_len: resourcecomplen,
                    crc: 0,
                    crc32: crc,
                    offset: fileoffs,
                })
            } else {
                None
            };
            // Data fork present when it has bytes, or when the file has neither
            // fork (a zero-length data fork). Its LZH bit is flags&4.
            let data = if datalength > 0 || resourcelength == 0 {
                Some(ForkInfo {
                    method: 0,
                    codec: ForkCodec::CompactPro {
                        lzh: flags & 4 != 0,
                    },
                    encrypted,
                    uncompressed_len: datalength,
                    compressed_len: datacomplen,
                    crc: 0,
                    crc32: crc,
                    offset: fileoffs + resourcecomplen as u64,
                })
            } else {
                None
            };

            entries.push(StuffItEntry {
                path,
                name,
                is_dir: false,
                type_code,
                creator_code,
                finder_flags,
                create_date,
                mod_date,
                data,
                rsrc,
            });
            numentries -= 1;
        }
    }
    Ok(())
}

fn read_u16(bytes: &[u8], pos: &mut usize) -> Result<u16> {
    let v = bytes
        .get(*pos..*pos + 2)
        .ok_or_else(|| anyhow::anyhow!("Compact Pro: index truncated"))?;
    *pos += 2;
    Ok(BigEndian::read_u16(v))
}

// ---------------------------------------------------------------------------
// Fork decompression
// ---------------------------------------------------------------------------

/// Decompress one Compact Pro fork (RLE, optionally LZH-fed). The CRC is not
/// checked here — Compact Pro stores one checksum per *file* covering the
/// resource fork followed by the data fork, so the caller verifies it once
/// both forks are in hand via [`verify_entry_crc`].
pub fn decompress_fork(archive: &[u8], fork: &ForkInfo) -> Result<Vec<u8>> {
    let lzh = match fork.codec {
        ForkCodec::CompactPro { lzh } => lzh,
        ForkCodec::StuffIt => bail!("compactpro::decompress_fork called on a StuffIt fork"),
    };
    if fork.encrypted {
        bail!("Compact Pro: encrypted entries are not supported");
    }

    let start = fork.offset as usize;
    let end = start
        .checked_add(fork.compressed_len as usize)
        .ok_or_else(|| anyhow::anyhow!("Compact Pro: fork length overflow"))?;
    if end > archive.len() {
        bail!("Compact Pro: fork data extends past end of archive");
    }
    let comp = &archive[start..end];
    let out_len = fork.uncompressed_len as usize;

    if lzh {
        let mut src = CpLzh::new(comp);
        rle_decode(&mut src, out_len)
    } else {
        let mut src = SliceSource::new(comp);
        rle_decode(&mut src, out_len)
    }
}

/// Verify a Compact Pro file's CRC-32. The stored checksum covers the
/// decompressed resource fork immediately followed by the decompressed data
/// fork (either may be empty), as a single reflected CRC-32 with no final
/// complement. This is the only integrity check for the format, so it runs
/// for every entry — single-fork, dual-fork, and empty alike.
pub fn verify_entry_crc(rsrc: &[u8], data: &[u8], expected: u32) -> Result<()> {
    let got = crc32_update(crc32_update(0xffff_ffff, rsrc), data);
    if got != expected {
        bail!("Compact Pro: file CRC mismatch (got {got:#010x}, expected {expected:#010x})");
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// RLE layer (XADCompactProRLEHandle)
// ---------------------------------------------------------------------------

/// A byte source the RLE layer pulls from: either the LZH decoder or, for
/// RLE-only forks, the raw compressed bytes.
trait ByteSource {
    fn next_byte(&mut self) -> Option<u8>;
}

struct SliceSource<'a> {
    data: &'a [u8],
    pos: usize,
}

impl<'a> SliceSource<'a> {
    fn new(data: &'a [u8]) -> Self {
        SliceSource { data, pos: 0 }
    }
}

impl ByteSource for SliceSource<'_> {
    fn next_byte(&mut self) -> Option<u8> {
        let v = self.data.get(self.pos).copied();
        if v.is_some() {
            self.pos += 1;
        }
        v
    }
}

#[derive(Default)]
struct RleState {
    saved: u8,
    /// Remaining repeats of `saved`. Signed so a malformed short run count
    /// can't wrap into a huge positive value (matches XAD's `int repeat`).
    repeat: i64,
    halfescaped: bool,
}

/// Produce one decompressed byte, mirroring `produceByteAtOffset`. `0x81` is
/// the escape; `0x81 0x82 n` repeats the previous byte, `0x81 0x82 0` emits a
/// literal `0x81 0x82`, `0x81 0x81` emits a literal `0x81`, and `0x81 x`
/// emits `0x81 x`.
fn rle_next(st: &mut RleState, src: &mut dyn ByteSource) -> Option<u8> {
    if st.repeat != 0 {
        st.repeat -= 1;
        return Some(st.saved);
    }

    let byte = if st.halfescaped {
        st.halfescaped = false;
        0x81
    } else {
        src.next_byte()?
    };

    if byte != 0x81 {
        st.saved = byte;
        return Some(byte);
    }

    let b2 = src.next_byte()?;
    if b2 == 0x82 {
        let count = src.next_byte()?;
        if count != 0 {
            st.repeat = count as i64 - 2;
            Some(st.saved)
        } else {
            st.repeat = 1;
            st.saved = 0x82;
            Some(0x81)
        }
    } else if b2 == 0x81 {
        st.halfescaped = true;
        st.saved = 0x81;
        Some(0x81)
    } else {
        st.repeat = 1;
        st.saved = b2;
        Some(0x81)
    }
}

fn rle_decode(src: &mut dyn ByteSource, out_len: usize) -> Result<Vec<u8>> {
    let mut out = Vec::with_capacity(out_len);
    let mut st = RleState::default();
    while out.len() < out_len {
        match rle_next(&mut st, src) {
            Some(b) => out.push(b),
            None => bail!(
                "Compact Pro: unexpected end of compressed data ({} of {} bytes)",
                out.len(),
                out_len
            ),
        }
    }
    Ok(out)
}

// ---------------------------------------------------------------------------
// LZH layer (XADCompactProLZHHandle over XADLZSSHandle)
// ---------------------------------------------------------------------------

const LZH_BLOCKSIZE: i64 = 0x1fff0;
const LZH_WINDOW_MASK: u64 = 0x1fff; // 8192-byte window

enum Token {
    Literal(u8),
    Match { offset: u32, length: i32 },
}

struct CpLzh<'a> {
    input: CpInput<'a>,
    window: Vec<u8>,
    pos: u64,
    matchlength: i32,
    matchoffset: u64,
    blockcount: i64,
    blockstart: i64,
    literalcode: Option<Huff>,
    lengthcode: Option<Huff>,
    offsetcode: Option<Huff>,
    ended: bool,
}

impl<'a> CpLzh<'a> {
    fn new(data: &'a [u8]) -> Self {
        CpLzh {
            input: CpInput::new(data),
            window: vec![0u8; (LZH_WINDOW_MASK + 1) as usize],
            pos: 0,
            matchlength: 0,
            matchoffset: 0,
            blockcount: LZH_BLOCKSIZE, // force a block header on first token
            blockstart: 0,
            literalcode: None,
            lengthcode: None,
            offsetcode: None,
            ended: false,
        }
    }

    fn produce(&mut self) -> Option<u8> {
        if self.ended {
            return None;
        }
        if self.matchlength == 0 {
            match self.next_token() {
                Some(Token::Literal(v)) => {
                    self.window[(self.pos & LZH_WINDOW_MASK) as usize] = v;
                    self.pos = self.pos.wrapping_add(1);
                    return Some(v);
                }
                Some(Token::Match { offset, length }) => {
                    self.matchlength = length;
                    self.matchoffset = self.pos.wrapping_sub(offset as u64);
                    // fall through to copy the first matched byte
                }
                None => {
                    self.ended = true;
                    return None;
                }
            }
        }

        self.matchlength -= 1;
        let byte = self.window[(self.matchoffset & LZH_WINDOW_MASK) as usize];
        self.matchoffset = self.matchoffset.wrapping_add(1);
        self.window[(self.pos & LZH_WINDOW_MASK) as usize] = byte;
        self.pos = self.pos.wrapping_add(1);
        Some(byte)
    }

    fn next_token(&mut self) -> Option<Token> {
        if self.blockcount >= LZH_BLOCKSIZE {
            if self.blockstart != 0 {
                // Skip the inter-block padding. (XAD's verbatim comment: "Don't
                // let your bad implementations leak into your file formats,
                // people!") Pad to an even offset relative to the block start.
                self.input.skip_to_byte_boundary();
                if (self.input.buffer_offset() - self.blockstart) & 1 != 0 {
                    self.input.skip_bytes(3);
                } else {
                    self.input.skip_bytes(2);
                }
            }
            self.literalcode = Some(self.parse_code(256)?);
            self.lengthcode = Some(self.parse_code(64)?);
            self.offsetcode = Some(self.parse_code(128)?);
            self.blockcount = 0;
            self.blockstart = self.input.buffer_offset();
        }

        if self.input.next_bit() != 0 {
            self.blockcount += 2;
            let sym = Huff::decode(self.literalcode.as_ref()?, &mut self.input)?;
            Some(Token::Literal(sym as u8))
        } else {
            self.blockcount += 3;
            let length = Huff::decode(self.lengthcode.as_ref()?, &mut self.input)?;
            let osym = Huff::decode(self.offsetcode.as_ref()?, &mut self.input)?;
            let offset = ((osym as u32) << 6) | self.input.next_bits(6);
            Some(Token::Match { offset, length })
        }
    }

    /// Parse one Huffman sub-table: a byte count, then that many bytes of two
    /// 4-bit code lengths each; the remaining symbols are absent (length 0).
    fn parse_code(&mut self, size: usize) -> Option<Huff> {
        let numbytes = self.input.next_byte()? as usize;
        if numbytes * 2 > size {
            return None;
        }
        let mut lengths = vec![0i32; size];
        for i in 0..numbytes {
            let val = self.input.next_byte()? as i32;
            lengths[2 * i] = val >> 4;
            lengths[2 * i + 1] = val & 0x0f;
        }
        Huff::build(&lengths)
    }
}

impl ByteSource for CpLzh<'_> {
    fn next_byte(&mut self) -> Option<u8> {
        self.produce()
    }
}

// ---------------------------------------------------------------------------
// Canonical Huffman (XADPrefixCode, shortestCodeIsZeros, high-bit-first)
// ---------------------------------------------------------------------------

const HUFF_MAX_BITS: usize = 15;

/// Canonical prefix code decoded high-bit-first, matching XADPrefixCode with
/// `shortestCodeIsZeros:YES`. Built from per-symbol bit lengths (0 = absent).
struct Huff {
    /// `counts[len]` = number of symbols assigned a code of that length.
    counts: [u16; HUFF_MAX_BITS + 1],
    /// Symbols ordered by (length, symbol value) — the canonical order.
    symbols: Vec<i32>,
}

impl Huff {
    fn build(lengths: &[i32]) -> Option<Huff> {
        let mut counts = [0u16; HUFF_MAX_BITS + 1];
        for &l in lengths {
            if !(0..=HUFF_MAX_BITS as i32).contains(&l) {
                return None;
            }
            counts[l as usize] += 1;
        }
        // An all-absent table is valid but decodes nothing; reject over-
        // subscribed code spaces (incomplete ones are allowed: a single symbol).
        let mut left: i64 = 1;
        for &count in &counts[1..=HUFF_MAX_BITS] {
            left = (left << 1) - count as i64;
            if left < 0 {
                return None;
            }
        }

        // Offsets into the symbol table, sorted by symbol within each length.
        let mut offs = [0u16; HUFF_MAX_BITS + 2];
        for len in 1..=HUFF_MAX_BITS {
            offs[len + 1] = offs[len] + counts[len];
        }
        let total = offs[HUFF_MAX_BITS + 1] as usize;
        let mut symbols = vec![0i32; total];
        for (sym, &len) in lengths.iter().enumerate() {
            if len != 0 {
                symbols[offs[len as usize] as usize] = sym as i32;
                offs[len as usize] += 1;
            }
        }

        Some(Huff { counts, symbols })
    }

    /// Decode one symbol, consuming bits high-bit-first (puff.c's algorithm,
    /// the inverse of the canonical assignment). Returns `None` on an invalid
    /// or truncated code.
    fn decode(&self, input: &mut CpInput) -> Option<i32> {
        let mut code: i32 = 0;
        let mut first: i32 = 0;
        let mut index: i32 = 0;
        for len in 1..=HUFF_MAX_BITS {
            code |= input.next_bit() as i32;
            let count = self.counts[len] as i32;
            if code - count < first {
                return self.symbols.get((index + (code - first)) as usize).copied();
            }
            index += count;
            first += count;
            first <<= 1;
            code <<= 1;
        }
        None
    }
}

// ---------------------------------------------------------------------------
// Bit reader (CSInputBuffer, non-LE path) over an in-memory slice
// ---------------------------------------------------------------------------

/// MSB-first bit reader replicating XADMaster's `CSInputBuffer` byte/bit
/// accounting closely enough that `buffer_offset()` (its `currbyte`) tracks
/// `ceil(consumed_bits / 8)` — the LZH block-padding skip depends on it. Reads
/// past end-of-input yield zero bits / `None` bytes rather than panicking.
struct CpInput<'a> {
    data: &'a [u8],
    currbyte: usize,
    bits: u32,
    numbits: u32,
}

impl<'a> CpInput<'a> {
    fn new(data: &'a [u8]) -> Self {
        CpInput {
            data,
            currbyte: 0,
            bits: 0,
            numbits: 0,
        }
    }

    fn peek_byte(&self, offs: usize) -> u32 {
        match self.data.get(self.currbyte + offs) {
            Some(&b) => b as u32,
            None => 0,
        }
    }

    fn fill_bits(&mut self) {
        let mut numbytes = ((32 - self.numbits) >> 3) as usize;
        let left = self.data.len().saturating_sub(self.currbyte);
        if numbytes > left {
            numbytes = left;
        }
        let s = (self.numbits >> 3) as usize;
        match numbytes {
            4 => {
                self.bits = (self.peek_byte(s) << 24)
                    | (self.peek_byte(s + 1) << 16)
                    | (self.peek_byte(s + 2) << 8)
                    | self.peek_byte(s + 3);
            }
            3 => {
                self.bits |= ((self.peek_byte(s) << 16)
                    | (self.peek_byte(s + 1) << 8)
                    | self.peek_byte(s + 2))
                    << (8 - self.numbits);
            }
            2 => {
                self.bits |=
                    ((self.peek_byte(s) << 8) | self.peek_byte(s + 1)) << (16 - self.numbits);
            }
            1 => {
                self.bits |= self.peek_byte(s) << (24 - self.numbits);
            }
            _ => {}
        }
        self.numbits += (numbytes as u32) * 8;
    }

    fn peek_bits(&mut self, n: u32) -> u32 {
        if n == 0 {
            return 0;
        }
        if n > self.numbits {
            self.fill_bits();
        }
        self.bits >> (32 - n)
    }

    fn skip_peeked(&mut self, n: u32) {
        let numbytes = ((n as i64) - ((self.numbits & 7) as i64) + 7) >> 3;
        self.currbyte += numbytes.max(0) as usize;
        self.bits = if n >= 32 { 0 } else { self.bits << n };
        self.numbits = self.numbits.saturating_sub(n);
    }

    fn next_bit(&mut self) -> u32 {
        let b = self.peek_bits(1);
        self.skip_peeked(1);
        b
    }

    fn next_bits(&mut self, n: u32) -> u32 {
        if n == 0 {
            return 0;
        }
        let b = self.peek_bits(n);
        self.skip_peeked(n);
        b
    }

    /// Byte-aligned read (Compact Pro only uses this at block boundaries, where
    /// the stream is on a byte boundary). `None` past end of input.
    fn next_byte(&mut self) -> Option<u8> {
        let v = self.data.get(self.currbyte).copied();
        if v.is_some() {
            self.currbyte += 1;
        }
        v
    }

    fn skip_to_byte_boundary(&mut self) {
        self.bits = 0;
        self.numbits = 0;
    }

    fn skip_bytes(&mut self, n: usize) {
        self.currbyte += n;
    }

    fn buffer_offset(&self) -> i64 {
        self.currbyte as i64
    }
}

// ---------------------------------------------------------------------------
// CRC-32 (reflected, polynomial 0xedb88320, no final XOR — Compact Pro stores
// the running value directly)
// ---------------------------------------------------------------------------

fn crc_table() -> &'static [u32; 256] {
    static TABLE: OnceLock<[u32; 256]> = OnceLock::new();
    TABLE.get_or_init(|| {
        let mut t = [0u32; 256];
        let mut n = 0usize;
        while n < 256 {
            let mut c = n as u32;
            let mut k = 0;
            while k < 8 {
                c = if c & 1 != 0 {
                    0xedb8_8320 ^ (c >> 1)
                } else {
                    c >> 1
                };
                k += 1;
            }
            t[n] = c;
            n += 1;
        }
        t
    })
}

fn crc32_update(mut crc: u32, data: &[u8]) -> u32 {
    let t = crc_table();
    for &b in data {
        crc = t[((crc ^ b as u32) & 0xff) as usize] ^ (crc >> 8);
    }
    crc
}

#[cfg(test)]
mod tests {
    use super::*;

    /// RLE: a plain byte stream with no `0x81` escapes round-trips verbatim.
    #[test]
    fn rle_passthrough_without_escapes() {
        let mut src = SliceSource::new(&[1, 2, 3, 4, 5]);
        let out = rle_decode(&mut src, 5).unwrap();
        assert_eq!(out, vec![1, 2, 3, 4, 5]);
    }

    /// RLE run: literal `A` then `0x81 0x82 0x04` repeats it to four total.
    #[test]
    fn rle_run_repeats_previous_byte() {
        let mut src = SliceSource::new(&[b'A', 0x81, 0x82, 0x04]);
        let out = rle_decode(&mut src, 4).unwrap();
        assert_eq!(out, vec![b'A', b'A', b'A', b'A']);
    }

    /// RLE escape for a literal `0x81 0x82` pair: `0x81 0x82 0x00`.
    #[test]
    fn rle_literal_escape_pair() {
        let mut src = SliceSource::new(&[0x81, 0x82, 0x00]);
        let out = rle_decode(&mut src, 2).unwrap();
        assert_eq!(out, vec![0x81, 0x82]);
    }

    /// CRC-32 stored form is the running value with no final complement; an
    /// empty buffer leaves the seed untouched.
    #[test]
    fn crc32_empty_is_seed() {
        assert_eq!(crc32_update(0xffff_ffff, &[]), 0xffff_ffff);
    }

    /// Reflected CRC-32 over "123456789": standard check value 0xCBF43926
    /// after final XOR, i.e. the stored (pre-XOR) value is its complement.
    #[test]
    fn crc32_check_value() {
        let stored = crc32_update(0xffff_ffff, b"123456789");
        assert_eq!(stored ^ 0xffff_ffff, 0xCBF4_3926);
    }

    /// End-to-end against a real **multi-block** Compact Pro archive (the
    /// 150 KB "Dunjin Text" data fork spans three LZH blocks, so this exercises
    /// the inter-block padding skip). Freely available from the IF Archive at
    /// `if-archive/games/mac/Dunjin44.cpt.hqx`. No-op when the file is absent.
    /// Every entry's per-file CRC-32 is checked, which is the format's own
    /// integrity guarantee — a decode error anywhere would fail it.
    #[test]
    fn extract_real_compactpro_if_present() {
        let path = std::path::Path::new("/Users/dani/Downloads/Dunjin44.cpt.hqx");
        if !path.is_file() {
            return;
        }
        let raw = std::fs::read(path).unwrap();
        let bh = crate::fs::binhex::parse_binhex(&raw).expect("binhex decode");
        assert!(is_compactpro(&bh.data_fork), "should detect Compact Pro");
        let archive = parse(&bh.data_fork).expect("compactpro parse");
        let files: Vec<_> = archive.entries.iter().filter(|e| !e.is_dir).collect();
        assert!(!files.is_empty());

        let mut verified = 0;
        let mut saw_multiblock_fork = false;
        for e in &files {
            let decode = |f: &ForkInfo| {
                decompress_fork(&bh.data_fork, f).unwrap_or_else(|err| panic!("{}: {err}", e.name))
            };
            let data = e.data.as_ref().map(&decode).unwrap_or_default();
            let rsrc = e.rsrc.as_ref().map(&decode).unwrap_or_default();
            // > ~128 KB of fork data guarantees the LZH stream crossed a block.
            if data.len() >= 150_000 || rsrc.len() >= 150_000 {
                saw_multiblock_fork = true;
            }
            // The per-file CRC-32 rides on whichever fork(s) the file has.
            if let Some(crc) = e.data.as_ref().or(e.rsrc.as_ref()).map(|f| f.crc32) {
                verify_entry_crc(&rsrc, &data, crc)
                    .unwrap_or_else(|err| panic!("{}: {err}", e.name));
                verified += 1;
            }
        }
        assert!(
            saw_multiblock_fork,
            "expected the 150 KB multi-block Dunjin Text fork"
        );
        assert!(verified > 0);
        eprintln!("Compact Pro: CRC-verified {verified} entries (incl. multi-block)");
    }

    /// Bits are consumed MSB-first, matching XADMaster's `CSInputNextBit` /
    /// `CSInputNextBitString`.
    #[test]
    fn cpinput_reads_bits_msb_first() {
        let mut inp = CpInput::new(&[0b1011_0010]);
        let bits: Vec<u32> = (0..8).map(|_| inp.next_bit()).collect();
        assert_eq!(bits, vec![1, 0, 1, 1, 0, 0, 1, 0]);

        let mut inp2 = CpInput::new(&[0b1100_1010, 0b1111_0000]);
        assert_eq!(inp2.next_bits(4), 0b1100);
        assert_eq!(inp2.next_bits(4), 0b1010);
        assert_eq!(inp2.next_bits(8), 0b1111_0000);
    }

    /// `buffer_offset()` (XAD's `currbyte`) must equal `ceil(consumed_bits/8)`
    /// at every step — the LZH inter-block padding skip depends on this exact
    /// accounting. Validated independently of any real archive.
    #[test]
    fn cpinput_byte_offset_is_ceil_of_consumed_bits() {
        let data = [0xAC, 0x35, 0xFF, 0x00, 0x42];
        let mut inp = CpInput::new(&data);
        for k in 1..=(data.len() as u32 * 8) {
            let _ = inp.next_bit();
            assert_eq!(
                inp.buffer_offset(),
                k.div_ceil(8) as i64,
                "after consuming {k} bits"
            );
        }
    }

    /// Skipping to the byte boundary rounds the position up to the next byte,
    /// and a subsequent byte read lands on that byte — the exact sequence the
    /// block-transition code performs before parsing a new block header.
    #[test]
    fn cpinput_skip_to_byte_boundary_then_byte_read_aligns() {
        let mut inp = CpInput::new(&[0xAB, 0xCD, 0xEF, 0x12]);
        for _ in 0..3 {
            inp.next_bit();
        }
        assert_eq!(inp.buffer_offset(), 1, "3 bits -> ceil = byte 1");
        inp.skip_to_byte_boundary();
        assert_eq!(inp.buffer_offset(), 1, "boundary holds at byte 1");
        assert_eq!(inp.next_byte(), Some(0xCD), "byte read resumes at byte 1");
        assert_eq!(inp.buffer_offset(), 2);
        // skip_bytes advances exactly, matching the +2/+3 padding skip.
        inp.skip_bytes(1);
        assert_eq!(inp.next_byte(), Some(0x12));
    }

    /// A single-symbol Huffman code (one length-1 symbol) decodes that symbol.
    #[test]
    fn huff_single_symbol() {
        let mut lengths = vec![0i32; 4];
        lengths[2] = 1; // symbol 2, code length 1
        let huff = Huff::build(&lengths).expect("valid code");
        let mut input = CpInput::new(&[0x00]);
        assert_eq!(huff.decode(&mut input), Some(2));
    }

    /// The per-file CRC covers `resource ++ data`; an empty file checksums to
    /// the bare seed, and order matters (rsrc before data).
    #[test]
    fn verify_entry_crc_covers_rsrc_then_data() {
        // Empty entry: seed value, as seen on Compact Pro's `Icon` placeholder.
        assert!(verify_entry_crc(&[], &[], 0xffff_ffff).is_ok());

        let rsrc = b"RSRC-BYTES";
        let data = b"DATA-BYTES";
        let expected = crc32_update(crc32_update(0xffff_ffff, rsrc), data);
        assert!(verify_entry_crc(rsrc, data, expected).is_ok());
        // Swapped order must not validate.
        assert!(verify_entry_crc(data, rsrc, expected).is_err());
        // A bit flip is caught.
        assert!(verify_entry_crc(rsrc, data, expected ^ 1).is_err());
    }

    /// Over-subscribed code lengths are rejected rather than producing a
    /// corrupt table.
    #[test]
    fn huff_rejects_oversubscribed() {
        // Three length-1 codes cannot coexist (only two 1-bit codes exist).
        let lengths = vec![1, 1, 1];
        assert!(Huff::build(&lengths).is_none());
    }
}
